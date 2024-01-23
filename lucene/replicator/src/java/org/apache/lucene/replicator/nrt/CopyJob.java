/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.replicator.nrt;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.util.IOUtils;

/**
 * Handles copying one set of files, e.g. all files for a new NRT point, or files for pre-copying a
 * merged segment. This notifies the caller via OnceDone when the job finishes or failed.
 *
 * @lucene.experimental
 */
public abstract class CopyJob implements Comparable<CopyJob> {
  private static final AtomicLong counter = new AtomicLong();
  // 目标 replica node
  protected final ReplicaNode dest;
  // 需要拷贝的文件列表
  protected final Map<String, FileMetaData> files;
  //
  public final long ord = counter.incrementAndGet();

  /** True for an NRT sync, false for pre-copying a newly merged segment
   * 优先级，NRT sync 优先级高，预拷贝merged segment 优先级底
   * */
  public final boolean highPriority;
  // 任务执行完的回调
  public final OnceDone onceDone;

  public final long startNS = System.nanoTime();

  public final String reason;
  // 需要拷贝的文件
  protected final List<Map.Entry<String, FileMetaData>> toCopy;

  protected long totBytes;

  protected long totBytesCopied;

  // The file we are currently copying:
  // 当前正在拷贝的文件
  protected CopyOneFile current;

  // Set when we are cancelled
  // 标记取消，在每次拉取新文件之前都会检查这个标志
  protected volatile Throwable exc;
  protected volatile String cancelReason;

  // toString may concurrently access this:
  // 拷贝完成的文件！
  protected final Map<String, String> copiedFiles = new ConcurrentHashMap<>();

  protected CopyJob(
      String reason,
      Map<String, FileMetaData> files,
      ReplicaNode dest,
      boolean highPriority,
      OnceDone onceDone)
      throws IOException {
    this.reason = reason;
    this.files = files;
    this.dest = dest;
    this.highPriority = highPriority;
    this.onceDone = onceDone;

    // Exceptions in here are bad:
    try {
      this.toCopy = dest.getFilesToCopy(this.files);
    } catch (Throwable t) {
      cancel("exc during init", t);
      throw new CorruptIndexException("exception while checking local files", "n/a", t);
    }
  }

  /** Callback invoked by CopyJob once all files have (finally) finished copying */
  public interface OnceDone {
    public void run(CopyJob job) throws IOException;
  }

  /**
   * Transfers whatever tmp files were already copied in this previous job and cancels the previous
   * job
   * 不管是否临时文件已经在老的任务中被拷贝，都进行转移，并且取消老的任务
   */
  public synchronized void transferAndCancel(CopyJob prevJob) throws IOException {
    synchronized (prevJob) {
      dest.message("CopyJob: now transfer prevJob " + prevJob);
      try {
        _transferAndCancel(prevJob);
      } catch (Throwable t) {
        dest.message("xfer: exc during transferAndCancel");
        cancel("exc during transferAndCancel", t);
        throw IOUtils.rethrowAlways(t);
      }
    }
  }

  private synchronized void _transferAndCancel(CopyJob prevJob) throws IOException {

    // Caller must already be sync'd on prevJob:
    assert Thread.holdsLock(prevJob);

    if (prevJob.exc != null) {
      // Already cancelled
      dest.message("xfer: prevJob was already cancelled; skip transfer");
      return;
    }

    // Cancel the previous job
    // 标记取消，在每次拉取新文件之前都会检查这个标志
    prevJob.exc = new Throwable();

    // Carry over already copied files that we also want to copy
    // 携带已经被拷贝，但是我们依然想拷贝的文件
    Iterator<Map.Entry<String, FileMetaData>> it = toCopy.iterator();
    long bytesAlreadyCopied = 0;

    // Iterate over all files we think we need to copy:
    // 遍历所有的文件，找到我们需要拷贝的
    while (it.hasNext()) {
      Map.Entry<String, FileMetaData> ent = it.next();
      String fileName = ent.getKey();
      String prevTmpFileName = prevJob.copiedFiles.get(fileName);
      if (prevTmpFileName != null) {
        // This fileName is common to both jobs, and the old job already finished copying it (to a
        // temp file), so we keep it:
        // 在老的任务中已经拷贝完成的文件，我们直接放入新的任务
        // 的拷贝完成的文件列表中，不需要重新拷贝
        long fileLength = ent.getValue().length;
        bytesAlreadyCopied += fileLength;
        dest.message(
            "xfer: carry over already-copied file "
                + fileName
                + " ("
                + prevTmpFileName
                + ", "
                + fileLength
                + " bytes)");
        copiedFiles.put(fileName, prevTmpFileName);

        // So we don't try to delete it, below:
        // 此时可以从老的已完成任务列表中删除此完成的文件
        prevJob.copiedFiles.remove(fileName);

        // So it's not in our copy list anymore:
        // 此时也可以将其从 遍历列表中删除
        it.remove();
      } else if (prevJob.current != null && prevJob.current.name.equals(fileName)) {
        // This fileName is common to both jobs, and it's the file that the previous job was in the
        // process of copying.  In this case
        // we continue copying it from the prevoius job.  This is important for cases where we are
        // copying over a large file
        // because otherwise we could keep failing the NRT copy and restarting this file from the
        // beginning and never catch up:
        // 如果老的任务正在拷贝这个文件，在这种情况下，我们会继续在老的任务中完成拷贝它。这么做很重要，特别是对于我们
        // 拷贝大文件的场景，否则，我们可能会继续使NRT副本失败，并从头开始重新启动此文件，永远无法赶上，可能每次拷贝
        // 过程中就有新的job生成，从而打断。但是这样并不能完全解决问题，因为有可能一直没有生效segments info!
        dest.message(
            "xfer: carry over in-progress file "
                + fileName
                + " ("
                + prevJob.current.tmpName
                + ") bytesCopied="
                + prevJob.current.getBytesCopied()
                + " of "
                + prevJob.current.bytesToCopy);
        bytesAlreadyCopied += prevJob.current.getBytesCopied();

        assert current == null;

        // must set current first, before writing/read to c.in/out in case that hits an exception,
        // so that we then close the temp
        // IndexOutput when cancelling ourselves:
        // 在 写/读 到 c.in/out之前，必须设置当前拷贝文件，
        // 以防遇到异常，这样我们就可以在取消时关闭临时IndexOutput
        // 新的job一开始 就会继续执行current
        current = newCopyOneFile(prevJob.current);

        // Tell our new (primary) connection we'd like to copy this file first, but resuming from
        // how many bytes we already copied last time:
        // We do this even if bytesToCopy == bytesCopied, because we still need to readLong() the
        // checksum from the primary connection:
        assert prevJob.current.getBytesCopied() <= prevJob.current.bytesToCopy;

        prevJob.current = null;

        totBytes += current.metaData.length;

        // So it's not in our copy list anymore:
        it.remove();
      } else {
        dest.message("xfer: file " + fileName + " will be fully copied");
      }
    }
    dest.message("xfer: " + bytesAlreadyCopied + " bytes already copied of " + totBytes);

    // Delete all temp files the old job wrote but we don't need:
    // 老的任务中 copiedFiles 没有被清理的，说明都是不需要保留的文件，都是可以直接删除的！
    dest.message("xfer: now delete old temp files: " + prevJob.copiedFiles.values());
    IOUtils.deleteFilesIgnoringExceptions(dest.dir, prevJob.copiedFiles.values());
    // 1.老的job 中的current 文件不在新的拷贝文件列表中
    // 2.或者上面的处理过程中，current 又被设置了？ 这种情况能不能发生？理论上是不会发生的，因为上面已经
    // 将老任务标记为 prevJob.exc 取消了，在每次拉取新文件之前都会检查这个标志
    // 这种情况下，直接删除即可
    if (prevJob.current != null) {
      IOUtils.closeWhileHandlingException(prevJob.current);
      if (Node.VERBOSE_FILES) {
        dest.message("remove partial file " + prevJob.current.tmpName);
      }
      dest.deleter.forceDeleteFile(prevJob.current.tmpName);
      prevJob.current = null;
    }
  }

  protected abstract CopyOneFile newCopyOneFile(CopyOneFile current);

  /** Begin copying files 开始拷贝文件*/
  public abstract void start() throws IOException;

  /**
   * Use current thread (blocking) to do all copying and then return once done, or throw exception
   * on failure
   */
  public abstract void runBlocking() throws Exception;

  // 取消 job，整个会直接取消，不会有新的job做交接，正在拉取的文件也会直接取消
  public void cancel(String reason, Throwable exc) throws IOException {
    if (this.exc != null) {
      // Already cancelled
      return;
    }

    dest.message(
        String.format(
            Locale.ROOT,
            "top: cancel after copying %s; exc=%s:\n  files=%s\n  copiedFiles=%s",
            Node.bytesToString(totBytesCopied),
            exc,
            files == null ? "null" : files.keySet(),
            copiedFiles.keySet()));

    if (exc == null) {
      exc = new Throwable();
    }
    // 设置取消标志
    this.exc = exc;
    this.cancelReason = reason;

    // Delete all temp files we wrote:
    // 删除所有的临时文件
    IOUtils.deleteFilesIgnoringExceptions(dest.dir, copiedFiles.values());

    if (current != null) {
      IOUtils.closeWhileHandlingException(current);
      if (Node.VERBOSE_FILES) {
        dest.message("remove partial file " + current.tmpName);
      }
      dest.deleter.forceDeleteFile(current.tmpName);
      current = null;
    }
  }

  /** Return true if this job is trying to copy any of the same files as the other job */
  public abstract boolean conflicts(CopyJob other);

  /** Renames all copied (tmp) files to their true file names */
  public abstract void finish() throws IOException;

  public abstract boolean getFailed();

  /** Returns only those file names (a subset of {@link #getFileNames}) that need to be copied */
  public abstract Set<String> getFileNamesToCopy();

  /** Returns all file names referenced in this copy job */
  public abstract Set<String> getFileNames();

  public abstract CopyState getCopyState();

  public abstract long getTotalBytesCopied();
}
