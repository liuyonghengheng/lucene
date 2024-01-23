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
import java.io.PrintStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.util.ThreadInterruptedException;

/*
 * This just asks IndexWriter to open new NRT reader, in order to publish a new NRT point.  This could be improved, if we separated out 1)
 * nrt flush (and incRef the SIS) from 2) opening a new reader, but this is tricky with IW's concurrency, and it would also be hard-ish to share
 * IW's reader pool with our searcher manager.  So we do the simpler solution now, but that adds some unnecessary latency to NRT refresh on
 * replicas since step 2) could otherwise be done concurrently with replicas copying files over.
 * 为了发布一个新的 NRT point 我们只让IndexWriter打开新的NRT reader。如果把 nrt flush (and incRef the SIS) 和 opening a new reader
 * 分开，有可能会有所改善，但这对于IW的并发性来说是棘手的，并且这也会很难做到 向我们的searcher manager共享IW's reader pool。所以我们执行简单的方案
 * 但是这会增加一些没必要的延迟，对于副本上的NRT refresh来说，因为步骤2）可以与副本复制文件同时进行。
 */

/**
 * Node that holds an IndexWriter, indexing documents into its local index.
 * PrimaryNode 代表index主副本，主要接收写入请求，并将docs 写入本地的index。
 * @lucene.experimental
 */
public abstract class PrimaryNode extends Node {

  // Current NRT segment infos, incRef'd with IndexWriter.deleter:
  // 当前的NRT segment infos。会通过IndexWriter.deleter增加引用，防止相关文件被删除！
  private SegmentInfos curInfos;

  //有写，也有读，replicanode是没有写的
  protected final IndexWriter writer;

  // IncRef'd state of the last published NRT point; when a replica comes asking, we give it this as
  // the current NRT point:
  // 代表最新的 NRT point。也是是和副本之间同步，需要用到的信息和数据结构
  private CopyState copyState;

  // 主副本的代，如果主切换或者重新打开，就会+1 。主要用来防止副本读取到老的数据
  protected final long primaryGen; //这个index的主分片每次被启动，对应的主分片代数就会增加
  private int remoteCloseTimeoutMs = -1;

  /**
   * Contains merged segments that have been copied to all running replicas (as of when that merge
   * started warming).
   * 包含所有的，已经merged 的segments files，并且这些文件是已经被拷贝到所有的replicas（当merge开始预热时）。
   */
  final Set<String> finishedMergedFiles = Collections.synchronizedSet(new HashSet<String>());

  private final AtomicInteger copyingCount = new AtomicInteger();

  public PrimaryNode(
      IndexWriter writer,
      int id,
      long primaryGen,
      long forcePrimaryVersion,
      SearcherFactory searcherFactory,
      PrintStream printStream)
      throws IOException {
    super(id, writer.getDirectory(), searcherFactory, printStream);
    message("top: now init primary");
    this.writer = writer;
    this.primaryGen = primaryGen;

    try {
      // So that when primary node's IndexWriter finishes a merge, but before it cuts over to the
      // merged segment,
      // it copies it out to the replicas.  This ensures the whole system's NRT latency remains low
      // even when a
      // large merge completes:

      // 当primary节点的IndexWriter完成了一个merge，但是还没有切换到merge好的segment，SegmentWarmer会将其
      // 复制到replicas。这会确保整体系统性的NRT延迟保持在低水平，即使是一个较大的merge！：
      writer.getConfig().setMergedSegmentWarmer(new PreCopyMergedSegmentWarmer(this));

      message("IWC:\n" + writer.getConfig());
      message("dir:\n" + writer.getDirectory());
      message("commitData: " + writer.getLiveCommitData());

      // Record our primaryGen in the userData, and set initial version to 0:
      // 注意，会将primaryGen记录到用户数据中
      Map<String, String> commitData = new HashMap<>();
      Iterable<Map.Entry<String, String>> iter = writer.getLiveCommitData();
      if (iter != null) {
        for (Map.Entry<String, String> ent : iter) {
          commitData.put(ent.getKey(), ent.getValue());
        }
      }
      // PRIMARY_GEN
      commitData.put(PRIMARY_GEN_KEY, Long.toString(primaryGen));
      // VERSION
      if (commitData.get(VERSION_KEY) == null) {
        commitData.put(VERSION_KEY, "0");
        message("add initial commitData version=0");
      } else {
        message("keep current commitData version=" + commitData.get(VERSION_KEY));
      }
      // 设置之后，在每次commit之前会应用 commitData 中自定义的信息
      writer.setLiveCommitData(commitData.entrySet(), false);

      // We forcefully advance the SIS version to an unused future version.  This is necessary if
      // the previous primary crashed and we are
      // starting up on an "older" index, else versions can be illegally reused but show different
      // results:
      // 我们强制增加SIS version到一个没有使用过的未来的version。这是非常有必要的，如果之前的primary 损坏，并且
      // 我们在老的 index 上启动，versions 可能也会被非法的重复使用，但是却显示不同的结果。即对于用户来说两个相同的
      // 版本数据却不同！
      if (forcePrimaryVersion != -1) {
        message("now forcePrimaryVersion to version=" + forcePrimaryVersion);
        writer.advanceSegmentInfosVersion(forcePrimaryVersion);
      }
      // 获取一个SearcherManager，获取之前会执行flush
      mgr = new SearcherManager(writer, true, true, searcherFactory);
      // 初始化/设置segments infos 相关的初始信息
      setCurrentInfos(Collections.<String>emptySet());
      message("init: infos version=" + curInfos.getVersion());

    } catch (Throwable t) {
      message("init: exception");
      t.printStackTrace(printStream);
      throw new RuntimeException(t);
    }
  }

  /**
   * Returns the current primary generation, which is incremented each time a new primary is started
   * for this index
   * 这个index的主分片每次被启动，对应的主分片代数就会增加
   */
  public long getPrimaryGen() {
    return primaryGen;
  }

  // TODO: in the future, we should separate "flush" (returns an incRef'd SegmentInfos) from
  // "refresh" (open new NRT reader from
  // IndexWriter) so that the latter can be done concurrently while copying files out to replicas,
  // minimizing the refresh time from the
  // replicas.  But fixing this is tricky because e.g. IndexWriter may complete a big merge just
  // after returning the incRef'd SegmentInfos
  // and before we can open a new reader causing us to close the just-merged readers only to then
  // open them again from the (now stale)
  // SegmentInfos.  To fix this "properly" I think IW.inc/decRefDeleter must also incread the
  // ReaderPool entry
  // 以后可能会 将flush 和 refresh 分开，以便后续的操作可以与‘拷贝files到reolica的过程’并发执行，最小化从replica
  // refresh的时间。但是实现这个是非常复杂和棘手的，例如 IndexWriter可能会在返回incRef'd SegmentInfos后，
  // 在我们可以打开新的reader之前，完成一次大合并，导致我们关闭刚刚合并的reader，然后从（现在已过时）SegmentInfos
  // 再次打开它们 （为什么会关闭刚merged reader？，应该只是错过了它？）
  // 为了“正确地”修复这个问题，我认为IW.inc/decRefDeleter还必须增加 ReaderPool条目
  // 所谓的flushAndRefresh 是指，先执行flush，生成SegmentInfos，然后更新 mgr(即index searcher，
  // 打开新的NRT reader)
  //
  /**
   * Flush all index operations to disk and opens a new near-real-time reader. new NRT point, to
   * make the changes visible to searching. Returns true if there were changes.
   * 把所有index操作都写入磁盘（并没有持久化到磁盘），并且打开一个新的近实时的reader。
   * 新的NRT point，为了使更改对搜索可见。如果有更改，则返回true。
   */
  public boolean flushAndRefresh() throws IOException {
    message("top: now flushAndRefresh");
    Set<String> completedMergeFiles;
    synchronized (finishedMergedFiles) {
      completedMergeFiles = Set.copyOf(finishedMergedFiles);
    }
    // 内部执行flush 和 生成新的reader，和如果有多个线程执行到这里，这里面会有锁阻塞！
    // 所以致力不用担心多线程并发的问题，都然也不能并发执行
    mgr.maybeRefreshBlocking();
    // 设置最新的 segmentsInfos，这是个同步函数
    // 如果有变化，则返回true
    boolean result = setCurrentInfos(completedMergeFiles);
    if (result) {
      message("top: opened NRT reader version=" + curInfos.getVersion());
      finishedMergedFiles.removeAll(completedMergeFiles);
      message(
          "flushAndRefresh: version="
              + curInfos.getVersion()
              + " completedMergeFiles="
              + completedMergeFiles
              + " finishedMergedFiles="
              + finishedMergedFiles);
    } else {
      message("top: no changes in flushAndRefresh; still version=" + curInfos.getVersion());
    }
    return result;
  }

  public long getCopyStateVersion() {
    return copyState.version;
  }

  public synchronized long getLastCommitVersion() {
    Iterable<Map.Entry<String, String>> iter = writer.getLiveCommitData();
    assert iter != null;
    for (Map.Entry<String, String> ent : iter) {
      if (ent.getKey().equals(VERSION_KEY)) {
        return Long.parseLong(ent.getValue());
      }
    }

    // In ctor we always install an initial version:
    throw new AssertionError("missing VERSION_KEY");
  }

  /**
   * @return the number of milliseconds to wait during shutdown for remote replicas to close
   */
  public int getRemoteCloseTimeoutMs() {
    return remoteCloseTimeoutMs;
  }

  /**
   * Set the number of milliseconds to wait during shutdown for remote replicas to close. {@code -1}
   * (the default) means forever, and {@code 0} means don't wait at all.
   */
  public void setRemoteCloseTimeoutMs(int remoteCloseTimeoutMs) {
    if (remoteCloseTimeoutMs < -1) {
      throw new IllegalArgumentException("bad timeout + + remoteCloseTimeoutMs");
    }
    this.remoteCloseTimeoutMs = remoteCloseTimeoutMs;
  }

  @Override
  public void commit() throws IOException {
    Map<String, String> commitData = new HashMap<>();
    commitData.put(PRIMARY_GEN_KEY, Long.toString(primaryGen));
    // TODO (opto): it's a bit wasteful that we put "last refresh" version here, not the actual
    // version we are committing, because it means
    // on xlog replay we are replaying more ops than necessary.
    commitData.put(VERSION_KEY, Long.toString(copyState.version));
    message("top: commit commitData=" + commitData);
    writer.setLiveCommitData(commitData.entrySet(), false);
    writer.commit();
  }

  /** IncRef the current CopyState and return it */
  public synchronized CopyState getCopyState() throws IOException {
    ensureOpen(false);
    // message("top: getCopyState replicaID=" + replicaID + " replicaNodeID=" + replicaNodeID + "
    // version=" + curInfos.getVersion() + " infos=" + curInfos.toString());
    assert curInfos == copyState.infos;
    writer.incRefDeleter(copyState.infos);
    int count = copyingCount.incrementAndGet();
    assert count > 0;
    return copyState;
  }

  /** Called once replica is done (or failed) copying an NRT point */
  public void releaseCopyState(CopyState copyState) throws IOException {
    // message("top: releaseCopyState version=" + copyState.version);
    assert copyState.infos != null;
    writer.decRefDeleter(copyState.infos);
    int count = copyingCount.decrementAndGet();
    assert count >= 0;
  }

  @Override
  public boolean isClosed() {
    return isClosed(false);
  }

  boolean isClosed(boolean allowClosing) {
    return "closed".equals(state) || (allowClosing == false && "closing".equals(state));
  }

  private void ensureOpen(boolean allowClosing) {
    if (isClosed(allowClosing)) {
      throw new AlreadyClosedException(state);
    }
  }

  /** Steals incoming infos refCount; returns true if there were changes. */
  private synchronized boolean setCurrentInfos(Set<String> completedMergeFiles) throws IOException {

    IndexSearcher searcher = null;
    SegmentInfos infos;
    try {
      searcher = mgr.acquire();
      infos = ((StandardDirectoryReader) searcher.getIndexReader()).getSegmentInfos();
    } finally {
      if (searcher != null) {
        mgr.release(searcher);
      }
    }
    if (curInfos != null && infos.getVersion() == curInfos.getVersion()) {
      // no change
      message(
          "top: skip switch to infos: version="
              + infos.getVersion()
              + " is unchanged: "
              + infos.toString());
      return false;
    }

    SegmentInfos oldInfos = curInfos;
    writer.incRefDeleter(infos); // 标记这些文件都是在用的，不能删除！
    curInfos = infos;
    if (oldInfos != null) {
      writer.decRefDeleter(oldInfos);
    }

    message("top: switch to infos=" + infos.toString() + " version=" + infos.getVersion());

    // Serialize the SegmentInfos.
    ByteBuffersDataOutput buffer = new ByteBuffersDataOutput();
    try (ByteBuffersIndexOutput tmpIndexOutput =
        new ByteBuffersIndexOutput(buffer, "temporary", "temporary")) {
      infos.write(tmpIndexOutput);
    }
    byte[] infosBytes = buffer.toArrayCopy();

    Map<String, FileMetaData> filesMetaData = new HashMap<String, FileMetaData>();
    for (SegmentCommitInfo info : infos) {
      for (String fileName : info.files()) {
        FileMetaData metaData = readLocalFileMetaData(fileName);
        // NOTE: we hold a refCount on this infos, so this file better exist:
        assert metaData != null : "file \"" + fileName + "\" is missing metadata";
        assert filesMetaData.containsKey(fileName) == false;
        filesMetaData.put(fileName, metaData);
      }
    }

    lastFileMetaData = Collections.unmodifiableMap(filesMetaData);

    message(
        "top: set copyState primaryGen="
            + primaryGen
            + " version="
            + infos.getVersion()
            + " files="
            + filesMetaData.keySet());
    copyState =
        new CopyState(
            lastFileMetaData,
            infos.getVersion(),
            infos.getGeneration(),
            infosBytes,
            completedMergeFiles,
            primaryGen,
            curInfos);
    return true;
  }

  private synchronized void waitForAllRemotesToClose() throws IOException {
    if (remoteCloseTimeoutMs == 0) {
      return;
    }
    long waitStartNs = System.nanoTime();
    // Wait for replicas to finish or crash or timeout:
    while (remoteCloseTimeoutMs < 0
        || (System.nanoTime() - waitStartNs) / 1_000_000 < remoteCloseTimeoutMs) {
      int count = copyingCount.get();
      if (count == 0) {
        return;
      }
      message("pendingCopies: " + count);

      try {
        wait(10);
      } catch (InterruptedException ie) {
        throw new ThreadInterruptedException(ie);
      }
    }
  }

  @Override
  public void close() throws IOException {
    state = "closing";
    message("top: close primary");

    synchronized (this) {
      waitForAllRemotesToClose();
      if (curInfos != null) {
        writer.decRefDeleter(curInfos);
        curInfos = null;
      }
    }

    mgr.close();

    writer.rollback();
    dir.close();

    state = "closed";
  }

  /** Called when a merge has finished, but before IW switches to the merged segment */
  protected abstract void preCopyMergedSegmentFiles(
      SegmentCommitInfo info, Map<String, FileMetaData> files) throws IOException;
}
