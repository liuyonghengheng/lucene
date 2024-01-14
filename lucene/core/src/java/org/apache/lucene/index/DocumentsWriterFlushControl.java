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
package org.apache.lucene.index;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * This class controls {@link DocumentsWriterPerThread} flushing during indexing. It tracks the
 * memory consumption per {@link DocumentsWriterPerThread} and uses a configured {@link FlushPolicy}
 * to decide if a {@link DocumentsWriterPerThread} must flush.
 *
 * <p>In addition to the {@link FlushPolicy} the flush control might set certain {@link
 * DocumentsWriterPerThread} as flush pending iff a {@link DocumentsWriterPerThread} exceeds the
 * {@link IndexWriterConfig#getRAMPerThreadHardLimitMB()} to prevent address space exhaustion.
 */
final class DocumentsWriterFlushControl implements Accountable, Closeable {
  private final long hardMaxBytesPerDWPT;
  private long activeBytes = 0;
  private volatile long flushBytes = 0;
  private volatile int numPending = 0;
  private int numDocsSinceStalled = 0; // only with assert
  private final AtomicBoolean flushDeletes = new AtomicBoolean(false);
  private boolean fullFlush = false;
  // only for assertion that we don't get stale DWPTs from the pool
  private boolean fullFlushMarkDone = false;
  // The flushQueue is used to concurrently distribute DWPTs that are ready to be flushed ie. when a
  // full flush is in
  // progress. This might be triggered by a commit or NRT refresh. The trigger will only walk all
  // eligible DWPTs and
  // mark them as flushable putting them in the flushQueue ready for other threads (ie. indexing
  // threads) to help flushing
  // flushQueue 的作用是并发的分发那些准备好被刷新的DWPTs，即当一个full flush在执行时（只有full flush的DWPTs 才会
  // 放到flushQueue 中，正常的单个DWPT直接放到flushingWriters，被阻塞过（blockedFlushes）的
  // 单个DWPT也会放到flushQueue）。这会被一个commit或者
  // NRT refresh (他们都会产生full flush)所触发。触发器只会遍历所有符合条件的DWPTs，并且把他们标记为flushable
  // 并把他们放入flushQueue，并为其他的线程做好准备（即 索引线程），以便帮助刷新
  //
  private final Queue<DocumentsWriterPerThread> flushQueue = new LinkedList<>();
  // only for safety reasons if a DWPT is close to the RAM limit
  // 在DWPT是pending状态，并且整体才做full flush，
  // 而且在DWPT接近RAM限制时，出于安全原因才会将其放入blockedFlushes中
  private final Queue<DocumentsWriterPerThread> blockedFlushes = new LinkedList<>();
  // flushingWriters holds all currently flushing writers. There might be writers in this list that
  // are also in the flushQueue which means that writers in the flushingWriters list are not
  // necessarily
  // already actively flushing. They are only in the state of flushing and might be picked up in the
  // future by
  // polling the flushQueue
  // flushingWriters 持有所有正在执行的flush的DWPT，也有可能在flushingWriters中的DWPT同时也在flushQueue中，
  // 这种情况意味着flushingWriters列表中的DWPT不一定已经在主动刷新。
  // 它们仅处于刷新状态，将来可能会通过轮询flushQueue flushingWriters来获取
  private final List<DocumentsWriterPerThread> flushingWriters = new ArrayList<>();

  private double maxConfiguredRamBuffer = 0;
  private long peakActiveBytes = 0; // only with assert
  private long peakFlushBytes = 0; // only with assert
  private long peakNetBytes = 0; // only with assert
  private long peakDelta = 0; // only with assert
  private boolean flushByRAMWasDisabled; // only with assert
  final DocumentsWriterStallControl stallControl = new DocumentsWriterStallControl();
  private final DocumentsWriterPerThreadPool perThreadPool;
  private final FlushPolicy flushPolicy;
  private boolean closed = false;
  private final DocumentsWriter documentsWriter;
  private final LiveIndexWriterConfig config;
  private final InfoStream infoStream;

  DocumentsWriterFlushControl(DocumentsWriter documentsWriter, LiveIndexWriterConfig config) {
    this.infoStream = config.getInfoStream();
    this.perThreadPool = documentsWriter.perThreadPool;
    this.flushPolicy = config.getFlushPolicy();
    this.config = config;
    this.hardMaxBytesPerDWPT = config.getRAMPerThreadHardLimitMB() * 1024L * 1024L;
    this.documentsWriter = documentsWriter;
  }

  public synchronized long activeBytes() {
    return activeBytes;
  }

  long getFlushingBytes() {
    return flushBytes;
  }

  synchronized long netBytes() {
    return flushBytes + activeBytes;
  }

  private long stallLimitBytes() {
    final double maxRamMB = config.getRAMBufferSizeMB();
    return maxRamMB != IndexWriterConfig.DISABLE_AUTO_FLUSH
        ? (long) (2 * (maxRamMB * 1024 * 1024))
        : Long.MAX_VALUE;
  }

  private boolean assertMemory() {
    final double maxRamMB = config.getRAMBufferSizeMB();
    // We can only assert if we have always been flushing by RAM usage; otherwise the assert will
    // false trip if e.g. the
    // flush-by-doc-count * doc size was large enough to use far more RAM than the sudden change to
    // IWC's maxRAMBufferSizeMB:
    if (maxRamMB != IndexWriterConfig.DISABLE_AUTO_FLUSH && flushByRAMWasDisabled == false) {
      // for this assert we must be tolerant to ram buffer changes!
      maxConfiguredRamBuffer = Math.max(maxRamMB, maxConfiguredRamBuffer);
      final long ram = flushBytes + activeBytes;
      final long ramBufferBytes = (long) (maxConfiguredRamBuffer * 1024 * 1024);
      // take peakDelta into account - worst case is that all flushing, pending and blocked DWPT had
      // maxMem and the last doc had the peakDelta

      // 2 * ramBufferBytes -> before we stall we need to cross the 2xRAM Buffer border this is
      // still a valid limit
      // (numPending + numFlushingDWPT() + numBlockedFlushes()) * peakDelta) -> those are the total
      // number of DWPT that are not active but not yet fully flushed
      // all of them could theoretically be taken out of the loop once they crossed the RAM buffer
      // and the last document was the peak delta
      // (numDocsSinceStalled * peakDelta) -> at any given time there could be n threads in flight
      // that crossed the stall control before we reached the limit and each of them could hold a
      // peak document
      final long expected =
          (2 * ramBufferBytes)
              + ((numPending + numFlushingDWPT() + numBlockedFlushes()) * peakDelta)
              + (numDocsSinceStalled * peakDelta);
      // the expected ram consumption is an upper bound at this point and not really the expected
      // consumption
      if (peakDelta < (ramBufferBytes >> 1)) {
        /*
         * if we are indexing with very low maxRamBuffer like 0.1MB memory can
         * easily overflow if we check out some DWPT based on docCount and have
         * several DWPT in flight indexing large documents (compared to the ram
         * buffer). This means that those DWPT and their threads will not hit
         * the stall control before asserting the memory which would in turn
         * fail. To prevent this we only assert if the largest document seen
         * is smaller than the 1/2 of the maxRamBufferMB
         */
        assert ram <= expected
            : "actual mem: "
                + ram
                + " byte, expected mem: "
                + expected
                + " byte, flush mem: "
                + flushBytes
                + ", active mem: "
                + activeBytes
                + ", pending DWPT: "
                + numPending
                + ", flushing DWPT: "
                + numFlushingDWPT()
                + ", blocked DWPT: "
                + numBlockedFlushes()
                + ", peakDelta mem: "
                + peakDelta
                + " bytes, ramBufferBytes="
                + ramBufferBytes
                + ", maxConfiguredRamBuffer="
                + maxConfiguredRamBuffer;
      }
    } else {
      flushByRAMWasDisabled = true;
    }
    return true;
  }

  // only for asserts
  private boolean updatePeaks(long delta) {
    peakActiveBytes = Math.max(peakActiveBytes, activeBytes);
    peakFlushBytes = Math.max(peakFlushBytes, flushBytes);
    peakNetBytes = Math.max(peakNetBytes, netBytes());
    peakDelta = Math.max(peakDelta, delta);

    return true;
  }

  /**
   * Return the smallest number of bytes that we would like to make sure to not miss from the global
   * RAM accounting.
   */
  private long ramBufferGranularity() {
    double ramBufferSizeMB = config.getRAMBufferSizeMB();
    if (ramBufferSizeMB == IndexWriterConfig.DISABLE_AUTO_FLUSH) {
      ramBufferSizeMB = config.getRAMPerThreadHardLimitMB();
    }
    // No more than ~0.1% of the RAM buffer size.
    long granularity = (long) (ramBufferSizeMB * 1024.d);
    // Or 16kB, so that with e.g. 64 active DWPTs, we'd never be missing more than 64*16kB = 1MB in
    // the global RAM buffer accounting.
    granularity = Math.min(granularity, 16 * 1024L);
    return granularity;
  }

  DocumentsWriterPerThread doAfterDocument(DocumentsWriterPerThread perThread) {
    final long delta = perThread.getCommitLastBytesUsedDelta();
    // in order to prevent contention in the case of many threads indexing small documents
    // we skip ram accounting unless the DWPT accumulated enough ram to be worthwhile
    // 为了防止在许多线程索引小文档的情况下发生争用，
    // 我们跳过ram 统计，除非DWPT积累了足够的ram
    if (config.getMaxBufferedDocs() == IndexWriterConfig.DISABLE_AUTO_FLUSH
        && delta < ramBufferGranularity()) {
      // Skip accounting for now, we'll come back to it later when the delta is bigger
      return null;
    }

    synchronized (this) {
      // we need to commit this under lock but calculate it outside of the lock to minimize the time
      // this lock is held
      // per document. The reason we update this under lock is that we mark DWPTs as pending without
      // acquiring it's
      // lock in #setFlushPending and this also reads the committed bytes and modifies the
      // flush/activeBytes.
      // In the future we can clean this up to be more intuitive.
      perThread.commitLastBytesUsed(delta);
      try {
        /*
         * We need to differentiate here if we are pending since setFlushPending
         * moves the perThread memory to the flushBytes and we could be set to
         * pending during a delete
         */
        if (perThread.isFlushPending()) {
          flushBytes += delta;
          assert updatePeaks(delta);
        } else {
          activeBytes += delta;
          assert updatePeaks(delta);
          flushPolicy.onChange(this, perThread);
          if (!perThread.isFlushPending() && perThread.ramBytesUsed() > hardMaxBytesPerDWPT) {
            // Safety check to prevent a single DWPT exceeding its RAM limit. This
            // is super important since we can not address more than 2048 MB per DWPT
            // 安全的检查，为了防止单个的DWPT超过内存上限。这是超级重要的，
            // 因为我们不能为单个DWPT占用超过2048 MB的空间
            setFlushPending(perThread);//直接设置成FlushPending状态
          }
        }
        // 这里如果DWPT已经被设置成FlushPending状态，则会将DWPT检出，或者因为有full flush 被阻塞。
        return checkout(perThread, false);
      } finally {
        boolean stalled = updateStallState();
        assert assertNumDocsSinceStalled(stalled) && assertMemory();
      }
    }
  }

  private DocumentsWriterPerThread checkout(
      DocumentsWriterPerThread perThread, boolean markPending) {
    assert Thread.holdsLock(this);
    if (fullFlush) {
      if (perThread.isFlushPending()) {
        checkoutAndBlock(perThread);
        // 如果当前是fullFlush状态，那么就要把后续的单独flush的DWPT给阻塞住，停止flush
        // 放入阻塞队列！，等full flush 处理完了之后再从阻塞队列中拿出来继续执行。
        return nextPendingFlush();
      }
    } else {//当前没有full flush
      if (markPending) {
        assert perThread.isFlushPending() == false;
        setFlushPending(perThread);//标记为FlushPending
      }

      if (perThread.isFlushPending()) {//如果状态是FlushPending
        return checkOutForFlush(perThread);// 从perThreadPool检出，放入flushingWriters ，正在刷新！
      }
    }
    return null;
  }

  private boolean assertNumDocsSinceStalled(boolean stalled) {
    /*
     *  updates the number of documents "finished" while we are in a stalled state.
     *  this is important for asserting memory upper bounds since it corresponds
     *  to the number of threads that are in-flight and crossed the stall control
     *  check before we actually stalled.
     *  see #assertMemory()
     */
    if (stalled) {
      numDocsSinceStalled++;
    } else {
      numDocsSinceStalled = 0;
    }
    return true;
  }

  synchronized void doAfterFlush(DocumentsWriterPerThread dwpt) {
    assert flushingWriters.contains(dwpt);
    try {
      flushingWriters.remove(dwpt);
      flushBytes -= dwpt.getLastCommittedBytesUsed();
      assert assertMemory();
    } finally {
      try {
        updateStallState();
      } finally {
        notifyAll();
      }
    }
  }

  private long stallStartNS;

  // 更新暂停状态
  private boolean updateStallState() {

    assert Thread.holdsLock(this);
    final long limit = stallLimitBytes();
    /*
     * we block indexing threads if net byte grows due to slow flushes
     * yet, for small ram buffers and large documents we can easily
     * reach the limit without any ongoing flushes. we need to ensure
     * that we don't stall/block if an ongoing or pending flush can
     * not free up enough memory to release the stall lock.
     * 如果net byte 由于刷新速度较慢而增长，我们会阻塞索引线程。但是，对于小的ram缓冲区和大型文档，
     * 我们可以在不进行任何刷新的情况下轻松达到限制。如果正在进行或挂起的flush无法释放足够的内存来释放暂停锁，
     * 我们需要确保我们不会暂停/阻塞。（flush 难道不是匾额来就不会被阻塞吗？）
     */
    final boolean stall = (activeBytes + flushBytes) > limit && activeBytes < limit && !closed;

    if (infoStream.isEnabled("DWFC")) {
      if (stall != stallControl.anyStalledThreads()) {
        if (stall) { // stall==true anyStalledThreads==false 说明需要暂停
          infoStream.message(
              "DW",
              String.format(
                  Locale.ROOT,
                  "now stalling flushes: netBytes: %.1f MB flushBytes: %.1f MB fullFlush: %b",
                  netBytes() / 1024. / 1024.,
                  getFlushingBytes() / 1024. / 1024.,
                  fullFlush));
          stallStartNS = System.nanoTime();
        } else {
          infoStream.message(
              "DW",
              String.format(
                  Locale.ROOT, // stall==false anyStalledThreads==true 说明可以解除暂停
                  "done stalling flushes for %.1f msec: netBytes: %.1f MB flushBytes: %.1f MB fullFlush: %b",
                  (System.nanoTime() - stallStartNS) / (double) TimeUnit.MILLISECONDS.toNanos(1),
                  netBytes() / 1024. / 1024.,
                  getFlushingBytes() / 1024. / 1024.,
                  fullFlush));
        }
      }
    }

    stallControl.updateStalled(stall);//具体设置stall或者解除stall
    return stall;
  }

  public synchronized void waitForFlush() {
    while (flushingWriters.size() != 0) {
      try {
        this.wait();
      } catch (InterruptedException e) {
        throw new ThreadInterruptedException(e);
      }
    }
  }

  /**
   * Sets flush pending state on the given {@link DocumentsWriterPerThread}. The {@link
   * DocumentsWriterPerThread} must have indexed at least on Document and must not be already
   * pending.
   */
  public synchronized void setFlushPending(DocumentsWriterPerThread perThread) {
    assert !perThread.isFlushPending();
    if (perThread.getNumDocsInRAM() > 0) {
      perThread.setFlushPending(); // write access synced
      final long bytes = perThread.getLastCommittedBytesUsed();
      flushBytes += bytes;
      activeBytes -= bytes;
      numPending++; // write access synced
      assert assertMemory();
    } // don't assert on numDocs since we could hit an abort excp. while selecting that dwpt for
    // flushing
  }

  synchronized void doOnAbort(DocumentsWriterPerThread perThread) {
    try {
      assert perThreadPool.isRegistered(perThread);
      assert perThread.isHeldByCurrentThread();
      if (perThread.isFlushPending()) {
        flushBytes -= perThread.getLastCommittedBytesUsed();
      } else {
        activeBytes -= perThread.getLastCommittedBytesUsed();
      }
      assert assertMemory();
      // Take it out of the loop this DWPT is stale
    } finally {
      updateStallState();
      boolean checkedOut = perThreadPool.checkout(perThread);
      assert checkedOut;
    }
  }

  /** To be called only by the owner of this object's monitor lock
   * 只有拥有这个对象的monitor lock才能调用这个方法
   * */
  private void checkoutAndBlock(DocumentsWriterPerThread perThread) {
    assert Thread.holdsLock(this);
    assert perThreadPool.isRegistered(perThread);
    assert perThread.isHeldByCurrentThread();//确保被当前线程持有
    assert perThread.isFlushPending() : "can not block non-pending threadstate";//确保是pending状态
    assert fullFlush : "can not block if fullFlush == false";//确保现在在做fullFlush
    numPending--; // write access synced
    blockedFlushes.add(perThread);//添加到blockedFlushes中
    boolean checkedOut = perThreadPool.checkout(perThread);//从perThreadPool中check出来
    assert checkedOut;
  }

  private synchronized DocumentsWriterPerThread checkOutForFlush(
      DocumentsWriterPerThread perThread) {
    assert Thread.holdsLock(this);
    assert perThread.isFlushPending();//确保是FlushPending状态
    assert perThread.isHeldByCurrentThread();//确保被当先线程持有
    assert perThreadPool.isRegistered(perThread);//确保在perThreadPool中
    try {
      addFlushingDWPT(perThread);//添加到flushingWriters中, 意思是flush运行中！
      numPending--; // write access synced
      boolean checkedOut = perThreadPool.checkout(perThread);//从perThreadPool中检出
      assert checkedOut;
      return perThread;
    } finally {
      updateStallState();// 这里也会检查和更新Stall状态，只要有新的DWPT被检出，就会检查Stall状态状态
    }
  }

  private void addFlushingDWPT(DocumentsWriterPerThread perThread) {
    assert flushingWriters.contains(perThread) == false : "DWPT is already flushing";
    // Record the flushing DWPT to reduce flushBytes in doAfterFlush
    // 记录flushing DWPT 来降低flushBytes in doAfterFlush
    flushingWriters.add(perThread);
  }

  @Override
  public String toString() {
    return "DocumentsWriterFlushControl [activeBytes="
        + activeBytes
        + ", flushBytes="
        + flushBytes
        + "]";
  }

  DocumentsWriterPerThread nextPendingFlush() {
    int numPending;
    boolean fullFlush;
    // 处理 full flush 的DWPTs
    synchronized (this) {
      final DocumentsWriterPerThread poll;
      if ((poll = flushQueue.poll()) != null) {//从flushQueue中取，说明有full flush 没刷完
        updateStallState();
        return poll;
      }
      fullFlush = this.fullFlush;
      numPending = this.numPending;
    }
    // 处理非 full flush 的DWPTs
    if (numPending > 0 && fullFlush == false) { // don't check if we are doing a full flush
      for (final DocumentsWriterPerThread next : perThreadPool) {
        if (next.isFlushPending()) {
          if (next.tryLock()) {
            try {
              if (perThreadPool.isRegistered(next)) {
                return checkOutForFlush(next);// 添加到flushingWriters 并从perThreadPool中检出
              }
            } finally {
              next.unlock();
            }
          }
        }
      }
    }
    return null;
  }

  @Override
  public synchronized void close() {
    // set by DW to signal that we are closing. in this case we try to not stall any threads anymore
    // etc.
    closed = true;
  }

  /**
   * Returns an iterator that provides access to all currently active {@link
   * DocumentsWriterPerThread}s
   */
  public Iterator<DocumentsWriterPerThread> allActiveWriters() {
    return perThreadPool.iterator();
  }

  synchronized void doOnDelete() {
    // pass null this is a global delete no update
    flushPolicy.onChange(this, null);
  }

  /**
   * Returns heap bytes currently consumed by buffered deletes/updates that would be freed if we
   * pushed all deletes. This does not include bytes consumed by already pushed delete/update
   * packets.
   */
  public long getDeleteBytesUsed() {
    return documentsWriter.deleteQueue.ramBytesUsed();
  }

  @Override
  public long ramBytesUsed() {
    // TODO: improve this to return more detailed info?
    return getDeleteBytesUsed() + netBytes();
  }

  synchronized int numFlushingDWPT() {
    return flushingWriters.size();
  }

  public boolean getAndResetApplyAllDeletes() {
    return flushDeletes.getAndSet(false);
  }

  public void setApplyAllDeletes() {
    flushDeletes.set(true);
  }

  DocumentsWriterPerThread obtainAndLock() {
    while (closed == false) {
      final DocumentsWriterPerThread perThread = perThreadPool.getAndLock();
      // 判断DWPT中的deleteQueue和 DW中的deleteQueue是否相同，只有相同的情况下才会返回DWPT
      // 不相同说明此时 正在执行full flush ，但是这个DWPT还没有被checkout，此时说明当前的DWPT
      // 不能用了，我们要尽快释放DWPT锁，然后去尝试获取其他的DWPT
      // （刷新过程同样需要获取DWPT锁）
      if (perThread.deleteQueue == documentsWriter.deleteQueue) {
        // simply return the DWPT even in a flush all case since we already hold the lock and the
        // DWPT is not stale
        // since it has the current delete queue associated with it. This means we have established
        // a happens-before
        // relationship and all docs indexed into this DWPT are guaranteed to not be flushed with
        // the currently
        // progress full flush.
        return perThread;
      } else {
        try {
          // we must first assert otherwise the full flush might make progress once we unlock the
          // dwpt
          assert fullFlush && fullFlushMarkDone == false
              : "found a stale DWPT but full flush mark phase is already done fullFlush: "
                  + fullFlush
                  + " markDone: "
                  + fullFlushMarkDone;
        } finally {
          perThread.unlock();
          // There is a flush-all in process and this DWPT is
          // now stale - try another one
        }
      }
    }
    throw new AlreadyClosedException("flush control is closed");
  }
  /**
   * 标记FullFlush，切换deleteQueue，将相关WDPT标记为FlushPending状态，从perThreadPool检出
   * 添加到flushQueue 等待刷新执行，将过程中阻塞的单独刷新的DWPT也放到flushQueue
   *
   * 从fullFlush 设置为true 开始，后续因资源达到上限而触发的DWPT刷新都会被阻塞，而被放入阻塞队列
   */
  long markForFullFlush() {
    final DocumentsWriterDeleteQueue flushingQueue;
    long seqNo;
    //同步，中间涉及到deleteQueue切换和获取，以及seqNo获取，所以要加同步，防止并发干扰
    synchronized (this) {
      assert fullFlush == false
          : "called DWFC#markForFullFlush() while full flush is still running";
      assert fullFlushMarkDone == false : "full flush collection marker is still set to true";
      fullFlush = true; // 标记为full flush
      flushingQueue = documentsWriter.deleteQueue;
      // Set a new delete queue - all subsequent DWPT will use this queue until
      // we do another full flush
      // 设置一个新的delete queue，所有后续的DWPT都会使用这个新的queue，直到下一次flush
      // 阻塞NewWriters的创建，这个过程，不允许创建新的DWPT，因为DWPT的创建需要deleteQueue参数，但是
      // 在deleteQueue 切换的过程中新建，如果有DWPT创建，和数据写入，会导致下面的perThreadPool.size
      // 变化，seqNo也会计算错误！
      perThreadPool
          .lockNewWriters(); // no new thread-states while we do a flush otherwise the seqNo
      // accounting might be off
      // 当我们进行刷新时没有新的线程状态，否则seqNo 计数可能会关闭，计算seqNo的瞬间可能有新的DWPT生成，后续还可能
      // 有数据写入，同时涉及到deleteQueue的切换，这可能会造成seqNo计算失败和乱序！所以要阻止新建DWPT
      // 实现方式是 类似信号量，只有减少到0才能生成新的dwpt，否则生成就一直被阻塞
      try {
        // Insert a gap in seqNo of current active thread count, in the worst case each of those
        // threads now have one operation in flight.  It's fine
        // if we have some sequence numbers that were never assigned:
        // 在seqNo中插入一段间隙，值就是当前活跃的DWPTS，在最差的情况就是每一个DWPT都有一个操作在执行中。
        // 为什么是不可能DWPT有多个操作？ 很简单，因为这里获取是getLastSequenceNumber()，DWPT完成操作之后通过
        // getNextSequenceNumber() 递增和获取seqno，他们最终都是去拿 deletequeue 的nextSeqNo，这个值是原子的
        // 如果我们有一些从未分配过的序列号，那也没关系（因为他本身是用来表示操作完成的顺序的，只要能保证顺序不乱即可）
        DocumentsWriterDeleteQueue newQueue =
            documentsWriter.deleteQueue.advanceQueue(perThreadPool.size());
        // 同步方法，将deleteQueue 设置为 advance，生成并返回新的deleteQueue
        // 这个方法执行完之后documentsWriter 的deleteQueue还是老的，此时有可能有新的数据写入DWPT和deleteQueue
        // 因为到现在为止还没有将DWPT设置为pendingFlush 状态，也没有从perThreadPool中检出！
        seqNo = documentsWriter.deleteQueue.getMaxSeqNo();
        // 设置为新的newQueue，此方法为同步方法
        documentsWriter.resetDeleteQueue(newQueue);
        // 直接将DW老的deleteQueue替换掉，后续的删除都会被保存到新的deleteQueue
        // 如果后续还是有数据写入，则不会把持有老的deleteQueue的DWPT分配出去，
        // 所以不存在切换完了 还有数据写入老的DWPT，或者删除数据写入到了老的或者不同的deleteQueue的情况！
      } finally {
        // 信号量-1,如果到达0,则唤起被阻塞的dwpt生成过程，此后可能就会有新的dwpt生成
        perThreadPool.unlockNewWriters();
      }
    }
    // 上面处理完之后，说明老的deleteQueue 以及和其相关的DWP 都切割完了，新的数据写入不会影响到他们。
    // 下面就是要处理对应的DWPT，将相关的DWPT从perThreadPool中check出来，并放入flushQueue中
    final List<DocumentsWriterPerThread> fullFlushBuffer = new ArrayList<>();
    for (final DocumentsWriterPerThread next :
        perThreadPool.filterAndLock(dwpt -> dwpt.deleteQueue == flushingQueue)) {
      // 只处理 deleteQueue == flushingQueue 的dwpt，因为此时perThreadPool已经可能有新创建的dwpt了
      try {
        if (next.getNumDocsInRAM() > 0) {
          final DocumentsWriterPerThread flushingDWPT;
          synchronized (this) {
            if (next.isFlushPending() == false) {
              // 标记为FlushPending状态，计算指标 numPending++，flushBytes，activeBytes
              setFlushPending(next);
            }
            // 从perThreadPool(删除)中checkout出来，放到flushingWriters
            flushingDWPT = checkOutForFlush(next);
          }
          assert flushingDWPT != null
              : "DWPT must never be null here since we hold the lock and it holds documents";
          assert next == flushingDWPT : "flushControl returned different DWPT";
          fullFlushBuffer.add(flushingDWPT);
        } else {
          // it's possible that we get a DWPT with 0 docs if we flush concurrently to
          // threads getting DWPTs from the pool. In this case we simply remove it from
          // the pool and drop it on the floor.
          // 如果DWPT只有0个doc，我们只需要把他们从perThreadPool中删除即可
          boolean checkout = perThreadPool.checkout(next);
          assert checkout;//解锁
        }
      } finally {
        next.unlock();
      }
    }
    synchronized (this) {
      /* make sure we move all DWPT that are where concurrently marked as
       * pending and moved to blocked are moved over to the flushQueue. There is
       * a chance that this happens since we marking DWPT for full flush without
       * blocking indexing.
       * 确认我们把所有的，被并发标记为pending并且移动到blocked的DWPT，都再移动到flushQueue中。
       * 这种情况有可能会发生，因为我们在不阻塞索引的情况下将DWPT标记为完全刷新。我们在上面说过，在
       * 标记为flush之后，切换deleteQueue之前，数据还是可以写入的此时可能会有DWPT flush，但是
       * 会被放入阻塞队列blockedQueue中。
       * */
      pruneBlockedQueue(flushingQueue);
      assert assertBlockedFlushes(documentsWriter.deleteQueue);
      flushQueue.addAll(fullFlushBuffer);// 移动到flushQueue
      updateStallState();//检查和更新Stall状态
      fullFlushMarkDone = //标记为full mark Done
          true; // at this point we must have collected all DWPTs that belong to the old delete
      // queue
      // 在这个时间点，我们必须收集完所有的隶属于老的delete queue 的 DWPTs！
    }
    assert assertActiveDeleteQueue(documentsWriter.deleteQueue);
    assert flushingQueue.getLastSequenceNumber() <= flushingQueue.getMaxSeqNo();
    return seqNo;
  }

  private boolean assertActiveDeleteQueue(DocumentsWriterDeleteQueue queue) {
    for (final DocumentsWriterPerThread next : perThreadPool) {
      assert next.deleteQueue == queue : "numDocs: " + next.getNumDocsInRAM();
    }
    return true;
  }

  /**
   * Prunes the blockedQueue by removing all DWPTs that are associated with the given flush queue.
   * 通过移除所有的和给定的flush queue 相关的DWPTs，来清理和裁减blockedQueue
   */
  private void pruneBlockedQueue(final DocumentsWriterDeleteQueue flushingQueue) {
    assert Thread.holdsLock(this);
    Iterator<DocumentsWriterPerThread> iterator = blockedFlushes.iterator();
    while (iterator.hasNext()) {
      DocumentsWriterPerThread blockedFlush = iterator.next();
      if (blockedFlush.deleteQueue == flushingQueue) {
        iterator.remove();// 从blockedFlushes中删除
        addFlushingDWPT(blockedFlush);// 添加到flushingWriters中
        // don't decr pending here - it's already done when DWPT is blocked
        // 不需要递减 pending 的数量 - 因为在DWPT被阻塞时就已经做过了
        flushQueue.add(blockedFlush);// 添加到flushQueue中
      }
    }
  }

  synchronized void finishFullFlush() {
    assert fullFlush;
    assert flushQueue.isEmpty();
    assert flushingWriters.isEmpty();
    try {
      if (!blockedFlushes.isEmpty()) {
        assert assertBlockedFlushes(documentsWriter.deleteQueue);
        //把block阻塞的DWPT，添加到flushQueue，注意当前的deleteQueue是新的，是被advance过的
        pruneBlockedQueue(documentsWriter.deleteQueue);
        assert blockedFlushes.isEmpty();
      }
    } finally {
      fullFlushMarkDone = fullFlush = false; // fullFlush状态标记为false

      updateStallState();//更新Stall状态，唤醒所有被阻塞indexing线程
    }
  }

  boolean assertBlockedFlushes(DocumentsWriterDeleteQueue flushingQueue) {
    for (DocumentsWriterPerThread blockedFlush : blockedFlushes) {
      assert blockedFlush.deleteQueue == flushingQueue;
    }
    return true;
  }

  synchronized void abortFullFlushes() {
    try {
      abortPendingFlushes();
    } finally {
      fullFlushMarkDone = fullFlush = false;
    }
  }

  synchronized void abortPendingFlushes() {
    try {
      for (DocumentsWriterPerThread dwpt : flushQueue) {
        try {
          documentsWriter.subtractFlushedNumDocs(dwpt.getNumDocsInRAM());
          dwpt.abort();
        } catch (
            @SuppressWarnings("unused")
            Exception ex) {
          // that's fine we just abort everything here this is best effort
        } finally {
          doAfterFlush(dwpt);
        }
      }
      for (DocumentsWriterPerThread blockedFlush : blockedFlushes) {
        try {
          addFlushingDWPT(
              blockedFlush); // add the blockedFlushes for correct accounting in doAfterFlush
          documentsWriter.subtractFlushedNumDocs(blockedFlush.getNumDocsInRAM());
          blockedFlush.abort();
        } catch (
            @SuppressWarnings("unused")
            Exception ex) {
          // that's fine we just abort everything here this is best effort
        } finally {
          doAfterFlush(blockedFlush);
        }
      }
    } finally {
      flushQueue.clear();
      blockedFlushes.clear();
      updateStallState();
    }
  }

  /** Returns <code>true</code> if a full flush is currently running */
  synchronized boolean isFullFlush() {
    return fullFlush;
  }

  /** Returns the number of flushes that are already checked out but not yet actively flushing */
  synchronized int numQueuedFlushes() {
    return flushQueue.size();
  }

  /**
   * Returns the number of flushes that are checked out but not yet available for flushing. This
   * only applies during a full flush if a DWPT needs flushing but must not be flushed until the
   * full flush has finished.
   */
  synchronized int numBlockedFlushes() {
    return blockedFlushes.size();
  }

  /**
   * This method will block if too many DWPT are currently flushing and no checked out DWPT are
   * available
   * 这个方法会被阻塞，如果有太多DWPT在并发刷新而且没有可用的检出的DWPT。
   */
  void waitIfStalled() {
    stallControl.waitIfStalled();
  }

  /** Returns <code>true</code> iff stalled */
  boolean anyStalledThreads() {
    return stallControl.anyStalledThreads();
  }

  /** Returns the {@link IndexWriter} {@link InfoStream} */
  public InfoStream getInfoStream() {
    return infoStream;
  }

  synchronized DocumentsWriterPerThread findLargestNonPendingWriter() {
    DocumentsWriterPerThread maxRamUsingWriter = null;
    // Note: should be initialized to -1 since some DWPTs might return 0 if their RAM usage has not
    // been committed yet.
    long maxRamSoFar = -1;
    int count = 0;
    for (DocumentsWriterPerThread next : perThreadPool) {
      if (next.isFlushPending() == false && next.getNumDocsInRAM() > 0) {
        final long nextRam = next.getLastCommittedBytesUsed();
        if (infoStream.isEnabled("FP")) {
          infoStream.message(
              "FP", "thread state has " + nextRam + " bytes; docInRAM=" + next.getNumDocsInRAM());
        }
        count++;
        if (nextRam > maxRamSoFar) {
          maxRamSoFar = nextRam;
          maxRamUsingWriter = next;
        }
      }
    }
    if (infoStream.isEnabled("FP")) {
      infoStream.message("FP", count + " in-use non-flushing threads states");
    }
    return maxRamUsingWriter;
  }

  /** Returns the largest non-pending flushable DWPT or <code>null</code> if there is none.
   * 返回最大的non-pending状态的DWPT，会先将其标记为FlushPending状态，并从perThreadPool checkout 出来
   * */
  final DocumentsWriterPerThread checkoutLargestNonPendingWriter() {
    DocumentsWriterPerThread largestNonPendingWriter = findLargestNonPendingWriter();
    if (largestNonPendingWriter != null) {
      // we only lock this very briefly to swap it's DWPT out - we don't go through the DWPTPool and
      // it's free queue
      // 我们仅仅上锁非常短暂的时间就可以将DWPT换出，我们不通过DWPTPool和他的queue
      largestNonPendingWriter.lock();
      try {
        if (perThreadPool.isRegistered(largestNonPendingWriter)) {
          synchronized (this) {
            try {
              return checkout(
                  largestNonPendingWriter, largestNonPendingWriter.isFlushPending() == false);
            } finally {
              updateStallState();
            }
          }
        }
      } finally {
        largestNonPendingWriter.unlock();
      }
    }
    return null;
  }

  long getPeakActiveBytes() {
    return peakActiveBytes;
  }

  long getPeakNetBytes() {
    return peakNetBytes;
  }
}
