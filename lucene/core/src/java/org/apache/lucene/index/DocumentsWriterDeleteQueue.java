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
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import org.apache.lucene.index.DocValuesUpdate.BinaryDocValuesUpdate;
import org.apache.lucene.index.DocValuesUpdate.NumericDocValuesUpdate;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.InfoStream;

/**
 * {@link DocumentsWriterDeleteQueue} is a non-blocking linked pending deletes queue. In contrast to
 * other queue implementation we only maintain the tail of the queue. A delete queue is always used
 * in a context of a set of DWPTs and a global delete pool. Each of the DWPT and the global pool
 * need to maintain their 'own' head of the queue (as a DeleteSlice instance per {@link
 * DocumentsWriterPerThread}). The difference between the DWPT and the global pool is that the DWPT
 * starts maintaining a head once it has added its first document since for its segments private
 * deletes only the deletes after that document are relevant. The global pool instead starts
 * maintaining the head once this instance is created by taking the sentinel instance as its initial
 * head.
 *
 * <p>Since each {@link DeleteSlice} maintains its own head and the list is only single linked the
 * garbage collector takes care of pruning the list for us. All nodes in the list that are still
 * relevant should be either directly or indirectly referenced by one of the DWPT's private {@link
 * DeleteSlice} or by the global {@link BufferedUpdates} slice.
 *
 * <p>Each DWPT as well as the global delete pool maintain their private DeleteSlice instance. In
 * the DWPT case updating a slice is equivalent to atomically finishing the document. The slice
 * update guarantees a "happens before" relationship to all other updates in the same indexing
 * session. When a DWPT updates a document it:
 *
 * <ol>
 *   <li>consumes a document and finishes its processing
 *   <li>updates its private {@link DeleteSlice} either by calling {@link #updateSlice(DeleteSlice)}
 *       or {@link #add(Node, DeleteSlice)} (if the document has a delTerm)
 *   <li>applies all deletes in the slice to its private {@link BufferedUpdates} and resets it
 *   <li>increments its internal document id
 * </ol>
 *
 * The DWPT also doesn't apply its current documents delete term until it has updated its delete
 * slice which ensures the consistency of the update. If the update fails before the DeleteSlice
 * could have been updated the deleteTerm will also not be added to its private deletes neither to
 * the global deletes.
 *
 * DocumentsWriterDelete队列是一个非阻塞队列，把pending deletes （后续需要做的删除）连接起来。
 * 与其他队列的实现方式相比，区别是，这个队列只维护队列的尾部。
 * 一个删除队列总是在包含一组DWPT和global delete pool组成的上下文中使用，后面解释。每一个DWPT和global pool都需要维护
 * 它们自己的头，当然这个头是delete queue中的一个节点。DWPT和global pool之间的区别在于，DWPT在添加了
 * 第一个文档后开始维护头（初始化的值是当前delete queue的尾），因为对于其segments，私有删除只有在有文档之后才需要进行。
 * 因为在这之前的删除和这个新的DWPT或者说segments都没有关系，global pool通过将哨兵实例
 * 作为其初始头，在创建此实例后就开始维护头。
 *
 * 由于每个DeleteSlice实例维护自己的头，delete queue中没有头，所有的 DeleteSlice 的头之前的 Node 是没有引用的，
 * 所以垃圾收集器会负责为我们裁减列表。
 * 并且列表中仍然相关的所有节点都还由DWPT的私有DeleteSlice直接或间接引用或被全局BufferedUpdates切片引用。
 *
 * 每个DWPT以及global delete pool都维护其专用DeleteSlice实例。在DWPT的情况下，更新slice相当于/说明原子地完成了doc。
 * slice更新保证了与同一indexing session中的所有其他updates之间的“先发生后发生”关系。DWPT更新文档时：
 * 1. 消费一个文档并完成对他的处理
 * 2. 更新其私有的DeleteSlice，方法是调用updateSlice（DeleteSlice）或add（Node，DeleteSlice）（如果文档具有delTerm）
 * 3. 将slice中的所有删除应用于其私有的BufferedUpdates并重置它
 * 4. 增加其内部文档id，其实是seqno，就是这个seqno 就代表了所谓的先后关系
 *
 * 如上步骤，DWPT也不会应用其当前文档delete term，直到它更新了其delete slice，以确保更新的一致性。
 * 如果在DeleteSlice更新之前更新失败，deleteTerm也不会添加到其private deletes，也不会添加到global deletes，
 * 但是会通过其他方式删除，从而才能保证真正的一致性。
 */
final class DocumentsWriterDeleteQueue implements Accountable, Closeable {

  // the current end (latest delete operation) in the delete queue:
  // 当前的delete队列尾部
  private volatile Node<?> tail;

  private volatile boolean closed = false;

  /** 用于记录对所有先前（已写入磁盘）段的deletes。每当任何段刷新时，我们都会和这组个deletes 集合捆绑在一起，
   * 并在新刷新的段之前插入到缓冲的更新流中。
   * Used to record deletes against all prior (already written to disk) segments. Whenever any
   * segment flushes, we bundle up this set of deletes and insert into the buffered updates stream
   * before the newly flushed segment(s).
   */
  private final DeleteSlice globalSlice;

  private final BufferedUpdates globalBufferedUpdates; //这里面的数据都是从globalSlice中添加过来的

  // only acquired to update the global deletes, pkg-private for access by tests:
  final ReentrantLock globalBufferLock = new ReentrantLock();

  final long generation;

  /**
   * Generates the sequence number that IW returns to callers changing the index, showing the
   * effective serialization of all operations.
   * 生成IW修改索引的序列号，并返回给调用者，显示所有操作的有效序列化。这个是全局的，一个操作一个编号（一个操作可能有多个doc）
   */
  private final AtomicLong nextSeqNo;

  private final InfoStream infoStream;

  private volatile long maxSeqNo = Long.MAX_VALUE;

  private final long startSeqNo;
  private final LongSupplier previousMaxSeqId;
  private boolean advanced;

  DocumentsWriterDeleteQueue(InfoStream infoStream) {
    // seqNo must start at 1 because some APIs negate this to also return a boolean
    this(infoStream, 0, 1, () -> 0);
  }

  private DocumentsWriterDeleteQueue(
      InfoStream infoStream, long generation, long startSeqNo, LongSupplier previousMaxSeqId) {
    this.infoStream = infoStream;
    this.globalBufferedUpdates = new BufferedUpdates("global");
    this.generation = generation;
    this.nextSeqNo = new AtomicLong(startSeqNo);
    this.startSeqNo = startSeqNo;
    this.previousMaxSeqId = previousMaxSeqId;
    long value = previousMaxSeqId.getAsLong();
    assert value <= startSeqNo : "illegal max sequence ID: " + value + " start was: " + startSeqNo;
    /*
     * we use a sentinel instance as our initial tail. No slice will ever try to
     * apply this tail since the head is always omitted.
     */
    tail = new Node<>(null); // sentinel
    globalSlice = new DeleteSlice(tail);
  }

  long addDelete(Query... queries) {
    long seqNo = add(new QueryArrayNode(queries));
    tryApplyGlobalSlice();
    return seqNo;
  }

  long addDelete(Term... terms) {
    long seqNo = add(new TermArrayNode(terms));// 将Node添加到队尾
    tryApplyGlobalSlice();// 将 globalSlice 应用到 globalBufferedUpdates 中，并将globalSlice 头尾重置
    return seqNo;
  }

  long addDocValuesUpdates(DocValuesUpdate... updates) {
    long seqNo = add(new DocValuesUpdatesNode(updates));
    tryApplyGlobalSlice();
    return seqNo;
  }

  static Node<Term> newNode(Term term) {
    return new TermNode(term);
  }

  static Node<DocValuesUpdate[]> newNode(DocValuesUpdate... updates) {
    return new DocValuesUpdatesNode(updates);
  }

  /** invariant for document update */
  long add(Node<?> deleteNode, DeleteSlice slice) {
    long seqNo = add(deleteNode);// 先添加到queue中
    /*
     * this is an update request where the term is the updated documents
     * delTerm. in that case we need to guarantee that this insert is atomic
     * with regards to the given delete slice. This means if two threads try to
     * update the same document with in turn the same delTerm one of them must
     * win. By taking the node we have created for our del term as the new tail
     * it is guaranteed that if another thread adds the same right after us we
     * will apply this delete next time we update our slice and one of the two
     * competing updates wins!
     * 在这种情况下，我们需要保证这个添加node对于给定的删除切片是原子的。这意味着如果有两个线程想要根据相同的delTerm做更新，
     * 其中一个一定成功。通过把delTerm node 作为新的尾部，可以保证如果另一个线程在我们之后添加相同的delTerm node，
     * 我们将在下次更新切片时应用此删除，并且两个竞争更新中的一个获胜。
     */
    slice.sliceTail = deleteNode;// 更新slice的tail，和queue保持一致
    assert slice.sliceHead != slice.sliceTail : "slice head and tail must differ after add";
    tryApplyGlobalSlice(); // TODO doing this each time is not necessary maybe
    // we can do it just every n times or so?
    // 更新全局的Slice 并应用到全局的updates

    return seqNo;
  }

  synchronized long add(Node<?> newNode) {
    ensureOpen();
    tail.next = newNode;// 把node加在队尾
    this.tail = newNode;
    return getNextSequenceNumber();
  }

  boolean anyChanges() {
    globalBufferLock.lock();
    try {
      /*
       * check if all items in the global slice were applied
       * and if the global slice is up-to-date
       * and if globalBufferedUpdates has changes
       */
      return globalBufferedUpdates.any()
          || !globalSlice.isEmpty()
          || globalSlice.sliceTail != tail
          || tail.next != null;
    } finally {
      globalBufferLock.unlock();
    }
  }

  void tryApplyGlobalSlice() {
    if (globalBufferLock.tryLock()) {
      ensureOpen();
      /*
       * global buffer 必须上锁，但如果现在有正在进行的更新，我们不需要更新它们。下次我们可以获得锁定时，
       * 只需把，从当前运行中的global slices 的尾部，之后，添加的deletes 应用一下即可！
       * The global buffer must be locked but we don't need to update them if
       * there is an update going on right now. It is sufficient to apply the
       * deletes that have been added after the current in-flight global slices
       * tail the next time we can get the lock!
       */
      try {
        // 如果globalSlice有变化，则把globalSlice中的Node应用到globalBufferedUpdates，
        // 也就是会把globalSlice中所有的item 都添加到globalBufferedUpdates中
        // 并且应用完之后会将globalSlice 中的头尾重置，head=tail
        // 也就是说下次截取是从这次的尾部开始
        if (updateSliceNoSeqNo(globalSlice)) {
          globalSlice.apply(globalBufferedUpdates, BufferedUpdates.MAX_INT);
        }
      } finally {
        globalBufferLock.unlock();
      }
    }
  }

  FrozenBufferedUpdates freezeGlobalBuffer(DeleteSlice callerSlice) {
    globalBufferLock.lock();
    try {
      ensureOpen();
      /*
       * Here we freeze the global buffer so we need to lock it, apply all
       * deletes in the queue and reset the global slice to let the GC prune the
       * queue.
       * 这里冻结global buffer 所以我们需要加锁，应用queue中的所有的deletes
       * 并且重置global slice以便GC可以回收queue
       */
      // 暂存tail，等会更新传入的局部的callerSlice 的tail
      final Node<?> currentTail = tail;//take the current tail make this local any
      // Changes after this call are applied later
      // and not relevant here
      // 获取当前尾部，使这个为本地的。此调用之后的任何更改稍后都会应用，与此处无关
      if (callerSlice != null) {
        // Update the callers slices so we are on the same page
        callerSlice.sliceTail = currentTail;//更新给定的DWPT中局部slice的tail
      }
      return freezeGlobalBufferInternal(currentTail);//应用 globalSlice 中的删除
    } finally {
      globalBufferLock.unlock();
    }
  }

  /**
   * This may freeze the global buffer unless the delete queue has already been closed. If the queue
   * has been closed this method will return <code>null</code>
   * 这里会冻结 global buffer，除非delete queue已经被关闭。
   * 如果queue已经被关闭这个方法会返回 null。
   * 跟上面的函数差不多，只不过调用这个函数一般说明，不需要处理DWPT的局部删除。
   */
  FrozenBufferedUpdates maybeFreezeGlobalBuffer() {
    globalBufferLock.lock();
    try {
      if (closed == false) {
        /*
         * Here we freeze the global buffer so we need to lock it, apply all
         * deletes in the queue and reset the global slice to let the GC prune the
         * queue.
         * 这里冻结global buffer 所以我们需要加锁，应用queue中的所有的deletes
         * 并且重置global slice以便GC可以回收queue
         */
        return freezeGlobalBufferInternal(tail); // take the current tail make this local any
      } else {
        assert anyChanges() == false : "we are closed but have changes";
        return null;
      }
    } finally {
      globalBufferLock.unlock();
    }
  }

  private FrozenBufferedUpdates freezeGlobalBufferInternal(final Node<?> currentTail) {
    assert globalBufferLock.isHeldByCurrentThread();
    if (globalSlice.sliceTail != currentTail) {
      globalSlice.sliceTail = currentTail;
      globalSlice.apply(globalBufferedUpdates, BufferedUpdates.MAX_INT);//应用globalSlice 中的删除
    }

    if (globalBufferedUpdates.any()) {
      final FrozenBufferedUpdates packet =
          new FrozenBufferedUpdates(infoStream, globalBufferedUpdates, null);
      globalBufferedUpdates.clear();//这里直接清空了
      return packet;
    } else {
      return null;
    }
  }

  DeleteSlice newSlice() {
    return new DeleteSlice(tail);
  }

  /** Negative result means there were new deletes since we last applied
   * 如果结果是负数意味着自从我们上次applied 又有新的delets了。
   * */
  synchronized long updateSlice(DeleteSlice slice) {
    ensureOpen();
    long seqNo = getNextSequenceNumber();
    if (slice.sliceTail != tail) {// slice 的tail 和当前 的tail不相同，说明又有新的delets了
      // new deletes arrived since we last checked
      slice.sliceTail = tail;// 修改slice 的tail 为当前的 tail
      seqNo = -seqNo;
    }
    return seqNo;
  }

  /**
   * 和updateSlice类似，但是不分配sequence number
   * Just like updateSlice, but does not assign a sequence number */
  boolean updateSliceNoSeqNo(DeleteSlice slice) {
    if (slice.sliceTail != tail) {
      // new deletes arrived since we last checked
      slice.sliceTail = tail;//对齐，将slice的队尾部设置成queue的尾部
      return true;
    }
    return false;
  }

  private void ensureOpen() {
    if (closed) {
      throw new AlreadyClosedException(
          "This " + DocumentsWriterDeleteQueue.class.getSimpleName() + " is already closed");
    }
  }

  public boolean isOpen() {
    return closed == false;
  }

  @Override
  public synchronized void close() {
    globalBufferLock.lock();
    try {
      if (anyChanges()) {
        throw new IllegalStateException("Can't close queue unless all changes are applied");
      }
      this.closed = true;
      long seqNo = nextSeqNo.get();
      assert seqNo <= maxSeqNo
          : "maxSeqNo must be greater or equal to " + seqNo + " but was " + maxSeqNo;
      nextSeqNo.set(maxSeqNo + 1);
    } finally {
      globalBufferLock.unlock();
    }
  }

  static class DeleteSlice {
    // No need to be volatile, slices are thread captive (only accessed by one thread)!
    // 不需要定义成volatile，slices都是线程私有的
    Node<?> sliceHead; // we don't apply this one
    Node<?> sliceTail;

    DeleteSlice(Node<?> currentTail) {
      assert currentTail != null;
      /*
       * Initially this is a 0 length slice pointing to the 'current' tail of
       * the queue. Once we update the slice we only need to assign the tail and
       * have a new slice
       */
      sliceHead = sliceTail = currentTail;
    }

    void apply(BufferedUpdates del, int docIDUpto) {
      if (sliceHead == sliceTail) {
        // 0 length slice
        return;
      }
      /* 当我们应用一个切片slice时，我们先取头部然后获取他的next作为我们的第一个term来应用，
       * 然后继续遍历后面的term，直到尾部。如果该切片中的头和尾不相等，那么该切片中至少还会有一个非空节点！
       * When we apply a slice we take the head and get its next as our first
       * item to apply and continue until we applied the tail. If the head and
       * tail in this slice are not equal then there will be at least one more
       * non-null node in the slice!
       */
      Node<?> current = sliceHead;
      do {
        current = current.next;
        assert current != null
            : "slice property violated between the head on the tail must not be a null node";
        // 这里会将Node中的item, 添加到 BufferedUpdates中，如果是删除的item会添加到BufferedUpdates中的 删除相关的map
        current.apply(del, docIDUpto);// 一个slice 更新buffer时，docIDUpto都相同，所以对应的就是同一条doc的写入的多个term
      } while (current != sliceTail);
      reset();// 应用完之后将当前DeleteSlice重置
    }

    void reset() {
      // Reset to a 0 length slice
      sliceHead = sliceTail;// 从尾部开始，重新截取
    }

    /**
     * Returns <code>true</code> iff the given node is identical to the slices tail, otherwise
     * <code>false</code>.
     */
    boolean isTail(Node<?> node) {
      return sliceTail == node;
    }

    /**
     * Returns <code>true</code> iff the given item is identical to the item hold by the slices
     * tail, otherwise <code>false</code>.
     */
    boolean isTailItem(Object object) {
      return sliceTail.item == object;
    }

    boolean isEmpty() {
      return sliceHead == sliceTail;
    }
  }

  public int numGlobalTermDeletes() {
    return globalBufferedUpdates.numTermDeletes.get();
  }

  void clear() {
    globalBufferLock.lock();
    try {
      final Node<?> currentTail = tail;
      globalSlice.sliceHead = globalSlice.sliceTail = currentTail;
      globalBufferedUpdates.clear();
    } finally {
      globalBufferLock.unlock();
    }
  }

  static class Node<T> {
    volatile Node<?> next;
    final T item;

    Node(T item) {
      this.item = item;
    }

    void apply(BufferedUpdates bufferedDeletes, int docIDUpto) {
      throw new IllegalStateException("sentinel item must never be applied");
    }

    boolean isDelete() {
      return true;
    }
  }

  private static final class TermNode extends Node<Term> {

    TermNode(Term term) {
      super(term);
    }

    @Override
    void apply(BufferedUpdates bufferedDeletes, int docIDUpto) {
      bufferedDeletes.addTerm(item, docIDUpto);// 把当前item 添加到buffer中，这里面会添加到删除的map
    }// 这里添加到了删除buffer中

    @Override
    public String toString() {
      return "del=" + item;
    }
  }

  private static final class QueryArrayNode extends Node<Query[]> {
    QueryArrayNode(Query[] query) {
      super(query);
    }

    @Override
    void apply(BufferedUpdates bufferedUpdates, int docIDUpto) {
      for (Query query : item) {
        bufferedUpdates.addQuery(query, docIDUpto);//这里添加到更新buffer
      }
    }
  }

  private static final class TermArrayNode extends Node<Term[]> {
    TermArrayNode(Term[] term) {
      super(term);
    }

    @Override
    void apply(BufferedUpdates bufferedUpdates, int docIDUpto) {
      for (Term term : item) {
        bufferedUpdates.addTerm(term, docIDUpto);//这里添加到更新buffer
      }
    }

    @Override
    public String toString() {
      return "dels=" + Arrays.toString(item);
    }
  }

  private static final class DocValuesUpdatesNode extends Node<DocValuesUpdate[]> {

    DocValuesUpdatesNode(DocValuesUpdate... updates) {
      super(updates);
    }

    @Override
    void apply(BufferedUpdates bufferedUpdates, int docIDUpto) {
      for (DocValuesUpdate update : item) {
        switch (update.type) {
          case NUMERIC:
            bufferedUpdates.addNumericUpdate((NumericDocValuesUpdate) update, docIDUpto);
            break;
          case BINARY:
            bufferedUpdates.addBinaryUpdate((BinaryDocValuesUpdate) update, docIDUpto);
            break;
          case NONE:
          case SORTED:
          case SORTED_SET:
          case SORTED_NUMERIC:
          default:
            throw new IllegalArgumentException(
                update.type + " DocValues updates not supported yet!");
        }
      }
    }

    @Override
    boolean isDelete() {
      return false;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("docValuesUpdates: ");
      if (item.length > 0) {
        sb.append("term=").append(item[0].term).append("; updates: [");
        for (DocValuesUpdate update : item) {
          sb.append(update.field).append(':').append(update.valueToString()).append(',');
        }
        sb.setCharAt(sb.length() - 1, ']');
      }
      return sb.toString();
    }
  }

  public int getBufferedUpdatesTermsSize() {
    final ReentrantLock lock = globalBufferLock; // Trusted final
    lock.lock();
    try {
      final Node<?> currentTail = tail;
      if (globalSlice.sliceTail != currentTail) {
        globalSlice.sliceTail = currentTail;
        globalSlice.apply(globalBufferedUpdates, BufferedUpdates.MAX_INT);
      }
      return globalBufferedUpdates.deleteTerms.size();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public long ramBytesUsed() {
    return globalBufferedUpdates.ramBytesUsed();
  }

  @Override
  public String toString() {
    return "DWDQ: [ generation: " + generation + " ]";
  }

  public long getNextSequenceNumber() {
    long seqNo = nextSeqNo.getAndIncrement();
    assert seqNo <= maxSeqNo : "seqNo=" + seqNo + " vs maxSeqNo=" + maxSeqNo;
    return seqNo;
  }

  long getLastSequenceNumber() {
    return nextSeqNo.get() - 1;
  }

  /**
   * Inserts a gap in the sequence numbers. This is used by IW during flush or commit to ensure any
   * in-flight threads get sequence numbers inside the gap
   */
  void skipSequenceNumbers(long jump) {
    nextSeqNo.addAndGet(jump);
  }

  /** Returns the maximum completed seq no for this queue. */
  long getMaxCompletedSeqNo() {
    if (startSeqNo < nextSeqNo.get()) {
      return getLastSequenceNumber();
    } else {
      // if we haven't advanced the seqNo make sure we fall back to the previous queue
      long value = previousMaxSeqId.getAsLong();
      assert value < startSeqNo : "illegal max sequence ID: " + value + " start was: " + startSeqNo;
      return value;
    }
  }

  // we use a static method to get this lambda since we previously introduced a memory leak since it
  // would
  // implicitly reference this.nextSeqNo which holds on to this del queue. see LUCENE-9478 for
  // reference
  private static LongSupplier getPrevMaxSeqIdSupplier(AtomicLong nextSeqNo) {
    return () -> nextSeqNo.get() - 1;
  }

  /**
   * Advances the queue to the next queue on flush. This carries over the the generation to the next
   * queue and set the {@link #getMaxSeqNo()} based on the given maxNumPendingOps. This method can
   * only be called once, subsequently the returned queue should be used.
   *
   * 在flush时，将DeleteQueue转换为下一个DeleteQueue。这会携带gen代到下一个queue并且基于给定的maxNumPendingOps
   * 设置getMaxSeqNo()。这个方法只能被调用一次，后续需要使用返回的queue。
   * @param maxNumPendingOps the max number of possible concurrent operations that will execute on
   *     this queue after it was advanced. This corresponds the the number of DWPTs that own the
   *     current queue at the moment when this queue is advanced since each these DWPTs can
   *     increment the seqId after we advanced it.
   * queue完成advanced之后，可能在这个queue中执行的并发操作的最大数量。这对应于当前队列advanced时拥有该队列的DWPT的数量，
   * 因为在我们advanced后，每个DWPT都可以增加seqId。
   * @return a new queue as a successor of this queue.
   */
  synchronized DocumentsWriterDeleteQueue advanceQueue(int maxNumPendingOps) {
    if (advanced) {
      throw new IllegalStateException("queue was already advanced");
    }
    advanced = true;
    long seqNo = getLastSequenceNumber() + maxNumPendingOps + 1;
    maxSeqNo = seqNo;//
    return new DocumentsWriterDeleteQueue(
        infoStream,
        generation + 1,
        seqNo + 1,
        // don't pass ::getMaxCompletedSeqNo here b/c otherwise we keep an reference to this queue
        // and this will be a memory leak since the queues can't be GCed
        // 不要使用 ::getMaxCompletedSeqNo 这种 lambda 表达式，这会导致我们在外部持有一个对这个queue的引用，
        // 并且这会因为无法Gc从而导致内存泄漏
        getPrevMaxSeqIdSupplier(nextSeqNo));
  }

  /**
   * Returns the maximum sequence number for this queue. This value will change once this queue is
   * advanced.
   */
  long getMaxSeqNo() {
    return maxSeqNo;
  }

  /** Returns <code>true</code> if it was advanced. */
  boolean isAdvanced() {
    return advanced;
  }
}
