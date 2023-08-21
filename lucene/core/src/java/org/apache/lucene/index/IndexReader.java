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
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DocumentStoredFieldVisitor;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.Bits;

/**
 * IndexReader is an abstract class, providing an interface for accessing a point-in-time view of an
 * index. Any changes made to the index via {@link IndexWriter} will not be visible until a new
 * {@code IndexReader} is opened. It's best to use {@link DirectoryReader#open(IndexWriter)} to
 * obtain an {@code IndexReader}, if your {@link IndexWriter} is in-process. When you need to
 * re-open to see changes to the index, it's best to use {@link
 * DirectoryReader#openIfChanged(DirectoryReader)} since the new reader will share resources with
 * the previous one when possible. Search of an index is done entirely through this abstract
 * interface, so that any subclass which implements it is searchable.
 * 一个时间点只能看到一个可用的index, 任何对 index 修改都需要打开一个新的IndexReader，或者重新打开（比如调用openIfChanged）之后才能被看到
 * <p>There are two different types of IndexReaders:
 * 组合模式，树形结构
 * <ul>
 *   <li>{@link LeafReader}: These indexes do not consist of several sub-readers, they are atomic.
 *       They support retrieval of stored fields, doc values, terms, and postings.//叶子Reader
 *   <li>{@link CompositeReader}: Instances (like {@link DirectoryReader}) of this reader can only
 *       be used to get stored fields from the underlying LeafReaders, but it is not possible to
 *       directly retrieve postings. To do that, get the sub-readers via {@link
 *       CompositeReader#getSequentialSubReaders}.//复合 Reader只能获取到fields，并且是从底层的LeafReaders获取的
 * </ul>
 *
 * <p>IndexReader instances for indexes on disk are usually constructed with a call to one of the
 * static <code>DirectoryReader.open()</code> methods, e.g. {@link
 * DirectoryReader#open(org.apache.lucene.store.Directory)}. {@link DirectoryReader} implements the
 * {@link CompositeReader} interface, it is not possible to directly get postings.
 * IndexReader实例一般都是通过地爱用一些静态函数来创建的，比如。。。。。。
 * <p>For efficiency, in this API documents are often referred to via <i>document numbers</i>,
 * non-negative integers which each name a unique document in the index. These document numbers are
 * ephemeral -- they may change as documents are added to and deleted from an index. Clients should
 * thus not rely on a given document having the same number between sessions.
 * 为了效率，文档又有一些编号作为索引，但是这些编号是短暂的，当文档被添加或者删除时，他们可能会修改。
 * <p><a id="thread-safety"></a>
 *
 * <p><b>NOTE</b>: {@link IndexReader} instances are completely thread safe, meaning multiple
 * threads can call any of its methods, concurrently. If your application requires external
 * synchronization, you should <b>not</b> synchronize on the <code>IndexReader</code> instance; use
 * your own (non-Lucene) objects instead.
 */
public abstract class IndexReader implements Closeable {

  private boolean closed = false;
  private boolean closedByChild = false;
  private final AtomicInteger refCount = new AtomicInteger(1);

  IndexReader() {
    if (!(this instanceof CompositeReader || this instanceof LeafReader))
      throw new Error(
          "IndexReader should never be directly extended, subclass LeafReader or CompositeReader instead.");
  }

  /**
   * A utility class that gives hooks in order to help build a cache based on the data that is
   * contained in this index.
   *
   * <p>Example: cache the number of documents that match a query per reader.
   *
   * <pre class="prettyprint">
   * public class QueryCountCache {
   *
   *   private final Query query;
   *   private final Map&lt;IndexReader.CacheKey, Integer&gt; counts = new ConcurrentHashMap&lt;&gt;();
   *
   *   // Create a cache of query counts for the given query
   *   public QueryCountCache(Query query) {
   *     this.query = query;
   *   }
   *
   *   // Count the number of matches of the query on the given IndexSearcher
   *   public int count(IndexSearcher searcher) throws IOException {
   *     IndexReader.CacheHelper cacheHelper = searcher.getIndexReader().getReaderCacheHelper();
   *     if (cacheHelper == null) {
   *       // reader doesn't support caching
   *       return searcher.count(query);
   *     } else {
   *       // make sure the cache entry is cleared when the reader is closed
   *       cacheHelper.addClosedListener(counts::remove);
   *       return counts.computeIfAbsent(cacheHelper.getKey(), cacheKey -&gt; {
   *         try {
   *           return searcher.count(query);
   *         } catch (IOException e) {
   *           throw new UncheckedIOException(e);
   *         }
   *       });
   *     }
   *   }
   *
   * }
   * </pre>
   *
   * @lucene.experimental
   */
  public static interface CacheHelper {

    /**
     * Get a key that the resource can be cached on. The given entry can be compared using identity,
     * ie. {@link Object#equals} is implemented as {@code ==} and {@link Object#hashCode} is
     * implemented as {@link System#identityHashCode}.
     */
    CacheKey getKey();

    /**
     * Add a {@link ClosedListener} which will be called when the resource guarded by {@link
     * #getKey()} is closed.
     */
    void addClosedListener(ClosedListener listener);
  }

  /** A cache key identifying a resource that is being cached on. *///缓存的key
  public static final class CacheKey {
    CacheKey() {} // only instantiable by core impls
  }

  /**
   * A listener that is called when a resource gets closed.
   * close监听器
   * @lucene.experimental
   */
  @FunctionalInterface
  public static interface ClosedListener {
    /**
     * Invoked when the resource (segment core, or index reader) that is being cached on is closed.
     */
    void onClose(CacheKey key) throws IOException;
  }

  private final Set<IndexReader> parentReaders =
      Collections.synchronizedSet(
          Collections.newSetFromMap(new WeakHashMap<IndexReader, Boolean>()));

  /**
   * Expert: This method is called by {@code IndexReader}s which wrap other readers (e.g. {@link
   * CompositeReader} or {@link FilterLeafReader}) to register the parent at the child (this reader)
   * on construction of the parent. When this reader is closed, it will mark all registered parents
   * as closed, too. The references to parent readers are weak only, so they can be GCed once they
   * are no longer in use.
   * 当当前的reader关闭时，也会将所有的父reader标记为关闭。都是弱引用
   * @lucene.experimental
   */
  public final void registerParentReader(IndexReader reader) {
    ensureOpen();
    parentReaders.add(reader);
  }

  /**
   * For test framework use only.
   *
   * @lucene.internal
   */
  protected void notifyReaderClosedListeners() throws IOException {
    // nothing to notify in the base impl
  }

  private void reportCloseToParentReaders() throws IOException {
    synchronized (parentReaders) {
      for (IndexReader parent : parentReaders) {
        parent.closedByChild = true;
        // cross memory barrier by a fake write://通过伪写入跨越内存屏障，保证之前的操作都已经完成并写入内存，再执行下一条指令
        parent.refCount.addAndGet(0);
        // recurse://递归：
        parent.reportCloseToParentReaders();
      }
    }
  }

  /** Expert: returns the current refCount for this reader */
  public final int getRefCount() {
    // NOTE: don't ensureOpen, so that callers can see
    // refCount is 0 (reader is closed) 如果是关闭的话，返回0
    return refCount.get();
  }

  /**
   * Expert: increments the refCount of this IndexReader instance. RefCounts are used to determine
   * when a reader can be closed safely, i.e. as soon as there are no more references. Be sure to
   * always call a corresponding {@link #decRef}, in a finally clause; otherwise the reader may
   * never be closed. Note that {@link #close} simply calls decRef(), which means that the
   * IndexReader will not really be closed until {@link #decRef} has been called for all outstanding
   * references.
   * 增加引用计数，引用计数用于确定能否安全的关闭reader。应定要在finally子句中执行执行相应的decRef，否则的话可能永远都无法被关闭了。
   * @see #decRef
   * @see #tryIncRef
   */
  public final void incRef() {
    if (!tryIncRef()) {
      ensureOpen();//说明已经关闭，直接抛出异常
    }
  }

  /**
   * Expert: increments the refCount of this IndexReader instance only if the IndexReader has not
   * been closed yet and returns <code>true</code> iff the refCount was successfully incremented,
   * otherwise <code>false</code>. If this method returns <code>false</code> the reader is either
   * already closed or is currently being closed. Either way this reader instance shouldn't be used
   * by an application unless <code>true</code> is returned.
   * return false 说明要么在关闭，要么已经关闭。也就是说返回false，那么这个实例是不能被使用的！
   * <p>RefCounts are used to determine when a reader can be closed safely, i.e. as soon as there
   * are no more references. Be sure to always call a corresponding {@link #decRef}, in a finally
   * clause; otherwise the reader may never be closed. Note that {@link #close} simply calls
   * decRef(), which means that the IndexReader will not really be closed until {@link #decRef} has
   * been called for all outstanding references.
   *
   * @see #decRef
   * @see #incRef
   */
  public final boolean tryIncRef() {
    int count;
    while ((count = refCount.get()) > 0) {
      if (refCount.compareAndSet(count, count + 1)) {
        return true;
      }
    }
    return false; //refCount<0 说明已经关闭
  }

  /**
   * Expert: decreases the refCount of this IndexReader instance. If the refCount drops to 0, then
   * this reader is closed. If an exception is hit, the refCount is unchanged.
   *
   * @throws IOException in case an IOException occurs in doClose()
   * @see #incRef
   */
  @SuppressWarnings("try")
  public final void decRef() throws IOException {
    // only check refcount here (don't call ensureOpen()), so we can
    // still close the reader if it was made invalid by a child:
    if (refCount.get() <= 0) {
      throw new AlreadyClosedException("this IndexReader is closed");
    }

    final int rc = refCount.decrementAndGet();
    if (rc == 0) { //减到0,可以关闭
      closed = true;
      try (Closeable finalizer = this::reportCloseToParentReaders;
          Closeable finalizer1 = this::notifyReaderClosedListeners) {
        doClose(); //执行close，也说明closed = true的情况下，可能已经关闭也可能正在关闭。
      }
    } else if (rc < 0) { //说明已经减多了，都已经被关闭了！
      throw new IllegalStateException(
          "too many decRef calls: refCount is " + rc + " after decrement");
    }
  }

  /**
   * Throws AlreadyClosedException if this IndexReader or any of its child readers is closed,
   * otherwise returns.
   */
  protected final void ensureOpen() throws AlreadyClosedException {
    if (refCount.get() <= 0) {
      throw new AlreadyClosedException("this IndexReader is closed");
    }
    // the happens before rule on reading the refCount, which must be after the fake write,
    // ensures that we see the value:
    if (closedByChild) {
      throw new AlreadyClosedException(
          "this IndexReader cannot be used anymore as one of its child readers was closed");
    }
  }

  /**
   * {@inheritDoc}
   *
   * <p>{@code IndexReader} subclasses are not allowed to implement equals/hashCode, so methods are
   * declared final.
   */
  @Override
  public final boolean equals(Object obj) {
    return (this == obj);
  }

  /**
   * {@inheritDoc}
   *
   * <p>{@code IndexReader} subclasses are not allowed to implement equals/hashCode, so methods are
   * declared final.
   */
  @Override
  public final int hashCode() {
    return System.identityHashCode(this);
  }

  /**
   * Retrieve term vectors for this document, or null if term vectors were not indexed. The returned
   * Fields instance acts like a single-document inverted index (the docID will be 0).
   * 返回多列
   * @deprecated use {@link #termVectors()} to retrieve one or more documents
   */
  @Deprecated
  public abstract Fields getTermVectors(int docID) throws IOException;

  /**
   * Retrieve term vector for this document and field, or null if term vectors were not indexed. The
   * returned Fields instance acts like a single-document inverted index (the docID will be 0).
   * 返回指定的一列
   * @deprecated use {@link #termVectors()} to retrieve one or more documents
   */
  @Deprecated
  public final Terms getTermVector(int docID, String field) throws IOException {
    Fields vectors = getTermVectors(docID);
    if (vectors == null) {
      return null;
    }
    return vectors.terms(field);
  }

  /**
   * Returns a {@link TermVectors} reader for the term vectors of this index.
   *
   * <p>This call never returns {@code null}, even if no term vectors were indexed. The returned
   * instance should only be used by a single thread.
   * 返回结果只能被一个线程使用？？？
   * <p>Example:
   *
   * <pre class="prettyprint">
   * TopDocs hits = searcher.search(query, 10);
   * TermVectors termVectors = reader.termVectors();
   * for (ScoreDoc hit : hits.scoreDocs) {
   *   Fields vector = termVectors.get(hit.doc);
   * }
   * </pre>
   *
   * @throws IOException If there is a low-level IO error
   */
  public abstract TermVectors termVectors() throws IOException;

  /**
   * Returns the number of documents in this index.
   *
   * <p><b>NOTE</b>: This operation may run in O(maxDoc). Implementations that can't return this
   * number in constant-time should cache it.
   */
  public abstract int numDocs();

  /**
   * Returns one greater than the largest possible document number. This may be used to, e.g.,
   * determine how big to allocate an array which will have an element for every document number in
   * an index. 返回 最大的编号+1,一般用于数组资源申请
   */
  public abstract int maxDoc();

  /**
   * Returns the number of deleted documents.
   *
   * <p><b>NOTE</b>: This operation may run in O(maxDoc).
   */
  public final int numDeletedDocs() {
    return maxDoc() - numDocs();
  }

  /**
   * Expert: visits the fields of a stored document, for custom processing/loading of each field. If
   * you simply want to load all fields, use {@link #document(int)}. If you want to load a subset,
   * use {@link DocumentStoredFieldVisitor}.
   *
   * @deprecated use {@link #storedFields()} to retrieve one or more documents
   */
  @Deprecated
  public abstract void document(int docID, StoredFieldVisitor visitor) throws IOException;

  /**
   * Returns the stored fields of the <code>n</code><sup>th</sup> <code>Document</code> in this
   * index. This is just sugar for using {@link DocumentStoredFieldVisitor}. 语法糖
   *
   * <p><b>NOTE:</b> for performance reasons, this method does not check if the requested document
   * is deleted, and therefore asking for a deleted document may yield unspecified results. Usually
   * this is not required, however you can test if the doc is deleted by checking the {@link Bits}
   * returned from {@link MultiBits#getLiveDocs}.为了性能，这个方法没有检查请求的文档是否被删除，
   * 因此要删除的文档可能会产生未指定的结果。通常并不要求，但是你可以通过检查getLiveDocs的返回值Bits，来确认是都被删除
   * <p><b>NOTE:</b> only the content of a field is returned, if that field was stored during
   * indexing. Metadata like boost, omitNorm, IndexOptions, tokenized, etc., are not preserved.
   * 如果field
   * @throws CorruptIndexException if the index is corrupt
   * @throws IOException if there is a low-level IO error
   * @deprecated use {@link #storedFields()} to retrieve one or more documents
   */
  // TODO: we need a separate StoredField, so that the
  // Document returned here contains that class not
  // IndexableField
  @Deprecated
  public final Document document(int docID) throws IOException {
    final DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor();
    document(docID, visitor);
    return visitor.getDocument();
  }

  /**
   * Like {@link #document(int)} but only loads the specified fields. Note that this is simply sugar
   * for {@link DocumentStoredFieldVisitor#DocumentStoredFieldVisitor(Set)}.
   *
   * @deprecated use {@link #storedFields()} to retrieve one or more documents
   */
  @Deprecated
  public final Document document(int docID, Set<String> fieldsToLoad) throws IOException {
    final DocumentStoredFieldVisitor visitor = new DocumentStoredFieldVisitor(fieldsToLoad);
    document(docID, visitor);
    return visitor.getDocument();
  }

  /**
   * Returns a {@link StoredFields} reader for the stored fields of this index.
   *
   * <p>This call never returns {@code null}, even if no stored fields were indexed. The returned
   * instance should only be used by a single thread.
   *
   * <p>Example:
   *
   * <pre class="prettyprint">
   * TopDocs hits = searcher.search(query, 10);
   * StoredFields storedFields = reader.storedFields();
   * for (ScoreDoc hit : hits.scoreDocs) {
   *   Document doc = storedFields.document(hit.doc);
   * }
   * </pre>
   *
   * @throws IOException If there is a low-level IO error
   */
  public abstract StoredFields storedFields() throws IOException;

  /**
   * Returns true if any documents have been deleted. Implementers should consider overriding this
   * method if {@link #maxDoc()} or {@link #numDocs()} are not constant-time operations.
   */
  public boolean hasDeletions() {
    return numDeletedDocs() > 0;
  }

  /**
   * Closes files associated with this index. Also saves any new deletions to disk. No other methods
   * should be called after this has been called.
   * 当这个方法被调用之后，其他的方法都不应该再被调用
   * @throws IOException if there is a low-level IO error
   */
  @Override
  public final synchronized void close() throws IOException {
    if (!closed) {
      decRef();
      closed = true; //实际上可能并没有被关闭，因为refcount可能不是0！
    }
  }

  /** Implements close. */
  protected abstract void doClose() throws IOException;

  /**
   * Expert: Returns the root {@link IndexReaderContext} for this {@link IndexReader}'s sub-reader
   * tree.
   * 返回子树的根
   * <p>Iff this reader is composed of sub readers, i.e. this reader being a composite reader, this
   * method returns a {@link CompositeReaderContext} holding the reader's direct children as well as
   * a view of the reader tree's atomic leaf contexts. All sub- {@link IndexReaderContext} instances
   * referenced from this readers top-level context are private to this reader and are not shared
   * with another context tree. For example, IndexSearcher uses this API to drive searching by one
   * atomic leaf reader at a time. If this reader is not composed of child readers, this method
   * returns an {@link LeafReaderContext}.
   *
   * <p>Note: Any of the sub-{@link CompositeReaderContext} instances referenced from this top-level
   * context do not support {@link CompositeReaderContext#leaves()}. Only the top-level context
   * maintains the convenience leaf-view for performance reasons. 只有最顶级的的可以调用 CompositeReaderContext#leaves()
   */
  public abstract IndexReaderContext getContext();

  /**
   * Returns the reader's leaves, or itself if this reader is atomic. This is a convenience method
   * calling {@code this.getContext().leaves()}.
   * 返回这个reader的所有的叶子readers，如果他自己就是原子的话就返回他自己。
   * @see IndexReaderContext#leaves()
   */
  public final List<LeafReaderContext> leaves() {
    return getContext().leaves();
  }

  /**
   * Optional method: Return a {@link CacheHelper} that can be used to cache based on the content of
   * this reader. Two readers that have different data or different sets of deleted documents will
   * be considered different.
   *
   * <p>A return value of {@code null} indicates that this reader is not suited for caching, which
   * is typically the case for short-lived wrappers that alter the content of the wrapped reader.
   *
   * @lucene.experimental
   */
  public abstract CacheHelper getReaderCacheHelper();

  /**
   * Returns the number of documents containing the <code>term</code>. This method returns 0 if the
   * term or field does not exists. This method does not take into account deleted documents that
   * have not yet been merged away.
   * 包含这个term的doc的数量，注意：此方法不考虑尚未合并的已删除文档！
   * @see TermsEnum#docFreq()
   */
  public abstract int docFreq(Term term) throws IOException;

  /**
   * Returns the total number of occurrences of {@code term} across all documents (the sum of the
   * freq() for each doc that has this term). Note that, like other term measures, this measure does
   * not take deleted documents into account.
   */
  public abstract long totalTermFreq(Term term) throws IOException;

  /**
   * Returns the sum of {@link TermsEnum#docFreq()} for all terms in this field. Note that, just
   * like other term measures, this measure does not take deleted documents into account.
   * 所有term的数量的和
   * @see Terms#getSumDocFreq()
   */
  public abstract long getSumDocFreq(String field) throws IOException;

  /**
   * Returns the number of documents that have at least one term for this field. Note that, just
   * like other term measures, this measure does not take deleted documents into account.
   * 包含这个filed的文档的个数
   * @see Terms#getDocCount()
   */
  public abstract int getDocCount(String field) throws IOException;

  /**
   * Returns the sum of {@link TermsEnum#totalTermFreq} for all terms in this field. Note that, just
   * like other term measures, this measure does not take deleted documents into account.
   * totalTermFreq 的和
   * @see Terms#getSumTotalTermFreq()
   */
  public abstract long getSumTotalTermFreq(String field) throws IOException;
}
