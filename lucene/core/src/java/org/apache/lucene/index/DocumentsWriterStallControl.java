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

import java.util.IdentityHashMap;
import java.util.Map;
import org.apache.lucene.util.ThreadInterruptedException;

/**
 * Controls the health status of a {@link DocumentsWriter} sessions. This class used to block
 * incoming indexing threads if flushing significantly slower than indexing to ensure the {@link
 * DocumentsWriter}s healthiness. If flushing is significantly slower than indexing the net memory
 * used within an {@link IndexWriter} session can increase very quickly and easily exceed the JVM's
 * available memory.
 *
 * <p>To prevent OOM Errors and ensure IndexWriter's stability this class blocks incoming threads
 * from indexing once 2 x number of available {@link DocumentsWriterPerThread}s in {@link
 * DocumentsWriterPerThreadPool} is exceeded. Once flushing catches up and the number of flushing
 * DWPT is equal or lower than the number of active {@link DocumentsWriterPerThread}s threads are
 * released and can continue indexing.
 *
 * 控制DocumentsWriter会话的运行健康状况。如果刷新速度明显慢于索引速度，则此类用于阻塞传入的索引线程，
 * 以确保DocumentsWriters的健康。如果刷新速度明显慢于索引，则IndexWriter会话中使用的净内存会迅速增加，
 * 并且很容易超过JVM的可用内存。
 *
 * 为了防止OOM错误并确保IndexWriter的稳定性，一旦超过DocumentsWriterPerThreadPool中可用DocumentsWriter PerThreads的2倍数量，此类就会阻止传入线程进行索引。一旦刷新赶上并且刷新DWPT的数量等于或低于活动DocumentsWriterPerThreads的数量，就会释放线程并可以继续索引。
 */
final class DocumentsWriterStallControl {

  private volatile boolean stalled;
  private int numWaiting; // only with assert
  private boolean wasStalled; // only with assert
  private final Map<Thread, Boolean> waiting = new IdentityHashMap<>(); // only with assert

  /**
   * Update the stalled flag status. This method will set the stalled flag to <code>true</code> iff
   * the number of flushing {@link DocumentsWriterPerThread} is greater than the number of active
   * {@link DocumentsWriterPerThread}. Otherwise it will reset the {@link
   * DocumentsWriterStallControl} to healthy and release all threads waiting on {@link
   * #waitIfStalled()}
   * 更新stalled标志状态。如果正在刷新的DWPT数量比活跃的DWPT数量大，这个方法会设置stalled标志为true。否则 他会重置
   * DocumentsWriterStallControl 到健康状态，并释放所有的因为waitIfStalled()而进入waiting状态的线程。
   */
  synchronized void updateStalled(boolean stalled) {
    if (this.stalled != stalled) {//状态变了就更新
      this.stalled = stalled;//设置
      if (stalled) {
        wasStalled = true;
      }
      notifyAll();//通知所有的被stallControl阻塞的线程，为什么只要状态变换，不管是true还是false 都会执行？
      // 是不是true的情况下，即便通知了，后续还是会被阻塞？
    }
  }

  /** Blocks if documents writing is currently in a stalled state. */
  void waitIfStalled() {
    if (stalled) {
      synchronized (this) {
        if (stalled) { // react on the first wakeup call!
          // don't loop here, higher level logic will re-stall!
          // 不要在这里循环，外层的逻辑会re-stall
          try {
            incWaiters();
            // Defensive, in case we have a concurrency bug that fails to .notify/All our thread:
            // just wait for up to 1 second here, and let caller re-stall if it's still needed:
            // 防御，有一个并发bug， 导致执行notice/All线程失败：
            //只需在此处等待1秒，如果仍然需要，请让调用者re-stall：
            wait(1000);//每次block 1s
            decrWaiters();
          } catch (InterruptedException e) {
            throw new ThreadInterruptedException(e);
          }
        }
      }
    }
  }

  boolean anyStalledThreads() {
    return stalled;
  }

  private void incWaiters() {
    numWaiting++;
    assert waiting.put(Thread.currentThread(), Boolean.TRUE) == null;
    assert numWaiting > 0;
  }

  private void decrWaiters() {
    numWaiting--;
    assert waiting.remove(Thread.currentThread()) != null;
    assert numWaiting >= 0;
  }

  synchronized boolean hasBlocked() { // for tests
    return numWaiting > 0;
  }

  synchronized int getNumWaiting() { // for tests
    return numWaiting;
  }

  boolean isHealthy() { // for tests
    return !stalled; // volatile read!
  }

  synchronized boolean isThreadQueued(Thread t) { // for tests
    return waiting.containsKey(t);
  }

  synchronized boolean wasStalled() { // for tests
    return wasStalled;
  }
}
