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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.lucene.index.DocumentsWriterPerThread.FlushedSegment;
import org.apache.lucene.util.IOConsumer;

/**
 * @lucene.internal
 */
final class DocumentsWriterFlushQueue {
  private final Queue<FlushTicket> queue = new ArrayDeque<>();
  // we track tickets separately since count must be present even before the ticket is，我们分开跟踪tickets 数量，是因为在
  // constructed ie. queue.size would not reflect it. 在ticket被构造之前 count 就必须存在和使用，所以queue.size无法反映count。
  private final AtomicInteger ticketCount = new AtomicInteger();
  private final ReentrantLock purgeLock = new ReentrantLock();

  synchronized boolean addDeletes(DocumentsWriterDeleteQueue deleteQueue) throws IOException {
    // first inc the ticket count - freeze opens a window for #anyChanges to fail
    incTickets();
    boolean success = false;
    try {
      FrozenBufferedUpdates frozenBufferedUpdates = deleteQueue.maybeFreezeGlobalBuffer();
      if (frozenBufferedUpdates != null) {
        // no need to publish anything if we don't have any frozen updates
        queue.add(new FlushTicket(frozenBufferedUpdates, false));
        success = true;
      }
    } finally {
      if (!success) {
        decTickets();
      }
    }
    return success;
  }

  private void incTickets() {
    int numTickets = ticketCount.incrementAndGet();
    assert numTickets > 0;
  }

  private void decTickets() {
    int numTickets = ticketCount.decrementAndGet();
    assert numTickets >= 0;
  }

  synchronized FlushTicket addFlushTicket(DocumentsWriterPerThread dwpt) throws IOException {
    // Each flush is assigned a ticket in the order they acquire the ticketQueue
    // lock 每个flush都会分配一个ticket，并且是按照他们获取到ticketQueue 锁的顺序获取的。
    incTickets();
    boolean success = false;
    try {
      // prepare flush freezes the global deletes - do in synced block!
      // prepareFlush 会冻结 global deletes，并生成冻结的globalUpdates
      // 将当前 deleteQueue tail和 deleteSlice 以及 golobalSlice 对齐，并将删除应用到updates
      final FlushTicket ticket = new FlushTicket(dwpt.prepareFlush(), true);
      queue.add(ticket);//将ticket添加到queue
      success = true;
      return ticket;
    } finally {
      if (!success) {
        decTickets();
      }
    }
  }

  synchronized void addSegment(FlushTicket ticket, FlushedSegment segment) {
    assert ticket.hasSegment;
    // the actual flush is done asynchronously and once done the FlushedSegment
    // is passed to the flush ticket
    ticket.setSegment(segment);
  }

  synchronized void markTicketFailed(FlushTicket ticket) {
    assert ticket.hasSegment;
    // to free the queue we mark tickets as failed just to clean up the queue.
    ticket.setFailed();
  }

  boolean hasTickets() {
    assert ticketCount.get() >= 0 : "ticketCount should be >= 0 but was: " + ticketCount.get();
    return ticketCount.get() != 0;
  }

  private void innerPurge(IOConsumer<FlushTicket> consumer) throws IOException {
    assert purgeLock.isHeldByCurrentThread();
    while (true) {
      final FlushTicket head;
      final boolean canPublish;
      synchronized (this) {
        head = queue.peek(); //拿到队头，判断是否可以publish，如果可以，则把处理之后从队列中删除，然后循环执行，知道找到不能publish的ticket，跳出循环
        canPublish = head != null && head.canPublish(); // do this synced
      }
      if (canPublish) {
        try {
          /*
           * if we block on publish -> lock IW -> lock BufferedDeletes we don't block
           * concurrent segment flushes just because they want to append to the queue.
           * the downside is that we need to force a purge on fullFlush since there could
           * be a ticket still in the queue.
           */
          consumer.accept(head);

        } finally {
          synchronized (this) {
            // finally remove the published ticket from the queue
            final FlushTicket poll = queue.poll();
            decTickets();
            // we hold the purgeLock so no other thread should have polled:
            assert poll == head;
          }
        }
      } else {
        break;
      }
    }
  }

  void forcePurge(IOConsumer<FlushTicket> consumer) throws IOException {
    assert !Thread.holdsLock(this);
    purgeLock.lock();
    try {
      innerPurge(consumer);
    } finally {
      purgeLock.unlock();
    }
  }

  void tryPurge(IOConsumer<FlushTicket> consumer) throws IOException {
    assert !Thread.holdsLock(this);
    if (purgeLock.tryLock()) {
      try {
        innerPurge(consumer);
      } finally {
        purgeLock.unlock();
      }
    }
  }

  int getTicketCount() {
    return ticketCount.get();
  }

  static final class FlushTicket {
    private final FrozenBufferedUpdates frozenUpdates;
    private final boolean hasSegment;
    private FlushedSegment segment;
    private boolean failed = false;
    private boolean published = false;

    FlushTicket(FrozenBufferedUpdates frozenUpdates, boolean hasSegment) {
      this.frozenUpdates = frozenUpdates;
      this.hasSegment = hasSegment;
    }

    boolean canPublish() {
      return hasSegment == false || segment != null || failed;
    }

    synchronized void markPublished() {
      assert published == false : "ticket was already published - can not publish twice";
      published = true;
    }

    private void setSegment(FlushedSegment segment) {
      assert !failed;
      this.segment = segment;
    }

    private void setFailed() {
      assert segment == null;
      failed = true;
    }

    /**
     * Returns the flushed segment or <code>null</code> if this flush ticket doesn't have a segment.
     * This can be the case if this ticket represents a flushed global frozen updates package.
     */
    FlushedSegment getFlushedSegment() {
      return segment;
    }

    /** Returns a frozen global deletes package. */
    FrozenBufferedUpdates getFrozenUpdates() {
      return frozenUpdates;
    }
  }
}
