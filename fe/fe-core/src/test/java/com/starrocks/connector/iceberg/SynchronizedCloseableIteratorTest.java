// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.iceberg;

import org.apache.iceberg.io.CloseableIterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class SynchronizedCloseableIteratorTest {

    // Mimics Iceberg's ParallelIterable: once closed, hasNext()/next() throw
    // IllegalStateException("Already closed").
    private static class AlreadyClosedIterator implements CloseableIterator<Integer> {
        private int index = 0;
        private final int size;
        private volatile boolean closed = false;

        AlreadyClosedIterator(int size) {
            this.size = size;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                throw new IllegalStateException("Already closed");
            }
            return index < size;
        }

        @Override
        public Integer next() {
            if (closed) {
                throw new IllegalStateException("Already closed");
            }
            return index++;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    @Test
    public void testDrainThenClosedBehavior() {
        SynchronizedCloseableIterator<Integer> it =
                new SynchronizedCloseableIterator<>(new AlreadyClosedIterator(3));
        int count = 0;
        while (it.hasNext()) {
            it.next();
            count++;
        }
        Assertions.assertEquals(3, count);

        Assertions.assertDoesNotThrow(it::close);
        Assertions.assertDoesNotThrow(it::close);            // close is idempotent
        Assertions.assertFalse(it.hasNext());                // no reopen / no throw after close
        Assertions.assertThrows(NoSuchElementException.class, it::next);
    }

    // After close, hasNext() reports exhausted even if an element was prefetched, so a closed
    // source never advertises another batch; an already-prefetched element is still drainable.
    @Test
    public void testClosedReportsExhaustedAfterPrefetch() {
        SynchronizedCloseableIterator<Integer> it =
                new SynchronizedCloseableIterator<>(new AlreadyClosedIterator(5));
        Assertions.assertTrue(it.hasNext());                 // prefetches and buffers one element
        Assertions.assertDoesNotThrow(it::close);
        Assertions.assertFalse(it.hasNext());                // closed -> exhausted, does not advertise the buffer
        Assertions.assertDoesNotThrow(it::next);             // an in-flight next() still returns the buffered element
    }

    // Consuming on one thread while another closes must never surface
    // IllegalStateException("Already closed") from the delegate.
    @Test
    public void testConcurrentConsumeAndClose() throws InterruptedException {
        for (int round = 0; round < 2000; round++) {
            SynchronizedCloseableIterator<Integer> it =
                    new SynchronizedCloseableIterator<>(new AlreadyClosedIterator(1000));
            AtomicReference<Throwable> failure = new AtomicReference<>();
            CountDownLatch start = new CountDownLatch(1);

            Thread consumer = new Thread(() -> {
                try {
                    start.await();
                    while (it.hasNext()) {
                        it.next();
                    }
                } catch (Throwable t) {
                    failure.set(t);
                }
            });
            Thread closer = new Thread(() -> {
                try {
                    start.await();
                    it.close();
                } catch (Throwable t) {
                    failure.set(t);
                }
            });

            consumer.start();
            closer.start();
            start.countDown();
            consumer.join();
            closer.join();

            if (failure.get() != null) {
                Assertions.fail("concurrent consume/close threw: " + failure.get());
            }
        }
    }
}
