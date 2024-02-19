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

import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncIterable<T> extends CloseableGroup implements CloseableIterable<T> {
    private static final Logger LOG = LogManager.getLogger(AsyncIterable.class);
    private final ConcurrentLinkedQueue<T> queue;
    private final AtomicBoolean buildFinished;
    public AsyncIterable(ConcurrentLinkedQueue<T> queue, AtomicBoolean buildFinished) {
        this.queue = queue;
        this.buildFinished = buildFinished;
    }

    @Override
    public CloseableIterator<T> iterator() {
        AsyncIterator<T> iter = new AsyncIterator<>(queue, buildFinished);
        addCloseable(iter);
        return iter;
    }

    private static class AsyncIterator<T> implements CloseableIterator<T> {
        private final ConcurrentLinkedQueue<T> queue;
        private final AtomicBoolean buildFinished;

        private AsyncIterator(ConcurrentLinkedQueue<T> queue, AtomicBoolean buildFinished) {
            this.queue = queue;
            this.buildFinished = buildFinished;
        }

        @Override
        public void close() {
            queue.clear();
        }

        @Override
        public boolean hasNext() {
            while (queue.isEmpty()) {
                if (buildFinished.get()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public T next() {
            return queue.poll();
        }
    }
}
