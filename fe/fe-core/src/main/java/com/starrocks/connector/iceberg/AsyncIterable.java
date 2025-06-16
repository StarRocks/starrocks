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

import org.apache.iceberg.MetadataParser;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentLinkedQueue;

public class AsyncIterable<T> extends CloseableGroup implements CloseableIterable<T> {
    private static final Logger LOG = LogManager.getLogger(AsyncIterable.class);
    private final ConcurrentLinkedQueue<T> scanTaskQueue;
    private final MetadataParser context;

    public AsyncIterable(ConcurrentLinkedQueue<T> queue, MetadataParser context) {
        this.scanTaskQueue = queue;
        this.context = context;
    }

    @Override
    public CloseableIterator<T> iterator() {
        AsyncIterator<T> iter = new AsyncIterator<>(scanTaskQueue, context);
        addCloseable(iter);
        return iter;
    }

    private static class AsyncIterator<T> implements CloseableIterator<T> {
        private final ConcurrentLinkedQueue<T> queue;
        private final MetadataParser context;

        public AsyncIterator(ConcurrentLinkedQueue<T> scanTaskQueue, MetadataParser context) {
            this.queue = scanTaskQueue;
            this.context = context;
        }

        @Override
        public void close() {
            context.clear();
        }

        @Override
        public boolean hasNext() {
            while (queue.isEmpty()) {
                if (isParserError()) {
                    throw context.getMetadataParserException();
                }

                if (isParserFinished()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public T next() {
            return queue.poll();
        }

        private boolean isParserError() {
            return context.parseError();
        }

        private boolean isParserFinished() {
            return context.parseFinish();
        }
    }
}