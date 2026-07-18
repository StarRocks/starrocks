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

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Makes a delegate CloseableIterator safe to close() from one thread while it is
 * consumed on another. The incremental scan-range scheduler consumes the iterator on
 * an executor thread while query cancellation closes the scan source on a separate
 * cleanup thread; without this, a concurrent close() lets the consumer touch an
 * already-closed underlying iterable (Iceberg's ParallelIterable throws "Already closed").
 *
 * <p>hasNext/next/close are serialized, and hasNext() prefetches one element so a
 * close() landing between a caller's hasNext() and next() still lets that next() return
 * the buffered element. Once closed, hasNext() reports exhausted (false) so a closed
 * source never advertises another batch to a standalone probe, and the delegate is
 * never accessed again.
 */
public class SynchronizedCloseableIterator<T> implements CloseableIterator<T> {
    private final CloseableIterator<T> delegate;
    private T buffered;
    private boolean hasBuffered = false;
    private boolean closed = false;

    public SynchronizedCloseableIterator(CloseableIterator<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public synchronized boolean hasNext() {
        if (closed) {
            return false;
        }
        if (hasBuffered) {
            return true;
        }
        if (delegate.hasNext()) {
            buffered = delegate.next();
            hasBuffered = true;
            return true;
        }
        return false;
    }

    @Override
    public synchronized T next() {
        // A buffered element prefetched before close() is still returned, so a next()
        // racing a concurrent close() does not throw.
        if (hasBuffered) {
            T next = buffered;
            buffered = null;
            hasBuffered = false;
            return next;
        }
        if (closed || !delegate.hasNext()) {
            throw new NoSuchElementException();
        }
        return delegate.next();
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        delegate.close();
    }
}
