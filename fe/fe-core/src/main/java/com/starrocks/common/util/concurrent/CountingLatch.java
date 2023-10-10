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

package com.starrocks.common.util.concurrent;

import com.google.common.base.Preconditions;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * CountDownLatch with extra counting up functionality.
 */
public class CountingLatch {
    /**
     * Synchronization control for CountingLatch.
     * Uses AQS state to represent count.
     */
    private static final class Sync extends AbstractQueuedSynchronizer {
        private Sync() {
        }

        private Sync(final int initialState) {
            setState(initialState);
        }

        int getCount() {
            return getState();
        }

        protected int tryAcquireShared(final int acquires) {
            return getState() == 0 ? 1 : -1;
        }

        protected boolean tryReleaseShared(final int delta) {
            // Decrement count; signal when transition to zero
            while (true) {
                final int st = getState();
                final int nextSt = st + delta;
                Preconditions.checkState(nextSt >= 0);
                if (compareAndSetState(st, nextSt)) {
                    return nextSt == 0;
                }
            }
        }
    }

    private final Sync sync;

    public CountingLatch() {
        sync = new Sync();
    }

    @SuppressWarnings("UnusedDeclaration")
    public CountingLatch(final int initialCount) {
        sync = new Sync(initialCount);
    }

    public void increment() {
        sync.releaseShared(1);
    }

    public int getCount() {
        return sync.getCount();
    }

    public void decrement() {
        sync.releaseShared(-1);
    }

    public void awaitZero() throws InterruptedException {
        sync.acquireSharedInterruptibly(1);
    }

    public boolean awaitZero(final long timeout,
                             final TimeUnit timeUnit) throws InterruptedException {
        return sync.tryAcquireSharedNanos(1, timeUnit.toNanos(timeout));
    }
}
