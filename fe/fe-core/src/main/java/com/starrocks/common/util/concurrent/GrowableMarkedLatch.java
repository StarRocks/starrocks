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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.starrocks.common.Status;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link MarkedCountDownLatch} variant whose mark set can still grow while the latch has not
 * completed. The count is the number of outstanding marks, so there is no fixed initial count:
 * {@link #addMark} raises the count, {@link #markedCountDown} lowers it, and waiters are released
 * when no marks remain.
 *
 * <p>Once the latch has completed — all marks counted down after at least one was ever added, or
 * forced by {@link #countDownToZero} — it is sealed: {@link #addMark} returns false and completion
 * can never be revoked. This mirrors the invariant needed by schedulers that add fragment
 * instances to a running query: an instance may only be attached while the query is still running.
 */
public class GrowableMarkedLatch<K, V> {
    private static final Logger LOG = LogManager.getLogger(GrowableMarkedLatch.class);

    private final Multimap<K, V> marks = HashMultimap.create();
    private final List<Runnable> listeners = Lists.newArrayList();
    private Status st = Status.OK;
    private boolean everMarked = false;
    private boolean forcedZero = false;

    /**
     * Adds a mark, raising the count by one. Returns false without adding when the latch has
     * already completed.
     */
    public synchronized boolean addMark(K key, V value) {
        if (isSealed()) {
            return false;
        }
        marks.put(key, value);
        everMarked = true;
        return true;
    }

    public synchronized boolean markedCountDown(K key, V value) {
        if (marks.remove(key, value)) {
            onStateChanged();
            return true;
        }
        return false;
    }

    public synchronized boolean markedCountDown(K key, V value, Status status) {
        if (st.ok()) {
            st = status;
        }
        return markedCountDown(key, value);
    }

    public synchronized void countDownToZero(Status status) {
        // update status first before completing, so that waiting threads observe it.
        if (st.ok()) {
            st = status;
        }
        forcedZero = true;
        marks.clear();
        onStateChanged();
    }

    public synchronized long getCount() {
        return marks.size();
    }

    public synchronized List<Entry<K, V>> getLeftMarks() {
        return Lists.newArrayList(marks.entries());
    }

    public synchronized Status getStatus() {
        return st;
    }

    public synchronized void addListener(Runnable listener) {
        listeners.add(new OneShotListener(listener));
        triggerListeners();
    }

    public synchronized boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        long deadlineNs = System.nanoTime() + unit.toNanos(timeout);
        while (!marks.isEmpty()) {
            long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNs - System.nanoTime());
            if (remainingMs <= 0) {
                return false;
            }
            wait(remainingMs);
        }
        return true;
    }

    private boolean isSealed() {
        return forcedZero || (everMarked && marks.isEmpty());
    }

    private void onStateChanged() {
        if (marks.isEmpty()) {
            notifyAll();
            triggerListeners();
        }
    }

    private void triggerListeners() {
        if (!marks.isEmpty()) {
            return;
        }
        for (Runnable listener : listeners) {
            try {
                listener.run();
            } catch (Throwable e) {
                LOG.warn("Listener invoke failed", e);
            }
        }
    }

    private static final class OneShotListener implements Runnable {
        private final AtomicBoolean hasRun = new AtomicBoolean(false);
        private final Runnable runnable;

        public OneShotListener(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void run() {
            if (hasRun.compareAndSet(false, true)) {
                runnable.run();
            }
        }
    }
}
