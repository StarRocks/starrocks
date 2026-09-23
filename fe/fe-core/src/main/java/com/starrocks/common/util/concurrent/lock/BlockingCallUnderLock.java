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

package com.starrocks.common.util.concurrent.lock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The external call a lock holder last went out on, so a slow lock can be attributed to it.
 *
 * <h3>Why this is not a ThreadLocal</h3>
 *
 * The thread that needs the answer is not the thread that has it. {@link LockManager}'s slow-lock trace runs
 * on a <em>waiter</em> and reports on the <em>owners</em>, so the record has to be readable across threads --
 * hence a map keyed by thread id rather than a {@link ThreadLocal}. The write side keeps a thread-local copy
 * anyway, purely so the common case of "nothing to clear" costs a thread-local read instead of a map
 * operation on every outermost lock release.
 *
 * <h3>What the record means, exactly</h3>
 *
 * <b>The last blocking call this thread started while holding a lock, and when it started.</b> Not "the call
 * currently in flight": the guards are one-line calls at the point of departure
 * ({@link BlockingCallValidator}), not scopes wrapped around the request, so there is no return event to
 * observe. Turning 25 guard sites into try-with-resources blocks would buy that distinction at the cost of
 * restructuring the control flow of every transport in the FE.
 *
 * <p>The distinction matters less than it sounds, because of when this is read. A slow-lock report exists
 * precisely because an owner is holding on for seconds; if that owner's last recorded call started seconds
 * ago and has not been followed by another, it is overwhelmingly the call it is still sitting in. That is why
 * the age is reported next to the name -- a call that started 4 seconds into a 4-second slow lock reads very
 * differently from one that started and returned at the top of the critical section, and the reader can tell
 * them apart.
 *
 * <h3>Lifetime</h3>
 *
 * Written only while a lock is held, and dropped when the thread releases its outermost lock (and by
 * {@link LockHoldDepth#reset}). So the map holds at most one entry per thread that is inside a critical
 * section right now, and a thread handed back to a pool leaves nothing behind.
 */
public final class BlockingCallUnderLock {

    /** What a lock holder went out to contact, and when. */
    public static final class Record {
        private final String transport;
        private final String catalog;
        private final long startTimeMs;

        private Record(String transport, String catalog, long startTimeMs) {
            this.transport = transport;
            this.catalog = catalog;
            this.startTimeMs = startTimeMs;
        }

        /** Short tag for the system contacted, e.g. {@code "hive-metastore"}. Never null. */
        public String getTransport() {
            return transport;
        }

        /** The catalog whose latency the lock is bound to, or null where the transport has no catalog. */
        public String getCatalog() {
            return catalog;
        }

        public long getStartTimeMs() {
            return startTimeMs;
        }
    }

    private static final Map<Long, Record> IN_FLIGHT = new ConcurrentHashMap<>();

    /**
     * The current thread's own copy of what it put in the map. Only ever read by the thread that wrote it,
     * and only to answer "is there anything of mine to remove" without touching the shared map.
     */
    private static final ThreadLocal<Record> OWN = new ThreadLocal<>();

    private BlockingCallUnderLock() {
    }

    /**
     * Record that this thread, which is holding at least one metadata lock, is about to contact
     * {@code transport}.
     *
     * @param catalog the catalog being contacted, or null where the transport is not per-catalog (thrift RPC
     *                to a BE, lake publish, kafka). Null is recorded as null rather than as a placeholder, so
     *                an aggregation by catalog can tell "no catalog" from a catalog literally named that.
     */
    static void started(String transport, String catalog) {
        Record record = new Record(transport, catalog, System.currentTimeMillis());
        OWN.set(record);
        IN_FLIGHT.put(Thread.currentThread().getId(), record);
    }

    /**
     * What the thread with this id last went out on while under lock, or null if it has none -- which is the
     * answer for every thread that is not in a critical section.
     */
    public static Record of(long threadId) {
        return IN_FLIGHT.get(threadId);
    }

    /**
     * Called when the current thread stops holding metadata locks. The thread-local check keeps this to a
     * single thread-local read for the overwhelming majority of releases, which never recorded anything.
     */
    static void clearForCurrentThread() {
        if (OWN.get() == null) {
            return;
        }
        OWN.remove();
        IN_FLIGHT.remove(Thread.currentThread().getId());
    }
}
