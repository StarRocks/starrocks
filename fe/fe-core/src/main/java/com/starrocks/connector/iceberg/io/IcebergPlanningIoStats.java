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

package com.starrocks.connector.iceberg.io;

import com.starrocks.common.util.DebugUtil;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Storage I/O done by one Iceberg scan planning, for the query profile.
 *
 * <p>The FileIO is shared by every query on a table, so the stats are found through a thread-local: the
 * planning thread installs them with {@link #enter()}, and {@link #wrap(ExecutorService)} carries them into
 * the planning pool, capturing them on the thread that submits a task (iceberg's ParallelIterable submits
 * from the consumer thread). I/O on a thread without stats -- other queries' background work, table loads --
 * is not counted.
 *
 * <p>One query keeps one instance per scan, so several plannings of the same scan add up. I/O on a table before
 * its scans plan -- loading the table, statistics in the optimizer -- is kept per table and added to the first scan
 * of that table.
 *
 * <p>Requests are counted in {@link IcebergCachingFileIO}: a HEAD is a length or existence check that went to
 * storage, a GET is a remote stream, timed over the calls into it (open, read, seek, close) so the time the
 * reader spends decoding is not counted. Retries come from the AWS SDK ({@link IcebergPlanningIoInterceptor})
 * and so are known for S3 only.
 */
public final class IcebergPlanningIoStats {
    private static final ThreadLocal<IcebergPlanningIoStats> CURRENT = new ThreadLocal<>();

    private final LongAdder headRequests = new LongAdder();
    private final LongAdder getRequests = new LongAdder();
    private final LongAdder ioNanos = new LongAdder();
    private final LongAdder retries = new LongAdder();
    // The slowest request: its time, and what it was ("HEAD snap-1.avro"). Written under the lock, only when a
    // request beats the current one, which after the first few requests is rare.
    private volatile long maxRequestNanos;
    private String slowestRequest;

    public static IcebergPlanningIoStats current() {
        return CURRENT.get();
    }

    /** A scope that leaves the thread as it is. */
    public static Scope none() {
        return new Scope(CURRENT.get());
    }

    /**
     * Installs {@code stats} unless the thread already has stats: I/O done on behalf of a planning stays with that
     * planning even when it goes through a table-level entry point.
     */
    public static Scope enterIfNone(IcebergPlanningIoStats stats) {
        IcebergPlanningIoStats current = CURRENT.get();
        return current != null ? new Scope(current) : stats.enter();
    }

    /** Adds what {@code other} counted to these stats. */
    public void add(IcebergPlanningIoStats other) {
        headRequests.add(other.headRequests.sum());
        getRequests.add(other.getRequests.sum());
        ioNanos.add(other.ioNanos.sum());
        retries.add(other.retries.sum());
        String otherSlowest;
        long otherMax;
        synchronized (other) {
            otherSlowest = other.slowestRequest;
            otherMax = other.maxRequestNanos;
        }
        synchronized (this) {
            if (otherSlowest != null && otherMax > maxRequestNanos) {
                maxRequestNanos = otherMax;
                slowestRequest = otherSlowest;
            }
        }
    }

    /** Installs these stats on the current thread until the returned scope is closed. */
    public Scope enter() {
        IcebergPlanningIoStats previous = CURRENT.get();
        CURRENT.set(this);
        return new Scope(previous);
    }

    public static final class Scope implements AutoCloseable {
        private final IcebergPlanningIoStats previous;

        private Scope(IcebergPlanningIoStats previous) {
            this.previous = previous;
        }

        @Override
        public void close() {
            if (previous == null) {
                CURRENT.remove();
            } else {
                CURRENT.set(previous);
            }
        }
    }

    public void recordHead(long nanos, String location) {
        headRequests.increment();
        recordTime(nanos, "HEAD", location);
    }

    public void recordGet(long nanos, String location) {
        getRequests.increment();
        recordTime(nanos, "GET", location);
    }

    public void recordRetry() {
        retries.increment();
    }

    private void recordTime(long nanos, String kind, String location) {
        ioNanos.add(nanos);
        if (nanos > maxRequestNanos) {
            synchronized (this) {
                if (nanos > maxRequestNanos) {
                    maxRequestNanos = nanos;
                    slowestRequest = kind + " " + fileName(location);
                }
            }
        }
    }

    // The file name only: enough to tell a manifest list (snap-*.avro) from a manifest, and short in the profile.
    private static String fileName(String location) {
        if (location == null) {
            return "";
        }
        int slash = location.lastIndexOf('/');
        return slash >= 0 ? location.substring(slash + 1) : location;
    }

    public long getHeadRequests() {
        return headRequests.sum();
    }

    public long getGetRequests() {
        return getRequests.sum();
    }

    public long getRequests() {
        return getHeadRequests() + getGetRequests();
    }

    public long getIoTimeMs() {
        return TimeUnit.NANOSECONDS.toMillis(ioNanos.sum());
    }

    public long getMaxRequestMs() {
        return TimeUnit.NANOSECONDS.toMillis(maxRequestNanos);
    }

    public synchronized String getSlowestRequest() {
        return slowestRequest;
    }

    public long getRetries() {
        return retries.sum();
    }

    public String formatRequests() {
        return getRequests() + " (HEAD: " + getHeadRequests() + ", GET: " + getGetRequests() + ")";
    }

    /** One phase in one line: "2 (HEAD: 1, GET: 1), 50ms". */
    public String formatPhase() {
        return formatRequests() + ", " + formatIoTime();
    }

    public String formatIoTime() {
        return prettyMs(getIoTimeMs());
    }

    /** "GET snap-1.avro 2s0ms", or null before any request. */
    public String formatSlowest() {
        String slowest = getSlowestRequest();
        return slowest == null ? null : slowest + " " + prettyMs(getMaxRequestMs());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Requests: ").append(getRequests())
                .append(" (HEAD: ").append(getHeadRequests()).append(", GET: ").append(getGetRequests()).append(")")
                .append(", IOTime: ").append(prettyMs(getIoTimeMs()))
                .append(", Retries: ").append(getRetries());
        String slowest = getSlowestRequest();
        if (slowest != null) {
            sb.append(", Slowest: ").append(slowest).append(" ").append(prettyMs(getMaxRequestMs()));
        }
        return sb.toString();
    }

    private static String prettyMs(long ms) {
        return ms == 0 ? "0ms" : DebugUtil.getPrettyStringMs(ms);
    }

    /**
     * An executor running each task with these stats installed. Only execute is forwarded; the pool is shared,
     * so this view never shuts it down.
     */
    public ExecutorService wrap(ExecutorService pool) {
        return new PropagatingExecutor(pool, this);
    }

    private static final class PropagatingExecutor extends AbstractExecutorService {
        private final ExecutorService pool;
        private final IcebergPlanningIoStats stats;

        private PropagatingExecutor(ExecutorService pool, IcebergPlanningIoStats stats) {
            this.pool = pool;
            this.stats = stats;
        }

        @Override
        public void execute(Runnable command) {
            pool.execute(() -> {
                try (Scope ignored = stats.enter()) {
                    command.run();
                }
            });
        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException("the planning pool is shared");
        }

        @Override
        public List<Runnable> shutdownNow() {
            throw new UnsupportedOperationException("the planning pool is shared");
        }

        @Override
        public boolean isShutdown() {
            return pool.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return pool.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return pool.awaitTermination(timeout, unit);
        }
    }
}
