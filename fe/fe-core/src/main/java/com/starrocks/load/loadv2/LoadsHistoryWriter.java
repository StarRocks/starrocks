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

package com.starrocks.load.loadv2;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.metric.GaugeMetricImpl;
import com.starrocks.metric.LongCounterMetric;
import com.starrocks.metric.MetricRepo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TLoadInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Buffers {@code information_schema.loads} rows and writes them to a table in batches.
 *
 * <p>Producers call {@link #offer(Supplier)} from the thread that changes a load's state;
 * that converts the job to its {@link TLoadInfo}, renders the VALUES tuple and appends it to
 * an in-memory queue. One daemon thread drains the queue through {@link #runOnce(int, long)},
 * turning a batch into a single multi-row {@code INSERT ... VALUES}. The {@link Target}
 * decides which table the batch goes to, which rows belong there and how a row renders; the
 * static entry points of this class feed the {@code _statistics_.loads_history} target, which
 * takes the row of every load that reached a final state and renders it with the column list
 * of {@link LoadsHistorySyncer#HISTORY_COLUMNS}, so the table keeps lining up with the view.
 *
 * <p>loads_history has no key, so a load must not be written here and copied by the backstop of
 * {@link LoadsHistorySyncer} as well. A row that arrives shortly after its load finished is
 * written before the backstop considers the load. For a row that arrives later, and for a row
 * held back until its load is visible ({@link #holdUntilVisible(long, Supplier)}), the backstop
 * is given the load id to leave out.
 */
public class LoadsHistoryWriter {
    private static final Logger LOG = LogManager.getLogger(LoadsHistoryWriter.class);

    // Longest value the varchar(2048) columns of loads_history accept, in bytes.
    private static final int MAX_VARCHAR_BYTES = 2048;

    // A batch that failed this many times in a row is split in halves; a single row that
    // failed this many times is dropped. Failures while the table cannot be reached at all
    // are not counted.
    static final int MAX_BATCH_ATTEMPTS = 3;

    // The backstop only copies loads that finished at least this long ago, see LoadsHistorySyncer.
    static final long SETTLE_MS = 60_000L;

    // A row that arrives this long after its load finished is a late row: the backstop could
    // copy the load while the row waits in the queue, and the table has no key to merge the
    // two. The backstop is told to leave the loads of queued late rows alone.
    static final long FRESH_ROW_MS = 45_000L;

    // How many loads the backstop can be told to leave alone at a time: late rows in the queue
    // plus rows held until their load is visible.
    static final int MAX_BACKSTOP_EXCLUSIONS = 1000;

    // Longest wait between two attempts after failed flushes.
    static final long MAX_BACKOFF_MS = 60_000L;

    // Failed flushes are logged with their stack trace at most this often.
    private static final long FAILURE_LOG_INTERVAL_MS = 60_000L;

    // With milliseconds, as the BE fills the datetime columns of the loads view from the same
    // epoch-millisecond fields; the backstop copies those values.
    private static final DateTimeFormatter DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private static final String HISTORY_TABLE = CatalogUtils.normalizeTableName(
            LoadsHistorySyncer.LOADS_HISTORY_DB_NAME, LoadsHistorySyncer.LOADS_HISTORY_TABLE_NAME);

    /** The table a writer fills and the rules for the rows it takes. */
    public interface Target {
        /** Normalized {@code db.table} name, for log messages. */
        String tableName();

        /** Whether the row belongs in the table; rows that do not are silently skipped. */
        boolean accepts(TLoadInfo info);

        /** The VALUES tuple of one row, in the order of {@link #insertPrefix()}'s column list. */
        String renderRow(TLoadInfo info);

        /** {@code INSERT INTO table (columns) VALUES }, which the rendered tuples follow. */
        String insertPrefix();

        /** Counter of rows dropped from a full queue; null while metrics are not initialized. */
        LongCounterMetric droppedCounter();

        /** Gauge of the last successful flush duration; null while metrics are not initialized. */
        GaugeMetricImpl<Long> flushLatencyGauge();

        /**
         * Whether a backstop statement copies loads into the table as well. Only for a table
         * without a key, where the backstop and the queue would otherwise write the same load
         * twice: the backstop is then kept away from the loads of late rows by id.
         */
        default boolean hasBackstop() {
            return false;
        }

        /**
         * The key under which the table keeps one row, or null if the table has no key. A
         * row offered for a key that is still queued replaces the queued one, as the write
         * of the newer row would replace the older one in the table anyway.
         */
        default String keyOf(TLoadInfo info) {
            return null;
        }
    }

    /** Finished loads, rendered in {@code loads} view column order, for loads_history. */
    static final Target HISTORY_TARGET = new Target() {
        @Override
        public String tableName() {
            return HISTORY_TABLE;
        }

        @Override
        public boolean accepts(TLoadInfo info) {
            // Not a final row: the table requires load_finish_time.
            return info.isSetLoad_finish_time_ms() && info.getLoad_finish_time_ms() > 0;
        }

        @Override
        public String renderRow(TLoadInfo info) {
            return LoadsHistoryWriter.renderRow(info);
        }

        @Override
        public String insertPrefix() {
            return INSERT_PREFIX;
        }

        @Override
        public LongCounterMetric droppedCounter() {
            return MetricRepo.hasInit ? MetricRepo.COUNTER_LOADS_HISTORY_DROPPED : null;
        }

        @Override
        public GaugeMetricImpl<Long> flushLatencyGauge() {
            return MetricRepo.hasInit ? MetricRepo.GAUGE_LOADS_HISTORY_FLUSH_LATENCY_MS : null;
        }

        @Override
        public boolean hasBackstop() {
            return true;
        }
    };

    /** Writes one INSERT; {@code rows} is only for logging. */
    @FunctionalInterface
    public interface Sink {
        void write(String sql, int rows);
    }

    /** Waits between attempts; replaced in tests. */
    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    /** What one {@link #runOnce(int, long)} did. */
    public enum FlushResult {
        /** Nothing was pending. */
        NOTHING,
        /** Everything that was pending when the batch was taken has been written. */
        DRAINED,
        /** A full batch was written and older rows may still be pending. */
        BACKLOG,
        /** The write failed; the rows were kept for retry (or dropped after too many attempts). */
        FAILED
    }

    /** A rendered VALUES tuple waiting to be written. */
    static final class Row {
        final long id;
        final String label;
        // Null for a table without a key.
        final String key;
        // Replaced under the queue lock when a newer row of the same key arrives.
        String values;
        // Approximate heap footprint used for the byte bound of the queue.
        int bytes;
        // When the load finished.
        final long finishMs;
        // Whether the backstop is kept away from this load by id while the row is queued.
        final boolean late;

        Row(String label, String values) {
            this(-1L, label, values, 0L, false);
        }

        Row(long id, String label, String values, long finishMs, boolean late) {
            this(id, label, null, values, finishMs, late);
        }

        Row(String label, String key, String values) {
            this(-1L, label, key, values, 0L, false);
        }

        Row(long id, String label, String key, String values, long finishMs, boolean late) {
            this.id = id;
            this.label = label;
            this.key = key;
            this.values = values;
            this.bytes = values.length();
            this.finishMs = finishMs;
            this.late = late;
        }
    }

    /** What a backstop run has to respect, see {@link #beginBackstop()}. */
    static final class BackstopScope {
        // Loads the run must leave out: rows held, and late rows in the queue.
        final List<Long> excludedIds;
        // The earliest finish time among the other rows in the queue, Long.MAX_VALUE if there
        // is none. The run must only copy loads that finished before it.
        final long oldestQueuedFinishMs;
        // The earliest finish time among the loads left to the backstop that no run has
        // covered yet, 0 if there is none. The run should reach back that far.
        final long oldestLostFinishMs;

        BackstopScope(List<Long> excludedIds, long oldestQueuedFinishMs, long oldestLostFinishMs) {
            this.excludedIds = excludedIds;
            this.oldestQueuedFinishMs = oldestQueuedFinishMs;
            this.oldestLostFinishMs = oldestLostFinishMs;
        }
    }

    /** A row that is written once its load is visible, or when waiting for that is given up. */
    private static final class Hold {
        final Supplier<TLoadInfo> row;
        final long heldAtMs;

        Hold(Supplier<TLoadInfo> row, long heldAtMs) {
            this.row = row;
            this.heldAtMs = heldAtMs;
        }
    }

    /**
     * The history writer that accepts rows on this node. Set while the leader daemon that
     * drains it runs, null on followers, so a job finishing on a node that is not the leader
     * is ignored.
     */
    private static volatile LoadsHistoryWriter active;

    private final Target target;
    // Read on every enqueue so that a smaller limit set at runtime takes effect at once.
    private final IntSupplier maxRows;
    private final LongSupplier maxBytes;
    private final Sink sink;
    // Whether the table can be written at all right now (some backend or compute node is alive).
    private final BooleanSupplier tableReachable;
    private LongSupplier clock = System::currentTimeMillis;
    private Sleeper sleeper = Thread::sleep;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition batchReady = lock.newCondition();
    // Pending rows, oldest first. The queue lives only for one leader session: whatever is
    // still here when this FE stops being leader is thrown away by clear(), and the backstop
    // sync on the next leader re-reads those loads from information_schema.loads instead.
    private final ArrayDeque<Row> queue = new ArrayDeque<>();
    // The queued row of each key, for targets with a key: at most one row per key is queued,
    // so a batch never carries two rows of one key and a newer row is never written before
    // an older one of the same load.
    private final Map<String, Row> queuedByKey = new HashMap<>();
    private long queuedBytes = 0L;
    private final AtomicLong coalescedTotal = new AtomicLong();
    private final AtomicLong droppedTotal = new AtomicLong();
    // Rows left to the backstop (dropped from the queue, or not queued), guarded by the lock:
    // when that last happened, and when it first happened among those no backstop run has
    // covered yet. 0 if there is none in this leader session.
    private long lastBackstopRequestMs = 0L;
    private long oldestBackstopRequestMs = 0L;
    // The earliest finish time among those loads, 0 if there is none.
    private long oldestLostFinishMs = 0L;
    // Loads the backstop must leave alone, guarded by the lock: rows held until their load is
    // visible, by load id, and the ids of the late rows in the queue.
    private final Map<Long, Hold> holds = new LinkedHashMap<>();
    private final Set<Long> lateIds = new HashSet<>();
    // Whether a backstop statement is running; it was built without the late rows that arrive now.
    private boolean backstopInFlight = false;
    private volatile int consecutiveFailures = 0;
    // The next attempt after a failed flush does not start before this time.
    private volatile long retryNotBeforeMs = 0L;
    private volatile long lastFailureLogMs = 0L;
    // When the last batch was taken off the queue (empty or not).
    private volatile long lastTakeMs = 0L;
    private volatile long lastFlushLatencyMs = -1L;
    // Retry state of the rows at the head of the queue: how often the current head batch
    // failed and how many rows the next attempt may take (Integer.MAX_VALUE: a full batch).
    private int headAttempts = 0;
    private int headLimit = Integer.MAX_VALUE;

    public LoadsHistoryWriter(Target target, IntSupplier maxRows, LongSupplier maxBytes, Sink sink) {
        this(target, maxRows, maxBytes, sink, () -> true);
    }

    public LoadsHistoryWriter(Target target, IntSupplier maxRows, LongSupplier maxBytes, Sink sink,
                              BooleanSupplier tableReachable) {
        this.target = target;
        this.maxRows = maxRows;
        this.maxBytes = maxBytes;
        this.sink = sink;
        this.tableReachable = tableReachable;
    }

    @VisibleForTesting
    void setClock(LongSupplier clock, Sleeper sleeper) {
        this.clock = clock;
        this.sleeper = sleeper;
    }

    public LoadsHistoryWriter(IntSupplier maxRows, LongSupplier maxBytes, Sink sink) {
        this(HISTORY_TARGET, maxRows, maxBytes, sink);
    }

    public LoadsHistoryWriter(IntSupplier maxRows, LongSupplier maxBytes, Sink sink, BooleanSupplier tableReachable) {
        this(HISTORY_TARGET, maxRows, maxBytes, sink, tableReachable);
    }

    @VisibleForTesting
    LoadsHistoryWriter(int maxRows, Sink sink) {
        this(() -> maxRows, () -> Long.MAX_VALUE, sink);
    }

    @VisibleForTesting
    LoadsHistoryWriter(Target target, int maxRows, Sink sink) {
        this(target, () -> maxRows, () -> Long.MAX_VALUE, sink);
    }

    public Target getTarget() {
        return target;
    }

    static void activate(LoadsHistoryWriter writer) {
        active = writer;
    }

    static void deactivate(LoadsHistoryWriter writer) {
        if (active == writer) {
            active = null;
        }
    }

    /**
     * Enqueue the row of a load that just reached a final state. The supplier is only
     * invoked when this node accepts rows, so followers do not pay for the conversion.
     */
    public static void offer(Supplier<TLoadInfo> row) {
        offerTo(active, row);
    }

    /** Same as {@link #offer(Supplier)} for jobs whose conversion yields a list of rows. */
    public static void offerAll(Supplier<List<TLoadInfo>> rows) {
        offerAllTo(active, rows);
    }

    /** Enqueue one row in {@code writer} unless it is null, swallowing conversion failures. */
    static void offerTo(LoadsHistoryWriter writer, Supplier<TLoadInfo> row) {
        if (writer == null || isCheckpointThread()) {
            return;
        }
        try {
            writer.enqueue(row.get());
        } catch (Throwable e) {
            LOG.warn("failed to record load for {}", writer.target.tableName(), e);
        }
    }

    /** Same as {@link #offerTo} for jobs whose conversion yields a list of rows. */
    static void offerAllTo(LoadsHistoryWriter writer, Supplier<List<TLoadInfo>> rows) {
        if (writer == null || isCheckpointThread()) {
            return;
        }
        try {
            for (TLoadInfo row : rows.get()) {
                writer.enqueue(row);
            }
        } catch (Throwable e) {
            LOG.warn("failed to record load for {}", writer.target.tableName(), e);
        }
    }

    /**
     * Take over the row of a load that is FINISHED while its transaction is only committed:
     * the row is written once {@code row} reports a visible time, or as it is when the load
     * has not become visible within half the backstop look-back. Until then the backstop
     * leaves the load alone. Returns false when this node does not accept rows or holds too
     * many already; the caller then records the row as it is.
     */
    public static boolean holdUntilVisible(long id, Supplier<TLoadInfo> row) {
        LoadsHistoryWriter writer = active;
        if (writer == null || isCheckpointThread()) {
            return false;
        }
        try {
            return writer.hold(id, row);
        } catch (Throwable e) {
            LOG.warn("failed to hold the loads_history row of load {}", id, e);
            return false;
        }
    }

    @VisibleForTesting
    boolean hold(long id, Supplier<TLoadInfo> row) {
        lock.lock();
        try {
            if (holds.size() + lateIds.size() >= MAX_BACKSTOP_EXCLUSIONS) {
                return false;
            }
            holds.put(id, new Hold(row, clock.getAsLong()));
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Queue the held rows whose load became visible, and those held for longer than half the
     * backstop look-back as they are. Called by the thread that also runs the backstop, so a
     * released row never arrives while a backstop statement is running.
     */
    public void releaseHolds() {
        List<Map.Entry<Long, Hold>> held;
        lock.lock();
        try {
            if (holds.isEmpty()) {
                return;
            }
            held = new ArrayList<>(holds.entrySet());
        } finally {
            lock.unlock();
        }
        long now = clock.getAsLong();
        for (Map.Entry<Long, Hold> entry : held) {
            TLoadInfo info;
            try {
                info = entry.getValue().row.get();
            } catch (Throwable e) {
                LOG.warn("failed to read the held loads_history row of load {}, leaving it to the backstop",
                        entry.getKey(), e);
                info = null;
            }
            if (info == null) {
                lock.lock();
                try {
                    holds.remove(entry.getKey());
                    requestBackstop(now, entry.getValue().heldAtMs);
                } finally {
                    lock.unlock();
                }
            } else if (info.isSetVisible_time_ms() || now - entry.getValue().heldAtMs >= maxHoldMs()) {
                enqueue(info, entry.getKey());
            }
        }
    }

    /** Rows waiting on this node, for the queue gauge. */
    public static long queuedRows() {
        return queuedRows(active);
    }

    // The checkpoint thread works on a copy of the metadata built from the image and the
    // journal. The writers are per JVM, so on a leader that checkpoints itself a state change
    // replayed into that copy would otherwise be recorded as if the load had just changed.
    private static boolean isCheckpointThread() {
        try {
            return GlobalStateMgr.isCheckpointThread();
        } catch (Throwable e) {
            return false;
        }
    }

    static long queuedRows(LoadsHistoryWriter writer) {
        return writer == null ? 0L : writer.queueSize();
    }

    public void enqueue(TLoadInfo info) {
        enqueue(info, null);
    }

    // heldId: the hold this row is released from, null for a row that was just offered.
    private void enqueue(TLoadInfo info, Long heldId) {
        // Not a row of this table, or a flush of the writers themselves: each flush is an INSERT
        // job, and recording those would make the writers feed themselves.
        boolean skipped = info == null || !target.accepts(info) || isInternalLoadTable(info);
        if (skipped && heldId == null) {
            return;
        }
        long now = clock.getAsLong();
        // The load finished a while before its row got here (a job that ends at its creation
        // time, a row held until its load was visible), so the backstop may copy the load while
        // the row waits in the queue.
        // Only a table that a backstop copies into as well has late rows.
        boolean late = !skipped && target.hasBackstop()
                && (heldId != null || now - info.getLoad_finish_time_ms() >= FRESH_ROW_MS);
        Row row = skipped ? null : new Row(info.getJob_id(), info.getLabel(), target.keyOf(info),
                target.renderRow(info), info.isSetLoad_finish_time_ms() ? info.getLoad_finish_time_ms() : 0L, late);
        lock.lock();
        try {
            if (heldId != null) {
                holds.remove(heldId);
            }
            if (row == null) {
                return;
            }
            if (late) {
                // The backstop cannot be told about this load any more (a statement is running),
                // or is told about too many already: leave the load to the next backstop run.
                boolean untold = backstopInFlight || holds.size() + lateIds.size() >= MAX_BACKSTOP_EXCLUSIONS;
                if (untold && heldId == null) {
                    requestBackstop(now, row.finishMs);
                    return;
                }
                lateIds.add(row.id);
            }
            int rowLimit = Math.max(1, maxRows.getAsInt());
            long byteLimit = maxBytes.getAsLong();
            Row queued = row.key == null ? null : queuedByKey.get(row.key);
            if (queued != null) {
                // The same load changed again before its row was written: keep its place in the
                // queue and write only the newer state. The newer row may be larger and the
                // limits may have been lowered, so the bounds are enforced here as well.
                queuedBytes += row.bytes - queued.bytes;
                queued.values = row.values;
                queued.bytes = row.bytes;
                coalescedTotal.incrementAndGet();
                int dropped = 0;
                while (queue.size() > 1 && (queue.size() > rowLimit || queuedBytes > byteLimit)) {
                    unqueue(leaveToBackstop(queue.pollFirst()));
                    dropped++;
                }
                countDropped(dropped);
                return;
            }
            int dropped = 0;
            while (!queue.isEmpty() && (queue.size() >= rowLimit || queuedBytes + row.bytes > byteLimit)) {
                unqueue(leaveToBackstop(queue.pollFirst()));
                dropped++;
            }
            countDropped(dropped);
            queue.addLast(row);
            queuedBytes += row.bytes;
            if (row.key != null) {
                queuedByKey.put(row.key, row);
            }
            batchReady.signal();
        } finally {
            lock.unlock();
        }
    }

    /** Whether the load wrote one of the internal load-history tables the writers fill themselves. */
    static boolean isInternalLoadTable(TLoadInfo info) {
        return isInternalLoadTable(info.getDb(), info.getTable());
    }

    /** Whether the table is one of the internal load-history tables the writers fill themselves. */
    public static boolean isInternalLoadTable(String dbName, String tableName) {
        if (!LoadsHistorySyncer.LOADS_HISTORY_DB_NAME.equalsIgnoreCase(dbName)) {
            return false;
        }
        for (String table : LoadsHistorySyncer.INTERNAL_LOAD_TABLES) {
            if (table.equalsIgnoreCase(tableName)) {
                return true;
            }
        }
        return false;
    }

    // How long a row is held for its load to become visible: half of the backstop's look-back.
    private static long maxHoldMs() {
        return LoadsHistorySyncer.backstopLookbackMinutes() * 60_000L / 2;
    }

    /** Caller holds the lock. Accounts for a row that just left the queue. */
    private void unqueue(Row row) {
        queuedBytes -= row.bytes;
        if (row.key != null) {
            queuedByKey.remove(row.key, row);
        }
    }

    /** Caller holds the lock. The row leaves the queue: the backstop may copy its load again. */
    private Row forget(Row row) {
        if (row.late) {
            lateIds.remove(row.id);
        }
        return row;
    }

    /** Caller holds the lock. The row is dropped from the queue and its load left to the backstop. */
    private Row leaveToBackstop(Row row) {
        requestBackstop(clock.getAsLong(), row.finishMs);
        return forget(row);
    }

    /** Caller holds the lock. A load that finished at {@code finishMs} was left to the backstop at {@code now}. */
    private void requestBackstop(long now, long finishMs) {
        lastBackstopRequestMs = Math.max(lastBackstopRequestMs, now);
        if (oldestBackstopRequestMs == 0L || now < oldestBackstopRequestMs) {
            oldestBackstopRequestMs = now;
        }
        if (finishMs > 0 && (oldestLostFinishMs == 0L || finishMs < oldestLostFinishMs)) {
            oldestLostFinishMs = finishMs;
        }
    }

    /** Caller holds the lock. */
    private void countDropped(int dropped) {
        if (dropped <= 0) {
            return;
        }
        droppedTotal.addAndGet(dropped);
        LongCounterMetric counter = target.droppedCounter();
        if (counter != null) {
            counter.increase((long) dropped);
        }
    }

    /**
     * Wait until {@code batchRows} rows are pending or {@code intervalMs} elapsed, then write
     * what is pending (at most {@code batchRows} rows) in one statement.
     */
    public FlushResult runOnce(int batchRows, long intervalMs) throws InterruptedException {
        // After a failed flush the next attempt waits, longer after each failure in a row; without
        // it a queue that holds a full batch would retry in a tight loop.
        long waitMs = retryNotBeforeMs - clock.getAsLong();
        if (waitMs > 0) {
            sleeper.sleep(waitMs);
        }
        List<Row> batch = takeBatch(Math.max(1, batchRows), intervalMs);
        if (batch.isEmpty()) {
            return FlushResult.NOTHING;
        }
        if (!flush(batch)) {
            retryNotBeforeMs = clock.getAsLong() + backoffMs(intervalMs, consecutiveFailures);
            return FlushResult.FAILED;
        }
        // The batch was capped by the configured size or, after failures, by the split limit;
        // only a batch below the cap actually used proves that nothing older is still queued.
        return batch.size() < lastTakeLimit ? FlushResult.DRAINED : FlushResult.BACKLOG;
    }

    // The cap the last takeBatch() applied, read by the daemon thread right after it.
    private volatile int lastTakeLimit = Integer.MAX_VALUE;

    /** The wait after {@code failures} failed flushes in a row: the flush interval, doubled per failure. */
    @VisibleForTesting
    static long backoffMs(long intervalMs, int failures) {
        if (intervalMs <= 0 || failures <= 0) {
            return 0L;
        }
        return Math.min(MAX_BACKOFF_MS, intervalMs << Math.min(failures - 1, 16));
    }

    @VisibleForTesting
    List<Row> takeBatch(int batchRows, long waitMs) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            int limit = Math.min(batchRows, headLimit);
            lastTakeLimit = limit;
            long remainingNanos = TimeUnit.MILLISECONDS.toNanos(waitMs);
            while (queue.size() < limit && remainingNanos > 0) {
                remainingNanos = batchReady.awaitNanos(remainingNanos);
            }
            int n = Math.min(queue.size(), limit);
            List<Row> batch = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                Row row = queue.pollFirst();
                unqueue(row);
                batch.add(row);
            }
            lastTakeMs = clock.getAsLong();
            return batch;
        } finally {
            lock.unlock();
        }
    }

    @VisibleForTesting
    boolean flush(List<Row> batch) {
        String sql = buildInsertSql(target.insertPrefix(), batch);
        long start = System.currentTimeMillis();
        try {
            sink.write(sql, batch.size());
        } catch (Throwable e) {
            consecutiveFailures++;
            onFlushFailed(batch, e);
            return false;
        }
        retryNotBeforeMs = 0L;
        lastFlushLatencyMs = System.currentTimeMillis() - start;
        GaugeMetricImpl<Long> latency = target.flushLatencyGauge();
        if (latency != null) {
            latency.setValue(lastFlushLatencyMs);
        }
        if (consecutiveFailures > 0) {
            LOG.info("writing to {} recovered after {} failed flushes", target.tableName(), consecutiveFailures);
            consecutiveFailures = 0;
        }
        lock.lock();
        try {
            headAttempts = 0;
            headLimit = Integer.MAX_VALUE;
            batch.forEach(this::forget);
        } finally {
            lock.unlock();
        }
        return true;
    }

    /**
     * Put a failed batch back in front of the queue. After {@link #MAX_BATCH_ATTEMPTS} failures
     * the next attempt takes only half of it, so that a row the table rejects is isolated and
     * finally dropped instead of blocking everything behind it. A failure while the table
     * cannot be reached says nothing about the rows, so it does not count as an attempt.
     */
    private void onFlushFailed(List<Row> batch, Throwable cause) {
        boolean reachable = isTableReachable() && !isClusterSideFailure(cause);
        // The stack trace is logged for the first failure and then once in a while.
        long now = clock.getAsLong();
        boolean verbose = consecutiveFailures <= 1 || now - lastFailureLogMs >= FAILURE_LOG_INTERVAL_MS;
        if (verbose) {
            lastFailureLogMs = now;
        }
        lock.lock();
        try {
            if (!reachable) {
                if (verbose) {
                    LOG.warn("failed to write {} rows to {}, the table cannot be written right now ({} failed flushes"
                            + " in a row), rows are kept for retry", batch.size(), target.tableName(),
                            consecutiveFailures, cause);
                }
                requeue(batch);
                return;
            }
            headAttempts++;
            if (headAttempts < MAX_BATCH_ATTEMPTS) {
                if (verbose) {
                    LOG.warn("failed to write {} rows to {} (attempt {}, {} failed flushes in a row), rows are kept"
                            + " for retry", batch.size(), target.tableName(), headAttempts, consecutiveFailures, cause);
                }
                requeue(batch);
                return;
            }
            headAttempts = 0;
            if (batch.size() == 1) {
                Row row = batch.get(0);
                LOG.warn("dropping {} row of load {} after {} failed attempts, it is left to the next backstop"
                        + " or reconciliation of the table, which recovers it while the load is still tracked: {}",
                        target.tableName(), row.label, MAX_BATCH_ATTEMPTS, cause.getMessage());
                headLimit = Integer.MAX_VALUE;
                leaveToBackstop(row);
                countDropped(1);
                return;
            }
            headLimit = Math.max(1, batch.size() / 2);
            LOG.warn("failed to write {} rows to {} {} times, retrying in batches of {}: {}",
                    batch.size(), target.tableName(), MAX_BATCH_ATTEMPTS, headLimit, cause.getMessage());
            requeue(batch);
        } finally {
            lock.unlock();
        }
    }

    // Messages of failures that no row of the batch can have caused: no node or replica to
    // write to, or a request that did not get through.
    private static final List<String> CLUSTER_SIDE_FAILURES = ImmutableList.of(
            "no alive backend", "no alive compute node", "no available", "not alive", "node not found",
            "no queryable replica", "replicas less than quorum", "rpcexception", "rpc failed", "connection refused",
            "timeout", "timed out");

    /** Whether the failure says nothing about the rows, so that it must not count as an attempt. */
    @VisibleForTesting
    static boolean isClusterSideFailure(Throwable cause) {
        for (Throwable t = cause; t != null; t = t.getCause() == t ? null : t.getCause()) {
            String message = t.getMessage();
            if (message == null) {
                continue;
            }
            String lower = message.toLowerCase();
            for (String part : CLUSTER_SIDE_FAILURES) {
                if (lower.contains(part)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isTableReachable() {
        try {
            return tableReachable.getAsBoolean();
        } catch (Throwable e) {
            return true;
        }
    }

    /**
     * Caller holds the lock. Re-inserts the batch at the head, dropping the oldest rows over the
     * bounds. A row whose key was queued again while the batch was being written is not put
     * back: the queued row is newer, and writing the old one after it would undo it.
     */
    private void requeue(List<Row> batch) {
        for (int i = batch.size() - 1; i >= 0; i--) {
            Row row = batch.get(i);
            if (row.key != null) {
                if (queuedByKey.containsKey(row.key)) {
                    coalescedTotal.incrementAndGet();
                    forget(row);
                    continue;
                }
                queuedByKey.put(row.key, row);
            }
            queue.addFirst(row);
            queuedBytes += row.bytes;
        }
        int rowLimit = Math.max(1, maxRows.getAsInt());
        long byteLimit = maxBytes.getAsLong();
        int dropped = 0;
        while (queue.size() > 1 && (queue.size() > rowLimit || queuedBytes > byteLimit)) {
            unqueue(leaveToBackstop(queue.pollFirst()));
            dropped++;
        }
        countDropped(dropped);
    }

    /** Discard everything pending. Called when this FE stops being leader. */
    public void clear() {
        lock.lock();
        try {
            queue.clear();
            queuedByKey.clear();
            queuedBytes = 0L;
            headAttempts = 0;
            headLimit = Integer.MAX_VALUE;
            holds.clear();
            lateIds.clear();
            backstopInFlight = false;
            lastBackstopRequestMs = 0L;
            oldestBackstopRequestMs = 0L;
            oldestLostFinishMs = 0L;
        } finally {
            lock.unlock();
        }
        consecutiveFailures = 0;
        retryNotBeforeMs = 0L;
        lastFailureLogMs = 0L;
    }

    public int queueSize() {
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    public long queuedBytes() {
        lock.lock();
        try {
            return queuedBytes;
        } finally {
            lock.unlock();
        }
    }

    public long droppedTotal() {
        return droppedTotal.get();
    }

    /** Rows that were replaced by a newer row of the same key before they were written. */
    public long coalescedTotal() {
        return coalescedTotal.get();
    }

    public int consecutiveFailures() {
        return consecutiveFailures;
    }

    public long lastFlushLatencyMs() {
        return lastFlushLatencyMs;
    }

    /**
     * When a row was last left to the backstop, in epoch milliseconds: a row dropped because the
     * queue was full or because it failed too often, or a late row that could not be queued.
     * 0 if there was none since this FE became leader.
     */
    public long lastBackstopRequestMs() {
        lock.lock();
        try {
            return lastBackstopRequestMs;
        } finally {
            lock.unlock();
        }
    }

    /**
     * When the first row was left to the backstop among those no backstop run has covered yet,
     * 0 if there is none. The backstop has to run once that moment lies past its settle window;
     * rows left to it later do not postpone that run, they wait for the next one.
     */
    public long oldestBackstopRequestMs() {
        lock.lock();
        try {
            return oldestBackstopRequestMs;
        } finally {
            lock.unlock();
        }
    }

    /** A backstop run copied every load that was left to it up to {@code coveredUntilMs}. */
    public void backstopCovered(long coveredUntilMs) {
        lock.lock();
        try {
            if (lastBackstopRequestMs <= coveredUntilMs) {
                oldestBackstopRequestMs = 0L;
                oldestLostFinishMs = 0L;
            } else if (oldestBackstopRequestMs != 0L && oldestBackstopRequestMs <= coveredUntilMs) {
                // Only the first and the last moment are kept; what is left was asked for after
                // the covered time.
                oldestBackstopRequestMs = coveredUntilMs + 1;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Start of a backstop run: what the run has to leave to this writer. The loads of the held
     * rows and of the late rows in the queue are left out by id; the other rows in the queue
     * arrived shortly after their load finished, so the run leaves them out by only copying
     * loads that finished before the oldest of them. Until {@link #endBackstop()} a late row
     * is not queued; a row that arrives shortly after its load finished still is, the run does
     * not consider loads that young.
     */
    public BackstopScope beginBackstop() {
        lock.lock();
        try {
            backstopInFlight = true;
            List<Long> ids = new ArrayList<>(holds.size() + lateIds.size());
            ids.addAll(holds.keySet());
            ids.addAll(lateIds);
            Collections.sort(ids);
            return new BackstopScope(ids, oldestQueuedFinishLocked(), oldestLostFinishMs);
        } finally {
            lock.unlock();
        }
    }

    /** The earliest finish time among the queued rows that are not late, Long.MAX_VALUE if there is none. */
    public long oldestQueuedFinishMs() {
        lock.lock();
        try {
            return oldestQueuedFinishLocked();
        } finally {
            lock.unlock();
        }
    }

    private long oldestQueuedFinishLocked() {
        long oldest = Long.MAX_VALUE;
        for (Row row : queue) {
            if (!row.late && row.finishMs > 0 && row.finishMs < oldest) {
                oldest = row.finishMs;
            }
        }
        return oldest;
    }

    public void endBackstop() {
        lock.lock();
        try {
            backstopInFlight = false;
        } finally {
            lock.unlock();
        }
    }

    @VisibleForTesting
    int heldRows() {
        lock.lock();
        try {
            return holds.size();
        } finally {
            lock.unlock();
        }
    }

    /** When the last batch was taken off the queue; rows queued now arrived after that. */
    public long lastTakeMs() {
        return lastTakeMs;
    }

    // ---- SQL rendering -------------------------------------------------------------------

    /**
     * How each column of loads_history is rendered from the loads view row. The column set
     * and order come from {@link LoadsHistorySyncer#HISTORY_COLUMNS}; this only maps names
     * to values, mirroring what the BE fills into the view for the same thrift field.
     */
    static final Map<String, Function<TLoadInfo, String>> RENDERERS =
            ImmutableMap.<String, Function<TLoadInfo, String>>builder()
            .put("id", info -> Long.toString(info.getJob_id()))
            .put("label", info -> text(info.getLabel()))
            .put("profile_id", info -> nullableText(info.isSetProfile_id(), info.getProfile_id()))
            .put("db_name", info -> text(info.getDb()))
            .put("table_name", info -> text(info.getTable()))
            .put("user", info -> text(info.getUser()))
            .put("warehouse", info -> nullableText(info.isSetWarehouse(), info.getWarehouse()))
            .put("state", info -> text(info.getState()))
            .put("progress", info -> text(info.getProgress()))
            .put("type", info -> text(info.getType()))
            .put("priority", info -> text(info.getPriority()))
            .put("scan_rows", info -> Long.toString(info.getNum_scan_rows()))
            .put("scan_bytes", info -> Long.toString(info.getNum_scan_bytes()))
            .put("filtered_rows", info -> Long.toString(info.getNum_filtered_rows()))
            .put("unselected_rows", info -> Long.toString(info.getNum_unselected_rows()))
            .put("sink_rows", info -> Long.toString(info.getNum_sink_rows()))
            .put("runtime_details", info -> json(info.getRuntime_details()))
            .put("create_time", info -> datetime(info.isSetCreate_time_ms(), info.getCreate_time_ms()))
            .put("load_start_time", info -> datetime(info.isSetLoad_start_time_ms(), info.getLoad_start_time_ms()))
            .put("load_commit_time", info -> datetime(info.isSetLoad_commit_time_ms(), info.getLoad_commit_time_ms()))
            .put("load_finish_time", info -> datetime(info.isSetLoad_finish_time_ms(), info.getLoad_finish_time_ms()))
            .put("properties", info -> json(info.getProperties()))
            .put("error_msg", info -> nullableText(info.isSetError_msg(), info.getError_msg()))
            .put("tracking_sql", info -> nullableText(info.isSetTracking_sql(), info.getTracking_sql()))
            .put("rejected_record_path",
                    info -> nullableText(info.isSetRejected_record_path(), info.getRejected_record_path()))
            .put("job_id", info -> Long.toString(info.getJob_id()))
            .put("txn_id", info -> nullableLong(info.isSetTxn_id() && info.getTxn_id() >= 0, info.getTxn_id()))
            .put("load_id", info -> nullableText(info.isSetLoad_id(), info.getLoad_id()))
            .put("table_id", info -> nullableLong(info.isSetTable_id(), info.getTable_id()))
            .put("parent_id", info -> nullableLong(info.isSetParent_id(), info.getParent_id()))
            .put("statement", info -> nullableText(info.isSetStatement(), info.getStatement()))
            .put("error_code", info -> nullableText(info.isSetError_code(), info.getError_code()))
            .put("visible_time", info -> datetime(info.isSetVisible_time_ms(), info.getVisible_time_ms()))
            .put("sink_bytes", info -> nullableLong(info.isSetSink_bytes(), info.getSink_bytes()))
            .put("client_ip", info -> nullableText(info.isSetClient_ip(), info.getClient_ip()))
            .put("be_ids", info -> nullableText(info.isSetBe_ids(), info.getBe_ids()))
            .put("commit_throttle_ms", info -> nullableLong(info.isSetCommit_throttle_ms(), info.getCommit_throttle_ms()))
            .put("cpu_ns", info -> nullableLong(info.isSetCpu_ns(), info.getCpu_ns()))
            .put("peak_mem_bytes", info -> nullableLong(info.isSetPeak_mem_bytes(), info.getPeak_mem_bytes()))
            .build();

    // Renderers in archive column order; fails at class load if the view gained a column
    // that has no rendering here.
    private static final List<Function<TLoadInfo, String>> COLUMN_RENDERERS = LoadsHistorySyncer.HISTORY_COLUMNS.stream()
            .map(column -> {
                Function<TLoadInfo, String> renderer = RENDERERS.get(column.first);
                if (renderer == null) {
                    throw new IllegalStateException("no loads_history rendering for column " + column.first);
                }
                return renderer;
            })
            .collect(ImmutableList.toImmutableList());

    private static final String INSERT_PREFIX =
            "INSERT INTO " + HISTORY_TABLE + " (" + LoadsHistorySyncer.columnList() + ") VALUES ";

    static String buildInsertSql(List<Row> rows) {
        return buildInsertSql(INSERT_PREFIX, rows);
    }

    static String buildInsertSql(String insertPrefix, List<Row> rows) {
        int size = insertPrefix.length();
        for (Row row : rows) {
            size += row.values.length() + 2;
        }
        StringBuilder sb = new StringBuilder(size);
        sb.append(insertPrefix);
        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(rows.get(i).values);
        }
        return sb.toString();
    }

    /** The INSERT prefix up to and including {@code VALUES}. */
    public static String insertPrefix() {
        return INSERT_PREFIX;
    }

    @VisibleForTesting
    static String renderRow(TLoadInfo info) {
        return renderRow(COLUMN_RENDERERS, info);
    }

    /** Render one VALUES tuple with one value per renderer, in renderer order. */
    static String renderRow(List<Function<TLoadInfo, String>> renderers, TLoadInfo info) {
        StringBuilder sb = new StringBuilder(512);
        sb.append('(');
        boolean first = true;
        for (Function<TLoadInfo, String> renderer : renderers) {
            if (!first) {
                sb.append(", ");
            }
            first = false;
            sb.append(renderer.apply(info));
        }
        sb.append(')');
        return sb.toString();
    }

    static String text(String value) {
        // The loads view renders an unset string in these columns as an empty string.
        return quote(value == null ? "" : value);
    }

    static String nullableText(boolean isSet, String value) {
        if (!isSet || value == null) {
            return "NULL";
        }
        return quote(value);
    }

    static String nullableLong(boolean isSet, long value) {
        return isSet ? Long.toString(value) : "NULL";
    }

    private static String json(String value) {
        if (value == null || value.isEmpty()) {
            return "NULL";
        }
        return "parse_json(" + quoteRaw(value) + ")";
    }

    static String datetime(boolean isSet, long epochMs) {
        if (!isSet || epochMs <= 0) {
            return "NULL";
        }
        return quoteRaw(formatDatetime(epochMs));
    }

    /**
     * Format an epoch-millisecond time as {@code yyyy-MM-dd HH:mm:ss.SSS} in the cluster's global
     * {@code time_zone}. Rows are rendered on whatever thread finishes a load, a user session
     * among them, so the session time zone must not leak in: the backstop statement and the BE
     * that fills the loads view for it run with the global zone, and every row of a load has to
     * carry the same wall-clock values whoever writes it.
     */
    public static String formatDatetime(long epochMs) {
        return DATETIME_FORMAT.withZone(globalTimeZone()).format(Instant.ofEpochMilli(epochMs));
    }

    private static ZoneId globalTimeZone() {
        try {
            String zone = GlobalStateMgr.getCurrentState().getVariableMgr().getDefaultSessionVariable().getTimeZone();
            if (zone != null && !zone.isEmpty()) {
                return ZoneId.of(zone, TimeUtils.TIME_ZONE_ALIAS_MAP);
            }
        } catch (Exception e) {
            LOG.debug("cannot resolve the global time zone, using {}", TimeUtils.DEFAULT_TIME_ZONE, e);
        }
        return ZoneId.of(TimeUtils.DEFAULT_TIME_ZONE);
    }

    /** Quote a value bound for a varchar(2048) column, trimming it to what the column holds. */
    @VisibleForTesting
    static String quote(String value) {
        return quoteRaw(fitVarchar(value));
    }

    /** Render a single-quoted SQL string literal, escaping what the lexer would misread. */
    @VisibleForTesting
    static String quoteRaw(String value) {
        StringBuilder sb = new StringBuilder(value.length() + 2);
        sb.append('\'');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\'':
                    sb.append("\\'");
                    break;
                case '\0':
                    sb.append("\\0");
                    break;
                default:
                    sb.append(c);
            }
        }
        sb.append('\'');
        return sb.toString();
    }

    /** Cut the value to the longest prefix that fits a varchar(2048) column in UTF-8. */
    @VisibleForTesting
    static String fitVarchar(String value) {
        if (value.length() <= MAX_VARCHAR_BYTES / 4) {
            // Every char takes at most four UTF-8 bytes (a surrogate pair four together).
            return value;
        }
        int bytes = 0;
        int end = 0;
        while (end < value.length()) {
            int codePoint = value.codePointAt(end);
            int width = codePoint < 0x80 ? 1 : codePoint < 0x800 ? 2 : codePoint < 0x10000 ? 3 : 4;
            if (bytes + width > MAX_VARCHAR_BYTES) {
                return value.substring(0, end);
            }
            bytes += width;
            end += Character.charCount(codePoint);
        }
        return value;
    }
}
