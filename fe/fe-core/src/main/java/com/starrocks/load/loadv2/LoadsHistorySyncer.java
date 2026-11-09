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
import com.starrocks.catalog.CatalogUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.system.information.LoadsSystemTable;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.load.loadv2.LoadsHistoryWriter.FlushResult;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * Keeps {@code _statistics_.loads_history} filled with the finished rows of
 * {@code information_schema.loads}. The archive table's columns are derived from the
 * view's schema, so the table definition and every INSERT name each column explicitly.
 *
 * <p>Rows are written as loads finish: the finish and cancel paths of every load type hand
 * their view row to a {@link LoadsHistoryWriter}, and this daemon's thread drains that writer
 * in batches. The former {@code INSERT ... SELECT} from the view stays as a backstop that runs
 * when this FE becomes leader and whenever the writer left a load to it, copying only loads
 * of the recent past that the table does not have yet. A load is only copied once it finished
 * more than a minute ago, so the backstop runs once the first load that was left to it is that
 * old, and again for the loads left to it in the meantime. After a leader change it reaches
 * back {@code loads_history_backstop_lookback_minutes}; loads that finished earlier and were
 * never written are not recovered.
 */
public class LoadsHistorySyncer extends LeaderDaemon {
    private static final Logger LOG = LogManager.getLogger(LoadsHistorySyncer.class);

    public static final String LOADS_HISTORY_DB_NAME = "_statistics_";
    public static final String LOADS_HISTORY_TABLE_NAME = "loads_history";

    // Tables in LOADS_HISTORY_DB_NAME that are filled by internal load-history writers. Loads
    // into them are the writers' own flushes and are neither queued nor copied by the backstop.
    public static final List<String> INTERNAL_LOAD_TABLES = ImmutableList.of(LOADS_HISTORY_TABLE_NAME, "load_jobs");

    // Partition column; the only NOT NULL column of the archive.
    private static final String LOAD_FINISH_TIME = "load_finish_time";

    // (column name, column type DDL) for every column of the archive, in view order.
    // TableKeeper adds whichever of these an existing table lacks, and nothing is written
    // until none is missing.
    //
    // rejected_record_path is kept although BE no longer populates it; rejected rows
    // live in `_statistics_.rejected_records`, joinable to this table via load label
    // or txn_id.
    static final List<Pair<String, String>> HISTORY_COLUMNS = LoadsSystemTable.columns().stream()
            .map(column -> Pair.create(column.getName().toLowerCase(), historyColumnType(column)))
            .collect(ImmutableList.toImmutableList());

    private static String historyColumnType(Column column) {
        Type type = column.getType();
        if (type.isBigint()) {
            return "bigint";
        }
        if (type.isVarchar()) {
            return "varchar(2048)";
        }
        if (type.isJsonType()) {
            return "json";
        }
        if (type.isDatetime()) {
            return "datetime";
        }
        throw new IllegalStateException("unsupported loads column type " + type + " for " + column.getName());
    }

    private static String columnDefinitions() {
        return HISTORY_COLUMNS.stream()
                .map(column -> "`" + column.first + "` " + column.second
                        + (LOAD_FINISH_TIME.equals(column.first) ? " not null" : ""))
                .collect(Collectors.joining(", "));
    }

    /** The archive's columns as a back-quoted, comma separated list in view order. */
    static String columnList() {
        return HISTORY_COLUMNS.stream()
                .map(column -> "`" + column.first + "`")
                .collect(Collectors.joining(", "));
    }

    private static final String LOADS_HISTORY_TABLE_CREATE =
            String.format("CREATE TABLE IF NOT EXISTS %s (%s) " +
            "PARTITION BY date_trunc('DAY', load_finish_time) " +
            "DISTRIBUTED BY HASH(label) BUCKETS 3 " +
            "PROPERTIES( " +
            "'replication_num' = '1', " +
            "'partition_live_number' = '" + Config.loads_history_retained_days + "'" +
            ")",
            LOADS_HISTORY_TABLE_NAME, columnDefinitions());

    // Backstop: copy loads of the recent past that the writer did not deliver. Only loads that
    // finished more than a minute ago are considered, so that their row has settled, and only
    // loads that finished before every row still in the writer's queue; rows the table already
    // holds are skipped by id, and the writer's own INSERTs into the table are left out as the
    // writer leaves them out. So are the loads whose row the writer still holds or has queued
    // long after they finished (%s: empty, or "AND l.id NOT IN (...) ").
    private static final String LOADS_HISTORY_SYNC =
            "INSERT INTO %s (%s) " +
            "SELECT %s FROM information_schema.loads l " +
            "WHERE l.load_finish_time IS NOT NULL " +
            "AND l.load_finish_time < NOW() - INTERVAL %d SECOND " +
            "AND l.load_finish_time > NOW() - INTERVAL %d MINUTE " +
            "AND NOT (l.db_name = '" + LOADS_HISTORY_DB_NAME + "' AND l.table_name IN (" + internalTableList() + ")) " +
            "%s" +
            "AND NOT EXISTS (SELECT 1 FROM %s h WHERE h.id = l.id);";

    /**
     * Runs the backstop statement for the loads that finished between {@code lookbackMinutes}
     * and {@code settleSeconds} ago, leaving out the loads with the given ids.
     */
    @FunctionalInterface
    interface Backstop {
        void run(List<Long> excludedIds, long settleSeconds, long lookbackMinutes);
    }

    private static String internalTableList() {
        return INTERNAL_LOAD_TABLES.stream().map(table -> "'" + table + "'").collect(Collectors.joining(", "));
    }

    // Slack between the FE clock and the NOW() of the backstop statement.
    static final long COVER_MARGIN_MS = 5_000L;

    private static final TableKeeper KEEPER =
            new TableKeeper(LOADS_HISTORY_DB_NAME, LOADS_HISTORY_TABLE_NAME, LOADS_HISTORY_TABLE_CREATE,
                    () -> Math.max(1, Config.loads_history_retained_days), HISTORY_COLUMNS);

    private final LoadsHistoryWriter writer;
    private final Backstop backstop;
    private final BooleanSupplier schemaReady;

    private LongSupplier clock = System::currentTimeMillis;

    // Leader-session state, reset in onStopped().
    // When this FE started to drain the writer as leader. Loads that finished before that may
    // sit in no queue: the previous leader's queue is gone, and nothing is queued before the
    // writer is active.
    private long sessionStartMs = 0L;
    // Whether the backstop ran once in this session, right away, for what an earlier leader missed.
    private boolean startupBackstopDone = false;
    // A successful backstop saw every load that finished up to this time.
    private long coveredUntilMs = 0L;
    private long lastBackstopAttemptMs = 0L;
    private boolean schemaNotReadyLogged = false;

    public static TableKeeper createKeeper() {
        return KEEPER;
    }

    public LoadsHistorySyncer() {
        this(new LoadsHistoryWriter(() -> Config.loads_history_queue_max_rows,
                        () -> Config.loads_history_queue_max_bytes, LoadsHistorySyncer::writeBatch,
                        LoadsHistorySyncer::hasAliveNode),
                (Backstop) LoadsHistorySyncer::syncData, KEEPER::isSchemaReady);
    }

    /** Whether any backend or compute node is alive, that is, whether an INSERT can run at all. */
    static boolean hasAliveNode() {
        SystemInfoService nodes = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        return nodes.getAliveBackendNumber() > 0 || nodes.getAliveComputeNodeNumber() > 0;
    }

    @VisibleForTesting
    void setClock(LongSupplier clock) {
        this.clock = clock;
    }

    @VisibleForTesting
    LoadsHistorySyncer(LoadsHistoryWriter writer, Runnable backstop, BooleanSupplier schemaReady) {
        this(writer, (excludedIds, settleSeconds, lookbackMinutes) -> backstop.run(), schemaReady);
    }

    @VisibleForTesting
    LoadsHistorySyncer(LoadsHistoryWriter writer, Backstop backstop, BooleanSupplier schemaReady) {
        // No inter-cycle delay: each cycle paces itself on the writer's bounded wait for a batch.
        super("load-history-syncer", 0L);
        this.writer = writer;
        this.backstop = backstop;
        this.schemaReady = schemaReady;
    }

    @VisibleForTesting
    LoadsHistoryWriter getWriter() {
        return writer;
    }

    private static void writeBatch(String sql, int rows) {
        // The statement carries every column of every row; the audit log gets a summary only.
        SimpleExecutor.getRepoExecutor().executeDML(sql,
                LoadsHistoryWriter.insertPrefix() + "... (" + rows + " rows)");
    }

    /** Run the backstop statement once. */
    public static void syncData(List<Long> excludedIds, long settleSeconds, long lookbackMinutes) {
        try {
            SimpleExecutor.getRepoExecutor().executeDML(
                    SQLBuilder.buildSyncSql(excludedIds, settleSeconds, lookbackMinutes));
        } catch (Exception e) {
            LOG.error("Failed to sync loads history", e);
            throw e;
        }
    }

    @Override
    protected void runAfterLeaseValid() throws InterruptedException {
        long flushIntervalMs = Math.max(1, Config.loads_history_flush_interval_second) * 1000L;
        if (FeConstants.runningUnitTest) {
            // Unit tests have no executor to write to.
            Thread.sleep(flushIntervalMs);
            return;
        }
        // Rows are accepted as soon as this FE leads; they wait in the writer until the table
        // keeper has created the table and appended the columns a newer version added, since
        // the explicit column lists would not resolve before that.
        LoadsHistoryWriter.activate(writer);
        if (!schemaReady.getAsBoolean()) {
            if (!schemaNotReadyLogged) {
                LOG.info("loads history table is missing or lacks columns, waiting for the table keeper");
                schemaNotReadyLogged = true;
            }
            Thread.sleep(flushIntervalMs);
            return;
        }
        schemaNotReadyLogged = false;
        try {
            runCycle(flushIntervalMs);
        } catch (InterruptedException e) {
            throw e;
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of LoadsHistorySyncer with error message {}", e.getMessage(), e);
        }
    }

    /** One cycle: flush pending rows, then run the backstop if it is wanted and safe. */
    @VisibleForTesting
    void runCycle(long flushIntervalMs) throws InterruptedException {
        if (sessionStartMs == 0L) {
            sessionStartMs = clock.getAsLong();
        }
        // Rows held until their load is visible are queued here, by the thread that runs the
        // backstop, so that none of them arrives while a backstop statement is running.
        writer.releaseHolds();
        FlushResult result = writer.runOnce(Config.loads_history_flush_batch_rows, flushIntervalMs);
        // A failed flush means the table cannot be written right now. Rows still in the queue do
        // not keep the backstop off, or a queue that never drains would starve it: the backstop
        // stays clear of them, see settleMs() and LoadsHistoryWriter.beginBackstop().
        if (result == FlushResult.FAILED) {
            return;
        }
        long now = clock.getAsLong();
        if (isBackstopDue(now) && isBackstopWanted(now, settleMs(now, writer.oldestQueuedFinishMs()))) {
            runBackstop(now);
        }
    }

    // How long ago a load must have finished for the backstop to copy it: the settle window, or
    // longer when rows wait in the queue. Those arrived at most FRESH_ROW_MS after their load
    // finished, so by copying only loads that finished before the oldest of them the backstop
    // cannot copy a load whose row a later flush writes again.
    private static long settleMs(long now, long oldestQueuedFinishMs) {
        if (oldestQueuedFinishMs == Long.MAX_VALUE) {
            return LoadsHistoryWriter.SETTLE_MS;
        }
        return Math.max(LoadsHistoryWriter.SETTLE_MS, now - oldestQueuedFinishMs + COVER_MARGIN_MS);
    }

    private boolean isBackstopWanted(long now, long settleMs) {
        if (!startupBackstopDone) {
            return true;
        }
        // The earliest moment a load was left to the backstop and no run has covered since:
        // the start of this leader session, or the first row the writer dropped or did not
        // queue. Rows left to the backstop later do not move it, so a steady trickle of them
        // cannot postpone the run; they are picked up by the run after.
        long leftToBackstopMs = writer.oldestBackstopRequestMs();
        if (sessionStartMs > coveredUntilMs && (leftToBackstopMs == 0L || sessionStartMs < leftToBackstopMs)) {
            leftToBackstopMs = sessionStartMs;
        }
        if (leftToBackstopMs == 0L) {
            return false;
        }
        // A run before that moment has passed the settle window would not see the load; the
        // backstop stays pending until then instead of being cleared by an earlier run.
        return now >= leftToBackstopMs + settleMs + COVER_MARGIN_MS;
    }

    private boolean isBackstopDue(long now) {
        // At least a second between attempts, whatever the interval is set to.
        long intervalMs = Math.max(1, Config.loads_history_sync_interval_second) * 1000L;
        return lastBackstopAttemptMs == 0L || now - lastBackstopAttemptMs >= intervalMs;
    }

    private void runBackstop(long now) {
        lastBackstopAttemptMs = now;
        long settleMs;
        try {
            LoadsHistoryWriter.BackstopScope scope = writer.beginBackstop();
            settleMs = settleMs(now, scope.oldestQueuedFinishMs);
            // Reach back to the oldest load that was left to the backstop when that is further
            // than the configured look-back, for example a row dropped after the table could not
            // be written for a long time. The view only holds what the load managers still keep.
            long lookbackMinutes = backstopLookbackMinutes();
            if (scope.oldestLostFinishMs > 0) {
                lookbackMinutes = Math.max(lookbackMinutes, (now - scope.oldestLostFinishMs) / 60_000L + 2);
            }
            backstop.run(scope.excludedIds, (settleMs + 999L) / 1000L, lookbackMinutes);
        } catch (Exception e) {
            // Logged by syncData; stay pending and try again after the interval.
            return;
        } finally {
            writer.endBackstop();
        }
        startupBackstopDone = true;
        coveredUntilMs = Math.max(coveredUntilMs, now - settleMs - COVER_MARGIN_MS);
        writer.backstopCovered(coveredUntilMs);
    }

    @Override
    protected void onStopped() {
        // Drop leader-session state: pending rows are discarded (the next leader's startup
        // backstop re-reads them from information_schema.loads) and the backstop re-arms.
        LoadsHistoryWriter.deactivate(writer);
        writer.clear();
        sessionStartMs = 0L;
        startupBackstopDone = false;
        coveredUntilMs = 0L;
        lastBackstopAttemptMs = 0L;
        schemaNotReadyLogged = false;
    }

    /**
     * Generate SQL for operations
     */
    static class SQLBuilder {
        public static String buildSyncSql() {
            return buildSyncSql(Collections.emptyList());
        }

        public static String buildSyncSql(List<Long> excludedIds) {
            return buildSyncSql(excludedIds, LoadsHistoryWriter.SETTLE_MS / 1000L, backstopLookbackMinutes());
        }

        public static String buildSyncSql(List<Long> excludedIds, long settleSeconds, long lookbackMinutes) {
            String table = CatalogUtils.normalizeTableName(LOADS_HISTORY_DB_NAME, LOADS_HISTORY_TABLE_NAME);
            String excluded = excludedIds.isEmpty() ? "" : "AND l.id NOT IN ("
                    + excludedIds.stream().map(String::valueOf).collect(Collectors.joining(", ")) + ") ";
            return String.format(LOADS_HISTORY_SYNC, table, columnList(), columnList(), settleSeconds, lookbackMinutes,
                    excluded, table);
        }
    }

    /** How far back the backstop looks. It must reach past the settle window, otherwise nothing qualifies. */
    static int backstopLookbackMinutes() {
        return Math.max(2, Config.loads_history_backstop_lookback_minutes);
    }
}
