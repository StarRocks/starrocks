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
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.load.loadv2.LoadJobsTable.NonFinalRow;
import com.starrocks.load.loadv2.LoadJobsTable.PartitionRange;
import com.starrocks.load.loadv2.LoadsHistoryWriter.FlushResult;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.streamload.AbstractStreamLoadTask;
import com.starrocks.load.streamload.StreamLoadMultiStmtTask;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.scheduler.history.TableKeeper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TResultBatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.collections4.ListUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Drains the {@code _statistics_.load_jobs} writer on the leader and periodically reconciles
 * the table with what the load managers hold.
 *
 * <p>A row keeps the last state the FE wrote. A load whose latest write was lost (the FE
 * stopped being leader with the row still queued, the queue overflowed, the write failed
 * for good) would show an outdated, non-final state forever. So every
 * {@code loads_history_sync_interval_second}, starting right after the table keeper reports
 * the schema ready, the daemon reads the rows whose state is not final and compares them
 * with the managers:
 * <ul>
 *   <li>a load a manager still holds, finished or not, whose state differs from the row is
 *   queued again with its current row, which repairs the table from the source of truth;</li>
 *   <li>a load no manager holds any more is marked UNKNOWN once it is past its timeout (a
 *   routine load JOB row has no timeout and is marked as soon as the job is gone).</li>
 * </ul>
 * That only repairs rows the table has. A load whose first row was lost has none (a short load
 * that was still queued when the leader changed, a row dropped from a full queue), and neither
 * has a load that already existed when this table was introduced. So the full pass, and the
 * pass after rows were dropped, also goes the other way: every load the managers hold that has
 * no row gets one.
 * A pass normally runs only when the flush before it left nothing behind, so that it does not
 * judge the table by rows that are still on their way. Most passes only read the partitions of
 * the last few days, where running loads are; once an hour a pass reads every partition, for
 * the loads that run longer, and then expires the table, see {@link LoadJobsTable#planExpiry}.
 */
public class LoadJobsSyncer extends LoadTableWriterDaemon {
    private static final Logger LOG = LogManager.getLogger(LoadJobsSyncer.class);

    private static final TableKeeper KEEPER = LoadJobsTable.createKeeper();

    // How often a pass reads every partition and expires the table.
    static final long FULL_PASS_INTERVAL_MS = 3_600_000L;

    // A pass that keeps being put off because the queue never drains runs anyway once it is
    // this many intervals late.
    static final int OVERDUE_INTERVALS = 10;

    private static final long DAY_MS = 86_400_000L;

    /** How the daemon reads and changes the table; replaced in tests. */
    interface TableAccess {
        /** The rows of a query, each as the text of its cells (null for a NULL cell). */
        List<List<String>> query(String sql);

        /** Run an UPDATE or DELETE. */
        void execute(String sql);

        /** Run an ALTER TABLE. */
        void executeDdl(String sql);

        /** The day partitions of the table. */
        List<PartitionRange> partitions();
    }

    /**
     * Stands for a load that a manager holds but whose row could not be read. Such a load is
     * neither repaired nor marked UNKNOWN in the pass that sees it.
     */
    static final TLoadInfo UNREADABLE = new TLoadInfo();

    /** Most loads one pass checks for a missing row. */
    static final int MISSING_ROWS_MAX_LOADS = 100000;

    private final Function<Collection<Long>, Map<Long, TLoadInfo>> loadLookup;
    // The current row of every load the managers hold, by id.
    private final Supplier<Map<Long, TLoadInfo>> heldLoads;
    private final TableAccess access;
    private LongSupplier clock = System::currentTimeMillis;

    // Leader-session state, reset in resetSession().
    private long sessionStartMs = 0L;
    private long lastReconcileAttemptMs = 0L;
    private long lastFullPassMs = 0L;
    // The writer's dropped total when loads without a row were last looked for.
    private long droppedAtLastMissingPass = 0L;

    public LoadJobsSyncer() {
        this(new LoadsHistoryWriter(LoadJobsTable.TARGET, () -> Config.loads_history_queue_max_rows,
                        () -> Config.loads_history_queue_max_bytes, LoadJobsSyncer::writeBatch,
                        LoadsHistorySyncer::hasAliveNode),
                LoadJobsSyncer::lookupLoads, LoadJobsSyncer::heldLoads, new RepoTableAccess(), KEEPER::isSchemaReady);
    }

    @VisibleForTesting
    LoadJobsSyncer(LoadsHistoryWriter writer, Function<Collection<Long>, Map<Long, TLoadInfo>> loadLookup,
                   TableAccess access, BooleanSupplier schemaReady) {
        this(writer, loadLookup, Collections::emptyMap, access, schemaReady);
    }

    @VisibleForTesting
    LoadJobsSyncer(LoadsHistoryWriter writer, Function<Collection<Long>, Map<Long, TLoadInfo>> loadLookup,
                   Supplier<Map<Long, TLoadInfo>> heldLoads, TableAccess access, BooleanSupplier schemaReady) {
        super("load-jobs-syncer", writer, schemaReady);
        this.loadLookup = loadLookup;
        this.heldLoads = heldLoads;
        this.access = access;
    }

    @VisibleForTesting
    void setClock(LongSupplier clock) {
        this.clock = clock;
    }

    private static void writeBatch(String sql, int rows) {
        // The statement carries every column of every row; the audit log gets a summary only.
        SimpleExecutor.getRepoExecutor().executeDML(sql, LoadJobsTable.INSERT_PREFIX + "... (" + rows + " rows)");
    }

    @Override
    protected void activateWriter() {
        LoadJobsTable.activate(writer);
    }

    @Override
    protected void deactivateWriter() {
        LoadJobsTable.deactivate(writer);
    }

    @Override
    protected void afterFlush(FlushResult result) {
        long now = clock.getAsLong();
        if (sessionStartMs == 0L) {
            sessionStartMs = now;
        }
        // At least a second between passes, whatever the interval is set to.
        long intervalMs = Math.max(1, Config.loads_history_sync_interval_second) * 1000L;
        // After a failed flush the table cannot be written, and the rows that are still queued
        // may be exactly the states it lacks.
        if (result == FlushResult.FAILED) {
            return;
        }
        // The same holds after a flush that left rows behind: reconciling now would repair, or
        // mark UNKNOWN, what the next flush delivers anyway. Under a load that never lets the
        // queue drain the pass still has to run at some point; what it then gets wrong about a
        // queued row is corrected when that row is written.
        if (result == FlushResult.BACKLOG
                && now - Math.max(sessionStartMs, lastReconcileAttemptMs) < OVERDUE_INTERVALS * intervalMs) {
            return;
        }
        if (lastReconcileAttemptMs == 0L || now - lastReconcileAttemptMs >= intervalMs) {
            reconcile(now);
        }
    }

    /** The span of create_time an ordinary pass reads: the longest default timeout plus a day. */
    private static long recentSpanMs() {
        long timeoutSecond = Math.max(Config.max_load_timeout_second,
                Math.max(Config.stream_load_default_timeout_second, Config.routine_load_task_timeout_second));
        return Math.max(0L, timeoutSecond) * 1000L + DAY_MS;
    }

    private void reconcile(long now) {
        lastReconcileAttemptMs = now;
        boolean fullPass = lastFullPassMs == 0L || now - lastFullPassMs >= FULL_PASS_INTERVAL_MS;
        String nowText = LoadsHistoryWriter.formatDatetime(now);
        String createdSince = fullPass ? null : LoadJobsTable.keyDay(now - recentSpanMs());
        int repaired = 0;
        List<Long> untracked = new ArrayList<>();
        try {
            List<NonFinalRow> rows = LoadJobsTable.toNonFinalRows(access.query(LoadJobsTable.buildNonFinalRowsSql(
                    createdSince, nowText, Config.max_load_timeout_second, Config.stream_load_default_timeout_second,
                    Config.routine_load_task_timeout_second)));
            if (!rows.isEmpty()) {
                Map<Long, TLoadInfo> held = loadLookup.apply(
                        rows.stream().map(row -> row.id).collect(Collectors.toCollection(HashSet::new)));
                for (NonFinalRow row : rows) {
                    TLoadInfo current = held.get(row.id);
                    if (current == UNREADABLE) {
                        // A manager holds the load but its row could not be read: say nothing
                        // about it in this pass, it is not a load that went away.
                        continue;
                    }
                    if (current == null) {
                        if (row.pastTimeout) {
                            untracked.add(row.id);
                        }
                    } else if (!row.state.equals(current.getState())) {
                        // The table missed a state change of a load that is still known here.
                        writer.enqueue(current);
                        repaired++;
                    }
                }
            }
            for (String sql : LoadJobsTable.buildMarkUnknownSql(untracked, LoadJobsTable.RECONCILE_IDS_PER_STATEMENT,
                    createdSince, nowText)) {
                access.execute(sql);
            }
        } catch (Exception e) {
            // Try again after the interval.
            LOG.warn("failed to reconcile {} with the load managers", LoadJobsTable.FULL_NAME, e);
            return;
        }
        if (repaired > 0 || !untracked.isEmpty()) {
            LOG.info("reconciled {}: {} rows queued again with the current state of their load, {} rows of loads"
                    + " that are no longer tracked marked as UNKNOWN", LoadJobsTable.FULL_NAME, repaired, untracked.size());
        }
        long dropped = writer.droppedTotal();
        if (fullPass || dropped != droppedAtLastMissingPass) {
            if (addMissingRows(now)) {
                droppedAtLastMissingPass = dropped;
            } else {
                // The full pass counts as done only when this part of it did.
                return;
            }
        }
        if (fullPass) {
            lastFullPassMs = now;
            expire(now);
        }
    }

    /**
     * Queue the current row of every load the managers hold that has no row in the table.
     * Loads that finished longer ago than the retention are left out: their row has expired,
     * or would with the next expiry. Returns false if the table could not be read.
     */
    private boolean addMissingRows(long now) {
        try {
            long finishCutoffMs = now - Math.max(1, Config.loads_history_retained_days) * DAY_MS;
            Map<Long, TLoadInfo> candidates = new HashMap<>();
            for (Map.Entry<Long, TLoadInfo> entry : heldLoads.get().entrySet()) {
                TLoadInfo row = entry.getValue();
                if (row == null || row == UNREADABLE || !writer.getTarget().accepts(row)) {
                    continue;
                }
                boolean expired = LoadJobsTable.FINAL_STATES.contains(row.getState())
                        && (row.isSetLoad_finish_time_ms() ? row.getLoad_finish_time_ms() : row.getCreate_time_ms())
                        < finishCutoffMs;
                if (!expired) {
                    candidates.put(entry.getKey(), row);
                }
                if (candidates.size() >= MISSING_ROWS_MAX_LOADS) {
                    break;
                }
            }
            if (candidates.isEmpty()) {
                return true;
            }
            Set<Long> existing = new HashSet<>();
            for (String sql : LoadJobsTable.buildExistingIdsSql(candidates.keySet(),
                    LoadJobsTable.RECONCILE_IDS_PER_STATEMENT)) {
                for (List<String> row : access.query(sql)) {
                    if (!row.isEmpty() && row.get(0) != null) {
                        existing.add(Long.parseLong(row.get(0)));
                    }
                }
            }
            int added = 0;
            for (Map.Entry<Long, TLoadInfo> entry : candidates.entrySet()) {
                if (!existing.contains(entry.getKey())) {
                    writer.enqueue(entry.getValue());
                    added++;
                }
            }
            if (added > 0) {
                LOG.info("reconciled {}: {} loads that had no row were queued with their current state",
                        LoadJobsTable.FULL_NAME, added);
            }
            return true;
        } catch (Exception e) {
            LOG.warn("failed to look for loads without a row in {}", LoadJobsTable.FULL_NAME, e);
            return false;
        }
    }

    /** Drop or purge the partitions that are past the retention, see {@link LoadJobsTable#planExpiry}. */
    private void expire(long now) {
        try {
            long cutoffMs = now - Math.max(1, Config.loads_history_retained_days) * DAY_MS;
            // The partition bounds are days in UTC; the finish times are in the zone rows are written in.
            String partitionCutoff = LoadJobsTable.utcDatetime(cutoffMs);
            String finishCutoff = LoadsHistoryWriter.formatDatetime(cutoffMs);
            List<PartitionRange> expired = access.partitions().stream()
                    .filter(partition -> partition.upper.compareTo(partitionCutoff) <= 0)
                    .collect(Collectors.toList());
            if (expired.isEmpty()) {
                return;
            }
            List<String> liveDays = access.query(LoadJobsTable.buildLiveDaysSql(partitionCutoff, finishCutoff)).stream()
                    .filter(row -> !row.isEmpty() && row.get(0) != null)
                    .map(row -> row.get(0))
                    .collect(Collectors.toList());
            for (String sql : LoadJobsTable.planExpiry(expired, liveDays, partitionCutoff, finishCutoff)) {
                try {
                    if (sql.startsWith("ALTER")) {
                        access.executeDdl(sql);
                    } else {
                        access.execute(sql);
                    }
                } catch (Exception e) {
                    // The other partitions do not depend on this one; it is tried again with the next full pass.
                    LOG.warn("failed to expire {} with: {}", LoadJobsTable.FULL_NAME, sql, e);
                }
            }
        } catch (Exception e) {
            LOG.warn("failed to expire {}", LoadJobsTable.FULL_NAME, e);
        }
    }

    /**
     * The current row of every load among {@code ids} that a manager still holds, finished or
     * not, by id. Sub-tasks of a multi-statement stream load are looked up by their own id,
     * which is the id of their row.
     */
    @VisibleForTesting
    static Map<Long, TLoadInfo> lookupLoads(Collection<Long> ids) {
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        Set<Long> wanted = ids instanceof Set ? (Set<Long>) ids : new HashSet<>(ids);
        Map<Long, TLoadInfo> held = new HashMap<>();
        for (Long id : wanted) {
            try {
                LoadJob job = state.getLoadMgr().getLoadJob(id);
                if (job != null) {
                    held.put(id, UNREADABLE);
                    held.put(id, job.toLoadJobsRow());
                    continue;
                }
                RoutineLoadJob routineLoadJob = state.getRoutineLoadMgr().getJob(id);
                if (routineLoadJob != null) {
                    held.put(id, UNREADABLE);
                    held.put(id, routineLoadJob.toLoadJobRow());
                }
            } catch (Exception e) {
                // A manager holds the load, or the lookup itself failed: either way it is not
                // known to be gone.
                held.putIfAbsent(id, UNREADABLE);
                LOG.warn("failed to read the current row of load {}", id, e);
            }
        }
        if (held.size() == wanted.size()) {
            return held;
        }
        // Stream load, routine load and merge commit tasks.
        for (AbstractStreamLoadTask task : state.getStreamLoadMgr().getAllTasks()) {
            boolean match = task instanceof StreamLoadMultiStmtTask
                    ? ((StreamLoadMultiStmtTask) task).getSubTaskIds().stream().anyMatch(wanted::contains)
                    : wanted.contains(task.getId());
            if (!match) {
                continue;
            }
            try {
                for (TLoadInfo row : task.toThrift()) {
                    if (wanted.contains(row.getJob_id())) {
                        held.putIfAbsent(row.getJob_id(), row);
                    }
                }
            } catch (Exception e) {
                // The task is held: none of its rows may be taken for a load that went away.
                if (task instanceof StreamLoadMultiStmtTask) {
                    ((StreamLoadMultiStmtTask) task).getSubTaskIds().stream().filter(wanted::contains)
                            .forEach(id -> held.putIfAbsent(id, UNREADABLE));
                } else {
                    held.putIfAbsent(task.getId(), UNREADABLE);
                }
                LOG.warn("failed to read the current row of load task {}", task.getId(), e);
            }
        }
        return held;
    }

    /** The current row of every load the managers hold, finished or not, by id. */
    @VisibleForTesting
    static Map<Long, TLoadInfo> heldLoads() {
        GlobalStateMgr state = GlobalStateMgr.getCurrentState();
        Map<Long, TLoadInfo> held = new HashMap<>();
        for (LoadJob job : state.getLoadMgr().getAllLoadJobs()) {
            try {
                held.put(job.getId(), job.toLoadJobsRow());
            } catch (Exception e) {
                LOG.debug("failed to read the current row of load {}", job.getId(), e);
            }
        }
        for (RoutineLoadJob job : state.getRoutineLoadMgr().getAllJobs()) {
            try {
                held.put(job.getId(), job.toLoadJobRow());
            } catch (Exception e) {
                LOG.debug("failed to read the current row of routine load job {}", job.getId(), e);
            }
        }
        for (AbstractStreamLoadTask task : state.getStreamLoadMgr().getAllTasks()) {
            try {
                for (TLoadInfo row : task.toThrift()) {
                    held.putIfAbsent(row.getJob_id(), row);
                }
            } catch (Exception e) {
                LOG.debug("failed to read the current row of load task {}", task.getId(), e);
            }
        }
        return held;
    }

    /** Pending rows are discarded by the base class: the first pass of the next leader reads
     * every non-final row back and repairs it from the managers. */
    @Override
    protected void resetSession() {
        sessionStartMs = 0L;
        lastReconcileAttemptMs = 0L;
        lastFullPassMs = 0L;
        droppedAtLastMissingPass = 0L;
    }

    /** Reads and changes the table through the internal executor and the catalog. */
    private static final class RepoTableAccess implements TableAccess {
        private static final DateTimeFormatter BOUND_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        @Override
        public List<List<String>> query(String sql) {
            return decode(SimpleExecutor.getRepoExecutor().executeDQL(sql));
        }

        @Override
        public void execute(String sql) {
            SimpleExecutor.getRepoExecutor().executeDML(sql);
        }

        @Override
        public void executeDdl(String sql) {
            SimpleExecutor.getRepoExecutor().executeDDL(sql);
        }

        @Override
        public List<PartitionRange> partitions() {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(LoadJobsTable.DB_NAME);
            Optional<Table> table = GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .mayGetTable(LoadJobsTable.DB_NAME, LoadJobsTable.TABLE_NAME);
            if (db == null || table.isEmpty() || !(table.get() instanceof OlapTable)) {
                return Collections.emptyList();
            }
            OlapTable olapTable = (OlapTable) table.get();
            List<PartitionRange> result = new ArrayList<>();
            try (AutoCloseableLock ignored = new AutoCloseableLock(db.getId(), olapTable.getId(), LockType.READ)) {
                if (!(olapTable.getPartitionInfo() instanceof RangePartitionInfo)) {
                    return result;
                }
                RangePartitionInfo partitionInfo = (RangePartitionInfo) olapTable.getPartitionInfo();
                List<Column> partitionColumns = partitionInfo.getPartitionColumns(olapTable.getIdToColumn());
                // An expression partitioned table always has a shadow partition, which is not a day.
                PartitionKey shadowKey = PartitionKey.createShadowPartitionKey(partitionColumns);
                partitionInfo.getIdToRange(false).forEach((partitionId, range) -> {
                    Partition partition = olapTable.getPartition(partitionId);
                    if (partition == null || range.lowerEndpoint().compareTo(shadowKey) == 0) {
                        return;
                    }
                    String lower = boundText(range.lowerEndpoint());
                    String upper = boundText(range.upperEndpoint());
                    if (lower != null && upper != null) {
                        result.add(new PartitionRange(partition.getName(), lower, upper));
                    }
                });
            } catch (Exception e) {
                LOG.warn("failed to list the partitions of {}", LoadJobsTable.FULL_NAME, e);
                return Collections.emptyList();
            }
            return result;
        }

        // A bound that is not a plain date (the open ends of the range) yields null and is skipped.
        private static String boundText(PartitionKey key) {
            try {
                LiteralExpr bound = key.getKeys().get(0);
                if (bound instanceof DateLiteral && !bound.isMinValue()) {
                    return BOUND_FORMAT.format(((DateLiteral) bound).toLocalDateTime());
                }
            } catch (Exception e) {
                // Not a date the table could hold rows for.
            }
            return null;
        }
    }

    /** The cells of the rows the internal executor returns: one JSON {@code {"data": [...]}} per row. */
    @VisibleForTesting
    static List<List<String>> decode(List<TResultBatch> batches) {
        List<List<String>> rows = new ArrayList<>();
        for (TResultBatch batch : ListUtils.emptyIfNull(batches)) {
            for (ByteBuffer buffer : batch.getRows()) {
                ByteBuf copied = Unpooled.copiedBuffer(buffer);
                JsonArray data = JsonParser.parseString(copied.toString(StandardCharsets.UTF_8))
                        .getAsJsonObject().get("data").getAsJsonArray();
                List<String> cells = new ArrayList<>(data.size());
                for (JsonElement cell : data) {
                    cells.add(cell.isJsonNull() ? null : cell.getAsString());
                }
                rows.add(cells);
            }
        }
        return rows;
    }
}
