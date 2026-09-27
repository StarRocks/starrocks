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

package com.starrocks.alter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.scheduler.Task;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.InsertStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;

/**
 * Online sort-key rewrite for a shared-data range-distribution async materialized view. A REPLACE flip
 * mechanically identical to {@link LakeRangeRewriteSchemaChangeJob} (the MV's single index IS its base,
 * so the base-meta re-point carries the new sort key), specialized for an MV target:
 *   - the rewrite INSERT targets a materialized view, so it must be a system INSERT
 *     ({@link #configureRewriteInsert});
 *   - a sort-key reorder is output-preserving, so it must NOT inactivate dependent MVs
 *     ({@link #affectedColumnsForMvInactivation} returns empty);
 *   - the flip also updates {@code TableProperty.mvSortKeys} so SHOW CREATE and query-rewrite reflect
 *     the new order ({@link #visualiseShadowIndex});
 *   - the MV's async refresh is suspended for the rewrite window and resumed on settle
 *     (wired via {@code beforeShadowBuild}/{@code afterJobSettled}, through the {@link RefreshCoordinator}
 *     seam), and reconciled to the same invariant on journal replay ({@link #onReplayReconcile}).
 */
public class LakeMvSortKeyRewriteJob extends LakeRangeRewriteSchemaChangeJob {
    private static final Logger LOG = LogManager.getLogger(LakeMvSortKeyRewriteJob.class);

    // Whether the MV's refresh Task was ACTIVE (not PAUSE) at submission. The job suspends refresh for
    // the window and resumes it on settle — but ONLY if it was active at submit, so a user who had
    // manually paused refresh (ALTER TASK ... SUSPEND) or an INACTIVE MV is not silently resurrected.
    // Serialized so both the live settle and the terminal-replay reconcile honor it.
    @SerializedName(value = "refreshWasActiveAtSubmit")
    private boolean refreshWasActiveAtSubmit;

    public LakeMvSortKeyRewriteJob() {
        super();
    }

    public LakeMvSortKeyRewriteJob(long jobId, long dbId, long tableId, String tableName, long timeoutMs) {
        super(jobId, dbId, tableId, tableName, timeoutMs);
    }

    protected LakeMvSortKeyRewriteJob(LakeMvSortKeyRewriteJob other) {
        super(other);
        this.refreshWasActiveAtSubmit = other.refreshWasActiveAtSubmit;
    }

    public void setRefreshWasActiveAtSubmit(boolean v) {
        this.refreshWasActiveAtSubmit = v;
    }

    @VisibleForTesting
    public boolean isRefreshWasActiveAtSubmit() {
        return refreshWasActiveAtSubmit;
    }

    /** Seam over refresh quiescing so tests need no live TaskManager / InsertOverwriteJobMgr. */
    interface RefreshCoordinator {
        void suspendRefresh();             // LIVE: suspendTask(task, false) -- journals; pause + kill run
        boolean hasRunningOverwriteJob();  // InsertOverwriteJobMgr.hasRunningOverwriteJob(mvId)
        boolean hasTempPartitions();       // mv.existTempPartitions() -- post-drain backstop
        void resumeRefresh();              // LIVE: resumeTask(task, false) -- journals
        void suspendRefreshForReplay();    // REPLAY: suspendTask(task, true) -- no journal, no drain
        void resumeRefreshForReplay();     // REPLAY: resumeTask(task, true) -- no journal
    }

    // Coordinator seam + drain timeout. Not serialized: re-resolved (coordinator) / reset to the default
    // (timeout) after a leader failover, exactly like LakeOnlineRewriteJobBase's own transient executor seams.
    private transient RefreshCoordinator refreshCoordinator;
    private transient long drainTimeoutMs = 60_000L;

    @VisibleForTesting
    public void setRefreshCoordinator(RefreshCoordinator coordinator) {
        this.refreshCoordinator = coordinator;
    }

    @VisibleForTesting
    public void setDrainTimeoutMs(long drainTimeoutMs) {
        this.drainTimeoutMs = drainTimeoutMs;
    }

    private RefreshCoordinator getRefreshCoordinator() {
        if (refreshCoordinator == null) {
            refreshCoordinator = new DefaultRefreshCoordinator(dbId, tableId);
        }
        return refreshCoordinator;
    }

    @Override
    protected void beforeShadowBuild() throws AlterCancelException {
        RefreshCoordinator coord = getRefreshCoordinator();
        coord.suspendRefresh();  // idempotent; pauses scheduling + kills the running refresh run
        // Drain any in-flight InsertOverwriteJob: killing the TaskRun thread does not roll back an
        // overwrite already past its temp-load commit into doCommit (the partition SWAP is a separate
        // metadata op that would drop the partition we are about to pin/build the shadow on). Drain BOTH
        // the running overwrite job AND any lingering temp partitions (mirrors the existing overwrite/
        // temp-partition guard in SchemaChangeHandler#processAlterTable): a temp partition present means
        // an overwrite swap has not fully cleaned up, and proceeding could drop/duplicate a partition.
        long deadline = System.currentTimeMillis() + drainTimeoutMs;
        while (coord.hasRunningOverwriteJob() || coord.hasTempPartitions()) {
            if (System.currentTimeMillis() > deadline) {
                throw new AlterCancelException("timed out draining in-flight refresh/insert-overwrite (or temp "
                        + "partitions) for mv " + tableName + " before sort-key rewrite");
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AlterCancelException("interrupted draining refresh for mv " + tableName);
            }
        }
    }

    @Override
    protected void afterJobSettled(boolean cancelled) {
        // Only restore refresh if it was active at submit -- never resurrect a task the user had paused.
        if (refreshWasActiveAtSubmit) {
            try {
                getRefreshCoordinator().resumeRefresh();  // idempotent; resume on FINISH and on CANCEL
            } catch (Exception e) {
                // The terminal FINISHED/CANCELLED state is already journaled by the time this runs
                // (called from runFinishedRewritingJob/cancelImpl in the base), so this is best-effort
                // cleanup -- letting the exception escape would propagate up through the runAlterJobV2
                // scheduler loop and could skip sibling alter jobs in that pass. Refresh will be
                // restored on the next FE restart/failover via onReplayReconcile.
                LOG.warn("failed to resume refresh task for mv {} after sort-key rewrite job {} settled "
                        + "(cancelled={}); will be reconciled on next FE restart/failover replay",
                        tableName, jobId, cancelled, e);
            }
        }
    }

    @Override
    protected void configureRewriteInsert(InsertStmt insertStmt) {
        // The rewrite target is a materialized view; InsertAnalyzer rejects a non-system INSERT into an
        // MV (mirrors MVRefreshProcessor.generateInsertAst). It is a plain INSERT INTO (not overwrite),
        // so no other isSystem()-gated behavior is affected.
        insertStmt.setSystem(true);
    }

    @Override
    protected Set<String> affectedColumnsForMvInactivation(@NotNull OlapTable table) {
        // A sort-key reorder does not change the MV's output columns or keysType, so nested dependent
        // MVs stay valid. Return empty (inactiveRelatedMaterializedViewsRecursive early-returns).
        return Sets.newHashSet();
    }

    @Override
    protected void visualiseShadowIndex(@NotNull OlapTable table) {
        super.visualiseShadowIndex(table);
        // Keep the MV sort-key property in sync with the flipped physical order so SHOW CREATE renders
        // the new ORDER BY and BestMvSelector orders columns correctly. Runs in the flip applier under
        // the table write lock, so it is journaled/replayed with the rest of the flip.
        if (table instanceof MaterializedView && getNewSortKeyColumns() != null) {
            applyNewMvSortKeys((MaterializedView) table);
        }
    }

    /**
     * Applies {@link #getNewSortKeyColumns()} to the MV's {@code TableProperty}: updates the transient
     * {@code mvSortKeys} field AND persists it into the serialized {@code properties} map (via
     * {@link TableProperty#putMvSortKeys()}), so the new order survives an FE restart / checkpoint
     * reload instead of being rebuilt from the pre-rewrite {@code mv_sort_keys} property.
     */
    @VisibleForTesting
    protected void applyNewMvSortKeys(@NotNull MaterializedView mv) {
        List<String> newOrder = getNewSortKeyColumns().stream()
                .map(Column::getName).collect(Collectors.toList());
        TableProperty property = mv.getTableProperty();
        if (property != null) {
            property.setMvSortKeys(newOrder);
            property.putMvSortKeys();
        }
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
        if (this != replayedJob) {   // copy the subclass field before reconcile reads it
            this.refreshWasActiveAtSubmit = ((LakeMvSortKeyRewriteJob) replayedJob).refreshWasActiveAtSubmit;
        }
        super.replay(replayedJob);   // reconstructs shadow catalog + SCHEMA_CHANGE for WAITING_TXN+; re-runs the flip for FINISHED
        // MV-specific replay reconcile: resume refresh on a terminal state (backstop for a crash between
        // the terminal alter entry and the resumeTask journal), and force SCHEMA_CHANGE for a non-terminal
        // state so a post-failover manual refresh's INSERT OVERWRITE is rejected until the scheduler
        // re-runs runPendingJob live. Uses isReplay=true (no journal/drain).
        onReplayReconcile();
    }

    /**
     * Seam invoked at the end of {@link #replay}, AFTER {@code super.replay} has released its table lock --
     * so any catalog mutation here acquires its own. Uses the isReplay=true refresh ops (no edit-log
     * write, no drain) and is idempotent and self-healing across repeated replays:
     * <ul>
     *   <li>terminal (FINISHED/CANCELLED): {@link RefreshCoordinator#resumeRefreshForReplay} -- closes the
     *       crash gap where the terminal alter entry journaled but the live resumeTask journal did not.
     *       Guarded by {@code refreshWasActiveAtSubmit}: never resurrect a task the user had paused.</li>
     *   <li>non-terminal (PENDING/WAITING_TXN/RUNNING/FINISHED_REWRITING): force the MV to
     *       {@link #jobTableState} so a post-failover manual refresh's INSERT OVERWRITE is rejected until
     *       the scheduler re-runs runPendingJob live (which re-suspends and re-drains), covering the
     *       PENDING branch where {@code super.replay} installs no catalog state (see
     *       {@link LakeOnlineRewriteJobBase#replay}); and converge the refresh task to PAUSE via
     *       {@link RefreshCoordinator#suspendRefreshForReplay} (SCHEMA_CHANGE alone protects the data but
     *       not the task's pause state).</li>
     * </ul>
     */
    protected void onReplayReconcile() {
        if (jobState.isFinalState()) {
            if (refreshWasActiveAtSubmit) {
                getRefreshCoordinator().resumeRefreshForReplay();
            }
        } else {
            ensureMvStateSchemaChangeOnReplay();
            getRefreshCoordinator().suspendRefreshForReplay();
        }
    }

    /**
     * Force the MV into {@link #jobTableState} on a non-terminal replay. Called with no lock held (the
     * lock {@link LakeOnlineRewriteJobBase#replay} takes is released by the time this runs), so this
     * acquires its own write lock rather than assuming one is already held.
     */
    private void ensureMvStateSchemaChangeOnReplay() {
        try (AutoCloseableLock ignore = new AutoCloseableLock(dbId, List.of(tableId), LockType.WRITE)) {
            OlapTable table = getTable();
            if (table != null) {
                table.setState(jobTableState());
            }
        }
    }

    @Override
    public AlterJobV2 copyForPersist() {
        return new LakeMvSortKeyRewriteJob(this);
    }

    /**
     * Production {@link RefreshCoordinator}: resolves the MV and its refresh {@link Task} fresh on every
     * call (the catalog object can change across replay/failover) and is a best-effort no-op once the MV,
     * its task, or the containing table is gone -- TaskManager's own edit-log journal is the primary
     * durability mechanism, so a reconcile that cannot resolve the task is harmless.
     */
    private static final class DefaultRefreshCoordinator implements RefreshCoordinator {
        private final long dbId;
        private final long tableId;

        DefaultRefreshCoordinator(long dbId, long tableId) {
            this.dbId = dbId;
            this.tableId = tableId;
        }

        private MaterializedView resolveMv() {
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(dbId, tableId);
            return table instanceof MaterializedView ? (MaterializedView) table : null;
        }

        private Task resolveTask() {
            MaterializedView mv = resolveMv();
            return mv == null ? null : GlobalStateMgr.getCurrentState().getTaskManager().getTask(mv);
        }

        @Override
        public void suspendRefresh() {
            Task task = resolveTask();
            if (task != null) {
                GlobalStateMgr.getCurrentState().getTaskManager().suspendTask(task, false);
            }
        }

        @Override
        public void resumeRefresh() {
            Task task = resolveTask();
            if (task != null) {
                GlobalStateMgr.getCurrentState().getTaskManager().resumeTask(task, false);
            }
        }

        @Override
        public void suspendRefreshForReplay() {
            Task task = resolveTask();
            if (task != null) {
                GlobalStateMgr.getCurrentState().getTaskManager().suspendTask(task, true);
            }
        }

        @Override
        public void resumeRefreshForReplay() {
            Task task = resolveTask();
            if (task != null) {
                GlobalStateMgr.getCurrentState().getTaskManager().resumeTask(task, true);
            }
        }

        @Override
        public boolean hasRunningOverwriteJob() {
            return GlobalStateMgr.getCurrentState().getInsertOverwriteJobMgr().hasRunningOverwriteJob(tableId);
        }

        @Override
        public boolean hasTempPartitions() {
            MaterializedView mv = resolveMv();
            return mv != null && mv.existTempPartitions();
        }
    }
}
