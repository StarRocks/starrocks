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

package com.starrocks.alter.reshard;

import com.starrocks.catalog.ColocateGroupSchema;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndex.IndexExtState;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletRange;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.planner.RangeColocateScanDispatch;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Colocate checker — drives every unstable range-colocate group toward range alignment
 * with {@link com.starrocks.catalog.ColocateRangeMgr}'s expected ranges and marks every
 * peer GroupId stable once aligned.
 *
 * <h3>Architecture</h3>
 * Stateless component owned by {@link TabletReshardJobMgr} and invoked from that manager's
 * existing scheduler tick. Every {@link #runOneCycle} call walks
 * {@link ColocateTableIndex#getUnstableGroupIds} and either does work or fast-returns when the
 * unstable set is empty (the steady-state). Shares the manager's
 * {@code tablet_reshard_job_scheduler_interval_ms} cadence; no separate thread, timer, or
 * config. Eliminates the second worker thread and the leader-state reconcile loop that the
 * previous {@code LeaderDaemon} ownership shape required.
 *
 * <p>Leader-state is gated at the manager level: {@code TabletReshardJobMgr} is only started
 * inside {@code GlobalStateMgr.startLeaderOnlyDaemonThreads}, so the checker never runs on a
 * brand-new follower. For the demoted-leader edge case, the per-RPC
 * {@link GlobalStateMgr#isLeaderWorkAdmissionOpen() admission} check inside
 * {@link SplitTabletJobFactory#forColocateAlignment} (and inside {@link
 * ColocateTableIndex#markAllGroupsWithSameColocateGroupIdStable}'s journal write) is the
 * safety net.
 *
 * <h3>What a cycle does</h3>
 * Iterates every unstable range-colocate {@code colocateGroupId}; for each peer {@link
 * ColocateTableIndex.GroupId}, every {@code NORMAL}-state table, every visible
 * {@link MaterializedIndex}; per misaligned tablet, computes the FE-supplied per-new-tablet
 * {@link TabletRange} list via {@link RangeColocateScanDispatch#computeAlignedTabletRanges}
 * and batches all misaligned tuples in a table into ONE {@link SplitTabletJob} (one job per
 * table per cycle — see {@link SplitTabletJobFactory#forColocateAlignment}). When every peer
 * is range-aligned, {@link ColocateTableIndex#markAllGroupsWithSameColocateGroupIdStable}
 * fires across peers in lock-step.
 */
public class ColocateChecker {
    private static final Logger LOG = LogManager.getLogger(ColocateChecker.class);

    /**
     * Invoked from {@link TabletReshardJobMgr#runAfterCatalogReady} on every scheduler tick.
     * Fast-returns when there's no work to do; otherwise iterates unstable range-colocate
     * groups and reconciles each one toward stable. The steady-state fast-path is O(1).
     */
    public void runOneCycle() {
        if (!RunMode.isSharedDataMode()) {
            return;
        }
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        // Steady-state fast-path: no unstable groups → cheap read-lock empty-check, no allocation.
        if (!colocateTableIndex.hasUnstableGroups()) {
            return;
        }
        Set<Long> processedColocateGroupIds = new HashSet<>();
        for (ColocateTableIndex.GroupId groupId : colocateTableIndex.getUnstableGroupIds()) {
            if (!colocateTableIndex.isRangeColocateGroup(groupId)) {
                continue;
            }
            // Peer GroupIds (cross-DB) share the same colocateGroupId; processGroup handles all
            // peers in one pass, so only run once per unique colocateGroupId.
            if (!processedColocateGroupIds.add(groupId.grpId)) {
                continue;
            }
            try {
                processGroup(colocateTableIndex, groupId.grpId);
            } catch (Exception e) {
                LOG.warn("failed to process colocate group id {}", groupId.grpId, e);
            }
        }
    }

    /**
     * Drive one {@code colocateGroupId} toward stability: iterate every peer GroupId × table ×
     * partition × visible index; fire an alignment {@link SplitTabletJob} for every table
     * with at least one misaligned tablet; if and only if every peer is fully aligned, mark
     * every peer GroupId stable in lock-step.
     */
    private void processGroup(ColocateTableIndex colocateTableIndex, long colocateGroupId) {
        List<ColocateRange> expectedRanges =
                colocateTableIndex.getColocateRangeMgr().getColocateRanges(colocateGroupId);
        if (expectedRanges.isEmpty()) {
            return;
        }
        List<ColocateTableIndex.GroupId> peers =
                colocateTableIndex.getAllGroupIdsWithSameColocateGroupId(colocateGroupId);
        if (peers.isEmpty()) {
            return;
        }
        ColocateGroupSchema schema = colocateTableIndex.getGroupSchema(peers.get(0));
        if (schema == null) {
            return;
        }
        int colocateColumnCount = schema.getColocateColumnCount();

        boolean allAligned = true;
        for (ColocateTableIndex.GroupId peerGroupId : peers) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(peerGroupId.dbId);
            if (db == null) {
                continue;
            }
            for (long tableId : colocateTableIndex.getAllTableIds(peerGroupId)) {
                if (!alignTableIfApplicable(db, tableId, expectedRanges, colocateColumnCount, colocateGroupId)) {
                    allAligned = false;
                }
            }
        }

        if (allAligned) {
            colocateTableIndex.markAllGroupsWithSameColocateGroupIdStable(colocateGroupId, true);
            LOG.info("marked colocate group id {} stable across {} peer GroupIds",
                    colocateGroupId, peers.size());
        }
    }

    /**
     * Per-table dispatch for {@link #processGroup}: looks up the table, filters out non-OlapTable
     * entries (still considered "aligned" — alignment isn't applicable), defers tables not in
     * {@code NORMAL} state, and otherwise hands off to {@link #processTable}.
     *
     * @return {@code true} iff the table contributed no obstacle to marking the colocate group
     *         stable this cycle (already aligned, or not an OlapTable). {@code false} when work
     *         is still needed (misaligned tablets, in-flight alter, lookup failure).
     */
    private boolean alignTableIfApplicable(Database db, long tableId, List<ColocateRange> expectedRanges,
                                            int colocateColumnCount, long colocateGroupId) {
        Table fetchedTable;
        try {
            fetchedTable = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        } catch (Exception e) {
            LOG.debug("table {} lookup failed in db {}, skipping", tableId, db.getId(), e);
            return false;
        }
        if (!(fetchedTable instanceof OlapTable olapTable)) {
            return true;
        }
        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            // In-flight alter / reshard job — skip this cycle, revisit next. Avoids the
            // SplitTabletJob.setTableState(NORMAL -> TABLET_RESHARD) race when two jobs
            // target the same table.
            return false;
        }
        try {
            return processTable(db, olapTable, expectedRanges, colocateColumnCount);
        } catch (Exception e) {
            LOG.warn("alignment failed for table {}.{} in colocate group id {}",
                    db.getFullName(), olapTable.getName(), colocateGroupId, e);
            return false;
        }
    }

    /**
     * Inspects every visible materialized index in every physical partition of {@code table};
     * if any tablet is not range-aligned with {@code expectedRanges}, builds the per-tablet
     * forced-boundaries map and fires a single alignment {@link SplitTabletJob} for the table.
     *
     * @return {@code true} iff every tablet in every visible index was already aligned (no job
     *         fired); {@code false} otherwise — the caller leaves the colocate group unstable.
     */
    private boolean processTable(Database db, OlapTable table, List<ColocateRange> expectedRanges,
                                  int colocateColumnCount) throws StarRocksException {
        // physicalPartitionId -> indexId -> oldTabletId -> per-new-tablet ranges that tile the
        // old tablet's range. Empty map means every tablet in every visible index is already
        // aligned against expectedRanges.
        Map<Long, Map<Long, Map<Long, List<TabletRange>>>> alignmentMap = new HashMap<>();
        boolean alignedSoFar = true;
        List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(table);

        try (AutoCloseableLock lock = new AutoCloseableLock(db.getId(), table.getId(), LockType.READ)) {
            for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
                for (MaterializedIndex index :
                        physicalPartition.getLatestMaterializedIndices(IndexExtState.VISIBLE)) {
                    if (RangeColocateScanDispatch.isTabletRangesAligned(
                            index, sortKeyColumns, expectedRanges, colocateColumnCount)) {
                        continue;
                    }
                    alignedSoFar = false;
                    for (Tablet tablet : index.getTablets()) {
                        if (tablet.getRange() == null) {
                            continue;
                        }
                        List<TabletRange> newRanges = RangeColocateScanDispatch.computeAlignedTabletRanges(
                                tablet.getRange().getRange(), sortKeyColumns, expectedRanges, colocateColumnCount);
                        if (newRanges.isEmpty()) {
                            continue;
                        }
                        alignmentMap
                                .computeIfAbsent(physicalPartition.getId(), k -> new HashMap<>())
                                .computeIfAbsent(index.getId(), k -> new HashMap<>())
                                .put(tablet.getId(), newRanges);
                    }
                }
            }
        }

        if (alignmentMap.isEmpty()) {
            return alignedSoFar;
        }

        // Re-validate leader admission immediately before the side-effecting factory call:
        // forColocateAlignment creates StarOS shards BEFORE addTabletReshardJob writes the
        // admission-guarded journal record. If demotion started since the cycle began,
        // proceeding would leak external shards.
        if (!GlobalStateMgr.getCurrentState().isLeaderWorkAdmissionOpen()) {
            LOG.info("leader admission closed before submitting alignment job for {}.{}, "
                            + "deferring to next cycle", db.getFullName(), table.getName());
            return false;
        }

        TabletReshardJob job = SplitTabletJobFactory.forColocateAlignment(db, table, alignmentMap);
        GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().addTabletReshardJob(job);
        LOG.info("submitted SplitTabletJob {} for table {}.{} covering {} partitions",
                job.getJobId(), db.getFullName(), table.getName(), alignmentMap.size());
        return false;
    }
}
