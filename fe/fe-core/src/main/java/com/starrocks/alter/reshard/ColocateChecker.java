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

import com.staros.client.StarClientException;
import com.staros.proto.ShardInfo;
import com.starrocks.catalog.ColocateGroupSchema;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateRangeUtils;
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
import com.starrocks.catalog.Tuple;
import com.starrocks.common.DdlException;
import com.starrocks.common.Range;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.planner.RangeColocateScanDispatch;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.common.MetaUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
 * brand-new follower. For the demoted-leader edge case, the alignment job's StarOS shard
 * creation is journal-first (runs in {@link SplitTabletJob#runPendingJob} after the job is
 * persisted), and the journal write inside {@link
 * ColocateTableIndex#markAllGroupsWithSameColocateGroupIdStable} is admission-gated — these
 * together prevent shard-leak on demotion.
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

    // Cap on tablets per getShardInfo RPC during PACK reconcile. A local safety bound on this
    // (potentially large) sweep — getShardInfo is not globally chunked, and other callers pass
    // smaller, bounded lists.
    private static final int GET_SHARD_INFO_BATCH_SIZE = 1000;

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
        List<ColocateRange> expectedRanges = colocateTableIndex.getColocateRanges(colocateGroupId);
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
            // Ranges are settled, so each tablet's expected PACK shard group is now well-defined.
            // Migrate any tablet still in the wrong PACK shard group (e.g. a split child left in the
            // old group) into place, and flip the group stable ONLY once placement is repaired. The
            // checker never revisits a stable group, so marking stable while a reassignment is still
            // pending (or a StarOS query/RPC failed) would leak a permanently mis-placed tablet. This
            // gate is on membership repair being issued and confirmed; gating on actual StarOS
            // worker-placement convergence can be layered on top later.
            if (reconcilePackPlacement(colocateTableIndex, peers, expectedRanges, colocateColumnCount)) {
                colocateTableIndex.markAllGroupsWithSameColocateGroupIdStable(colocateGroupId, true);
                LOG.info("marked colocate group id {} stable across {} peer GroupIds",
                        colocateGroupId, peers.size());
            }
        }
    }

    /**
     * For every NORMAL-state table across all peer GroupIds, finds tablets whose actual StarOS
     * PACK shard-group membership disagrees with the PACK group expected from their range and
     * issues a {@link StarOSAgent#reassignShardGroups} to migrate each into place.
     *
     * @return {@code true} iff placement is fully settled — every table's membership was read
     *         completely and no tablet needed repair. A read/RPC failure, an incomplete membership
     *         response, a transient table-lookup failure, or any misplaced tablet (reassignment
     *         issued but only confirmed by a re-read next cycle) makes the group NOT settled, so the
     *         caller leaves it unstable and retries on the next tick.
     */
    private boolean reconcilePackPlacement(ColocateTableIndex colocateTableIndex,
                                           List<ColocateTableIndex.GroupId> peers,
                                           List<ColocateRange> expectedRanges, int colocateColumnCount) {
        boolean settled = true;
        for (ColocateTableIndex.GroupId peerGroupId : peers) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(peerGroupId.dbId);
            if (db == null) {
                continue;
            }
            for (long tableId : colocateTableIndex.getAllTableIds(peerGroupId)) {
                Table fetchedTable;
                try {
                    fetchedTable = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
                } catch (Exception e) {
                    LOG.debug("table {} lookup failed in db {} during PACK reconcile, retrying next cycle",
                            tableId, db.getId(), e);
                    settled = false;
                    continue;
                }
                if (!(fetchedTable instanceof OlapTable olapTable)) {
                    continue;
                }
                if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
                    settled = false;
                    continue;
                }
                settled &= reconcileTablePackPlacement(db, olapTable, expectedRanges, colocateColumnCount);
            }
        }
        return settled;
    }

    /**
     * Snapshots every visible tablet's range under a table READ lock, queries the tablets' actual
     * shard-group membership from StarOS in bounded batches, and reassigns each {@link MisplacedTablet}
     * into its expected PACK shard group.
     *
     * @return {@code true} iff the table is fully settled: the membership read covered every tablet
     *         and nothing needed repair. A StarOS query failure, an incomplete response (a requested
     *         tablet missing from the result — treated as an unread membership, never as "no groups",
     *         so it cannot turn into a bogus add-only reassignment), or any misplaced tablet
     *         (reassignment issued, confirmed by the next cycle's re-read) ⇒ {@code false}.
     */
    private boolean reconcileTablePackPlacement(Database db, OlapTable table,
                                                List<ColocateRange> expectedRanges, int colocateColumnCount) {
        Map<Long, Range<Tuple>> tabletIdToRange = new HashMap<>();
        try (AutoCloseableLock lock = new AutoCloseableLock(db.getId(), table.getId(), LockType.READ)) {
            for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
                for (MaterializedIndex index :
                        physicalPartition.getLatestMaterializedIndices(IndexExtState.VISIBLE)) {
                    for (Tablet tablet : index.getTablets()) {
                        if (tablet.getRange() != null) {
                            tabletIdToRange.put(tablet.getId(), tablet.getRange().getRange());
                        }
                    }
                }
            }
        }
        if (tabletIdToRange.isEmpty()) {
            return true;
        }

        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        List<Long> tabletIds = new ArrayList<>(tabletIdToRange.keySet());
        Map<Long, List<Long>> tabletIdToGroupIds = new HashMap<>();
        // Query membership in bounded batches so one very large table cannot produce an oversized RPC.
        for (int batchStart = 0; batchStart < tabletIds.size(); batchStart += GET_SHARD_INFO_BATCH_SIZE) {
            List<Long> batch = tabletIds.subList(batchStart,
                    Math.min(batchStart + GET_SHARD_INFO_BATCH_SIZE, tabletIds.size()));
            try {
                for (ShardInfo shardInfo : starOSAgent.getShardInfo(batch, StarOSAgent.DEFAULT_WORKER_GROUP_ID)) {
                    tabletIdToGroupIds.put(shardInfo.getShardId(), shardInfo.getGroupIdsList());
                }
            } catch (StarClientException e) {
                LOG.warn("failed to query shard membership for PACK reconcile of table {}.{}: {}",
                        db.getFullName(), table.getName(), e.getMessage());
                return false;
            }
        }
        // An incomplete response is an unread membership, not "tablet has no groups": treat it as a
        // failed read and retry next cycle, so a missing StarOS entry cannot become a bogus repair.
        if (!tabletIdToGroupIds.keySet().containsAll(tabletIdToRange.keySet())) {
            LOG.warn("incomplete shard membership response during PACK reconcile of table {}.{}; retrying",
                    db.getFullName(), table.getName());
            return false;
        }

        List<MisplacedTablet> misplacedTablets =
                findMisplacedTablets(tabletIdToRange, tabletIdToGroupIds, expectedRanges, colocateColumnCount);
        for (MisplacedTablet misplaced : misplacedTablets) {
            // Send the minimal delta: only add the expected group if the tablet is not already in it
            // (the double-membership case after a partial prior repair needs a remove-only update),
            // and only remove a stale group if one is present.
            List<Long> addGroupIds = tabletIdToGroupIds.get(misplaced.tabletId()).contains(misplaced.expectedPackGroupId())
                    ? Collections.emptyList()
                    : List.of(misplaced.expectedPackGroupId());
            List<Long> removeGroupIds = misplaced.currentPackGroupId() == PhysicalPartition.INVALID_SHARD_GROUP_ID
                    ? Collections.emptyList()
                    : List.of(misplaced.currentPackGroupId());
            try {
                starOSAgent.reassignShardGroups(misplaced.tabletId(), addGroupIds, removeGroupIds);
                LOG.info("reassigned tablet {} from PACK shard group {} to {} in table {}.{}",
                        misplaced.tabletId(), misplaced.currentPackGroupId(), misplaced.expectedPackGroupId(),
                        db.getFullName(), table.getName());
            } catch (DdlException e) {
                LOG.warn("failed to reassign tablet {} to PACK shard group {} in table {}.{}: {}",
                        misplaced.tabletId(), misplaced.expectedPackGroupId(),
                        db.getFullName(), table.getName(), e.getMessage());
            }
        }
        // Settled only when nothing needed repair; if reassignments were issued, stay unstable so
        // the next cycle re-reads and confirms membership before the group is flipped stable.
        return misplacedTablets.isEmpty();
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

        try (AutoCloseableLock lock = new AutoCloseableLock(db.getId(), table.getId(), LockType.READ)) {
            for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
                for (MaterializedIndex index :
                        physicalPartition.getLatestMaterializedIndices(IndexExtState.VISIBLE)) {
                    // Each visible index (base + every rollup/MV) can have its own sort-key arity.
                    // Using the base index's sort key for an MV with a shorter prefix would compute
                    // boundaries the MV's tablets can never align with — alignment iteration would
                    // livelock. Resolve per-index here (E1). Use getMetaId() (not getId()) — the
                    // physical id changes after reshard while metaId is stable.
                    List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(table, index.getMetaId());
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

        // The factory now only builds local state and journals the job; the StarOS shard
        // creation runs in SplitTabletJob.runPendingJob (after the journal write), so a
        // leader demotion at this point cannot leak external shards.
        TabletReshardJob job = SplitTabletJobFactory.forColocateAlignment(db, table, alignmentMap);
        GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().addTabletReshardJob(job);
        LOG.info("submitted SplitTabletJob {} for table {}.{} covering {} partitions",
                job.getJobId(), db.getFullName(), table.getName(), alignmentMap.size());
        return false;
    }

    // ---- Misplaced-PACK-group detection ----
    //
    // Range alignment (the loop above) is sufficient for query correctness but not for host-local
    // execution: a user-driven Level-1 split leaves one child in the OLD PACK shard group, so after
    // post-publish reclassification a tablet can be range-aligned yet sit in the wrong PACK shard
    // group. The methods below identify those tablets by comparing each tablet's actual StarOS
    // shard-group membership against the PACK shard group expected from its range per
    // ColocateRangeMgr. The resulting (remove currentPackGroupId, add expectedPackGroupId) delta is
    // what StarOSAgent.reassignShardGroups consumes to migrate the tablet. The range->PACK-group
    // mapping itself lives in ColocateRangeUtils.lookupPackShardGroupId, shared with SplitTabletJob's
    // post-publish classifier.

    /**
     * A tablet whose actual PACK shard-group membership disagrees with the PACK shard group expected
     * from its range. {@code currentPackGroupId} is a stale colocate PACK shard group the tablet is
     * still a member of (or {@link PhysicalPartition#INVALID_SHARD_GROUP_ID} if it is in none), and
     * {@code expectedPackGroupId} is where it belongs — together the move delta a future
     * {@code reassignShardGroups} consumes.
     */
    public record MisplacedTablet(long tabletId, long currentPackGroupId, long expectedPackGroupId) {
    }

    /**
     * Classifies one tablet's PACK-shard-group placement against expectation. Returns {@code null}
     * when the tablet is correctly placed — a member of {@code expectedPackGroupId} and of no other
     * PACK shard group in {@code packGroupIds}. Otherwise returns the {@link MisplacedTablet} move:
     * {@code currentPackGroupId} is a stale colocate PACK shard group still present (or
     * {@link PhysicalPartition#INVALID_SHARD_GROUP_ID} if none), and {@code expectedPackGroupId} is the
     * target. SPREAD and unrelated shard groups are ignored because they are not in {@code packGroupIds}.
     */
    static MisplacedTablet classifyTabletPlacement(long tabletId, List<Long> actualGroupIds,
                                                   long expectedPackGroupId, Set<Long> packGroupIds) {
        boolean alreadyInExpectedGroup = actualGroupIds.contains(expectedPackGroupId);
        long stalePackGroupId = PhysicalPartition.INVALID_SHARD_GROUP_ID;
        for (long groupId : actualGroupIds) {
            if (groupId != expectedPackGroupId && packGroupIds.contains(groupId)) {
                stalePackGroupId = groupId;
                break;
            }
        }
        if (alreadyInExpectedGroup && stalePackGroupId == PhysicalPartition.INVALID_SHARD_GROUP_ID) {
            return null;
        }
        return new MisplacedTablet(tabletId, stalePackGroupId, expectedPackGroupId);
    }

    /**
     * Pure detection over a set of tablets: for each tablet whose colocate prefix maps to a known
     * {@link ColocateRange} (via {@link ColocateRangeUtils#lookupPackShardGroupId}), compares its
     * actual shard-group membership ({@code tabletIdToGroupIds}, as returned by
     * {@code StarOSAgent.getShardInfo(...).getGroupIdsList()} — the StarOS shard id equals the tablet
     * id in shared-data mode) against the expected PACK shard group and collects every
     * {@link MisplacedTablet}. Tablets whose range is not covered by {@code expectedRanges} are
     * skipped (defensive; cannot happen under the full-coverage invariant).
     */
    static List<MisplacedTablet> findMisplacedTablets(Map<Long, Range<Tuple>> tabletIdToRange,
                                                      Map<Long, List<Long>> tabletIdToGroupIds,
                                                      List<ColocateRange> expectedRanges,
                                                      int colocateColumnCount) {
        Set<Long> packGroupIds = expectedRanges.stream()
                .map(ColocateRange::getShardGroupId)
                .collect(Collectors.toSet());
        List<MisplacedTablet> misplaced = new ArrayList<>();
        for (Map.Entry<Long, Range<Tuple>> entry : tabletIdToRange.entrySet()) {
            long tabletId = entry.getKey();
            long expectedGroupId = ColocateRangeUtils.lookupPackShardGroupId(
                    entry.getValue(), expectedRanges, colocateColumnCount);
            if (expectedGroupId == PhysicalPartition.INVALID_SHARD_GROUP_ID) {
                continue;
            }
            List<Long> actualGroupIds = tabletIdToGroupIds.getOrDefault(tabletId, Collections.emptyList());
            MisplacedTablet misplacedTablet =
                    classifyTabletPlacement(tabletId, actualGroupIds, expectedGroupId, packGroupIds);
            if (misplacedTablet != null) {
                misplaced.add(misplacedTablet);
            }
        }
        return misplaced;
    }
}
