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

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
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
import com.starrocks.common.Config;
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
 * Owned by {@link TabletReshardJobMgr} and invoked from that manager's existing scheduler tick.
 * Holds one piece of state — a {@link ColocateConvergenceCache} that throttles repeated StarOS
 * placement-convergence queries; every other decision is recomputed each tick. Every
 * {@link #runOneCycle} call walks
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

    // Throttles per-tick queryShardGroupStable load; all its semantics live in the cache class.
    private final ColocateConvergenceCache convergenceCache = new ColocateConvergenceCache();

    // Cap on consecutive re-fires after an aborted alignment attempt on an unchanged layout, so a
    // persistently-aborting job cannot become its own (slow) storm. Past the cap the table is
    // treated like a deterministic dead-end and left unaligned until its layout/data changes.
    static final int ALIGNMENT_ABORT_RETRY_CAP = 3;

    // Per-table memory of the last alignment attempt (keyed by tableId). Split/merge is deterministic,
    // so re-issuing identical alignment work on an unchanged layout makes no progress — it just churns
    // tablets. This map lets the checker suppress that re-issue (the fix for the self-sustaining
    // alignment-job storm). The latch is per TABLE (not per group) because alignment jobs are
    // per-table: keying by group would let one table's attempt suppress a peer that had never been
    // attempted, or miss a peer's abort. An entry is cleared when the table becomes aligned, and the
    // whole map is cleared whenever no group is unstable; it also naturally re-arms when the table's
    // layout/data changes, on a bounded number of aborts, or after an FE restart.
    private final Map<Long, TableAlignmentAttempt> lastAttemptByTable = new HashMap<>();

    /**
     * What the checker last did for one table: the {@link #tableConvergenceSignature} observed when it
     * fired, the alignment job id submitted, and how many times an aborted attempt has been re-fired on
     * this same signature. {@code suppressionLogged} keeps the "no progress" warning to once per stuck
     * state.
     */
    static final class TableAlignmentAttempt {
        final long signature;
        final long jobId;
        final int abortRetries;
        boolean suppressionLogged;

        TableAlignmentAttempt(long signature, long jobId, int abortRetries) {
            this.signature = signature;
            this.jobId = jobId;
            this.abortRetries = abortRetries;
        }
    }

    /** Outcome of {@link #decideAlignment}: whether to fire this cycle and the abort counter to store. */
    record AlignmentDecision(boolean fire, int nextAbortRetries) {
    }

    /**
     * Should the checker (re)issue an alignment job for a table this cycle, and if it fires what
     * abort-retry count should it record? Fire when this is the first attempt ({@code prev == null}) or
     * the layout/data changed since the last attempt (real progress is possible); otherwise the last
     * completed attempt made no progress on this exact input and — because split/merge is deterministic
     * — a re-issue would too, so suppress. The sole exception is a previous attempt that ended in an
     * abort (a transient failure, not a deterministic dead-end): re-fire, but only up to
     * {@link #ALIGNMENT_ABORT_RETRY_CAP} times on an unchanged signature. {@code nextAbortRetries} is
     * only meaningful when {@code fire} is true and a job is actually submitted.
     */
    static AlignmentDecision decideAlignment(TableAlignmentAttempt prev, long currentSignature,
            boolean prevJobAborted) {
        if (prev == null || prev.signature != currentSignature) {
            return new AlignmentDecision(true, 0);
        }
        boolean fire = prevJobAborted && prev.abortRetries < ALIGNMENT_ABORT_RETRY_CAP;
        return new AlignmentDecision(fire, prev.abortRetries + 1);
    }

    /** Visible for testing: whether a table currently has a recorded alignment attempt (is latched). */
    boolean hasRecordedAttempt(long tableId) {
        return lastAttemptByTable.containsKey(tableId);
    }

    /** True iff the alignment job is still tracked and ended in {@code ABORTED}. */
    static boolean isJobAborted(long jobId) {
        TabletReshardJob job = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().getTabletReshardJob(jobId);
        return job != null && job.isAborted();
    }

    /** True iff the alignment job has reached a terminal state (or is no longer tracked). */
    static boolean isJobSettled(long jobId) {
        TabletReshardJob job = GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().getTabletReshardJob(jobId);
        return job == null || job.isDone();
    }

    // murmur3-128 via Guava's Hashing (the idiom already used across FE, e.g. HDFSBackendSelector /
    // the hash rings) — no hand-rolled mixing. Order-independent combination uses
    // Hashing.combineUnordered so the signature does not depend on partition / index / tablet order.
    private static final HashFunction SIGNATURE_HASH = Hashing.murmur3_128();

    /**
     * Order-independent signature of the group's expected ranges — the part of a table's convergence
     * signature that is shared across all peer tables, so a change to the colocate ranges (e.g. a peer
     * split adds a boundary) re-arms every table. Each range is hashed and the results are combined with
     * {@link Hashing#combineUnordered} so list order does not matter.
     */
    static long expectedRangesSignature(List<ColocateRange> expectedRanges) {
        List<HashCode> parts = new ArrayList<>(expectedRanges.size());
        for (ColocateRange colocateRange : expectedRanges) {
            parts.add(SIGNATURE_HASH.hashInt(colocateRange.getRange().hashCode()));
        }
        // A registered group always has at least the [MIN, MAX) range, so parts is never empty
        // (combineUnordered requires a non-empty iterable).
        return Hashing.combineUnordered(parts).asLong();
    }

    /**
     * Canonical, order-independent 64-bit signature of everything one table's alignment decision and
     * split feasibility depend on: {@code expectedRangesSig} plus the table's visible-index tablet
     * ranges and each physical partition's {@code dataVersion}. It intentionally excludes tablet ids
     * (they churn on every fallback split, which would defeat the latch). A murmur3 hash (rather than a
     * concatenated string) is used so it cannot alias on a delimiter inside a VARCHAR range value, stays
     * compact regardless of tablet count, and is trivial to extend by hashing in one more field. Per-tablet
     * and per-index contributions are combined with {@link Hashing#combineUnordered}, so the result is
     * order-independent and deterministic; a hash collision would only mask a real change, which is
     * self-healing (the table stays unaligned → correct shuffle, and re-arms on the next change).
     * Read-locks the table.
     *
     * <p>{@code dataVersion} — not {@code visibleVersion} — is included because BE's external-boundaries
     * split can fall back to an identical tablet for data-distribution reasons, not only structural
     * ones: it rejects a split whose effective segment envelope is empty or a degenerate single key, or
     * whose per-rowset row/byte weight does not straddle the boundary (see {@code tablet_splitter.cpp}
     * steps 5-6 and 10). So a tablet that cannot be split today can become splittable after a load
     * widens its data — with no tablet-range change — and the latch must re-arm on that. A reshard
     * publish (including the identical-tablet fallback) advances only {@code visibleVersion}, while a
     * real load advances {@code dataVersion}, so keying on {@code dataVersion} re-arms on genuine data
     * changes but not on fallback churn. Tablet ranges are still carried to detect reshard progress (a
     * successful split changes ranges but not {@code dataVersion}).
     */
    static long tableConvergenceSignature(Database db, OlapTable table, long expectedRangesSig) {
        List<HashCode> indexParts = new ArrayList<>();
        indexParts.add(SIGNATURE_HASH.hashLong(expectedRangesSig));
        try (AutoCloseableLock lock = new AutoCloseableLock(db.getId(), table.getId(), LockType.READ)) {
            for (PhysicalPartition physicalPartition : table.getPhysicalPartitions()) {
                for (MaterializedIndex index :
                        physicalPartition.getLatestMaterializedIndices(IndexExtState.VISIBLE)) {
                    List<HashCode> tabletParts = new ArrayList<>();
                    for (Tablet tablet : index.getTablets()) {
                        tabletParts.add(SIGNATURE_HASH.hashInt(
                                tablet.getRange() == null ? 0 : tablet.getRange().getRange().hashCode()));
                    }
                    Hasher indexHasher = SIGNATURE_HASH.newHasher()
                            .putLong(physicalPartition.getId())
                            .putLong(index.getMetaId())
                            .putLong(physicalPartition.getDataVersion());
                    // Combine tablet ranges order-independently within the index (empty for a
                    // range-less index — its topology still contributes via the fields above).
                    if (!tabletParts.isEmpty()) {
                        indexHasher.putBytes(Hashing.combineUnordered(tabletParts).asBytes());
                    }
                    indexParts.add(indexHasher.hash());
                }
            }
        }
        // Combine per-index contributions order-independently across partitions / indexes.
        return Hashing.combineUnordered(indexParts).asLong();
    }

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
            // Nothing left to converge: drop any lingering negative-cache entries so the cache is
            // bounded to the duration of active migrations (and reclaimed after a leader gap), and
            // drop the per-table alignment-attempt memory for the same reason.
            convergenceCache.clear();
            lastAttemptByTable.clear();
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
     * with at least one misaligned tablet (unless that table is latched — see
     * {@link #alignTableIfApplicable}); if and only if every peer is fully aligned, mark
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
        long expectedRangesSig = expectedRangesSignature(expectedRanges);

        boolean allAligned = true;
        for (ColocateTableIndex.GroupId peerGroupId : peers) {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(peerGroupId.dbId);
            if (db == null) {
                continue;
            }
            for (long tableId : colocateTableIndex.getAllTableIds(peerGroupId)) {
                if (!alignTableIfApplicable(db, tableId, expectedRanges, expectedRangesSig,
                        colocateColumnCount, colocateGroupId)) {
                    allAligned = false;
                }
            }
        }

        if (allAligned) {
            // Ranges are settled, so each tablet's expected PACK shard group is now well-defined.
            // Migrate any tablet still in the wrong PACK shard group (reconcilePackPlacement), then
            // confirm StarOS has actually placed every PACK group's shards onto co-resident workers
            // (isPlacementConverged). Flip the group stable ONLY when both hold: the checker never
            // revisits a stable group, so marking it stable while a reassignment is pending, a query
            // failed, or placement is still migrating would leak a permanently mis-placed or
            // non-host-local tablet. Both gates fail closed — any failure or pending work keeps the
            // group unstable for the next tick.
            if (reconcilePackPlacement(colocateTableIndex, peers, expectedRanges, colocateColumnCount)
                    && isPlacementConverged(colocateTableIndex, peers, expectedRanges, colocateGroupId)) {
                colocateTableIndex.markAllGroupsWithSameColocateGroupIdStable(colocateGroupId, true);
                // allAligned means every peer table returned aligned this pass, which already cleared
                // its own lastAttemptByTable entry, so no group-level cleanup is needed here.
                LOG.info("marked colocate group id {} stable across {} peer GroupIds",
                        colocateGroupId, peers.size());
            }
        }
    }

    /**
     * Final stability gate: after range alignment and PACK membership repair, asks StarOS whether
     * every PACK shard group of this colocate group has actually converged onto co-resident workers —
     * placement is done, not merely that membership was assigned. Range alignment + membership are
     * sufficient for query correctness, but the colocate-join optimization only pays off when execution
     * is host-local, so the stable flip waits for StarOS placement to converge.
     *
     * <p>Queried in bounded batches (PACK groups accumulate as colocate ranges split). Best-effort /
     * eventually-consistent: a {@code false} for any group (still migrating), a short response, or a
     * query failure returns {@code false}, leaving the group unstable for the next tick. A group that
     * never converges simply stays unstable and the colocate join falls back to a correct shuffle plan
     * — no livelock, no correctness risk.
     *
     * @return {@code true} iff every PACK shard group reports placement-converged.
     */
    boolean isPlacementConverged(ColocateTableIndex colocateTableIndex,
                                 List<ColocateTableIndex.GroupId> peers,
                                 List<ColocateRange> expectedRanges, long colocateGroupId) {
        List<Long> packGroupIds = expectedRanges.stream()
                .map(ColocateRange::getShardGroupId)
                .collect(Collectors.toList());
        // Negative-cache fast path, before worker-group resolution / any RPC: if any PACK group was
        // recently reported not-converged, the whole colocate group cannot be converged yet (it needs
        // every PACK group converged), so skip the StarOS round-trip this tick. The cache is
        // negative-only, so a flip still rides a fresh all-true read — see ColocateConvergenceCache.
        if (convergenceCache.shouldSkipQuery(packGroupIds)) {
            LOG.debug("colocate group {} skipped placement-convergence query; a PACK group was reported "
                    + "not-converged within the cache TTL", colocateGroupId);
            return false;
        }
        // Both expected failures on this path fail closed (group stays unstable, retried next tick):
        // resolving the worker group can throw ErrorReportException (warehouse has no available compute
        // nodes) and the StarOS query can throw StarClientException. Anything unexpected propagates to
        // runOneCycle, which logs it with a stack trace rather than masking it as "not converged".
        try {
            long workerGroupId = resolveWorkerGroupId(colocateTableIndex, peers);
            StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
            // queryShardGroupStable computes each group's stability server-side, so keep this batch small
            // to bound per-RPC latency; the full result is assembled across repeated calls.
            int batchSize = Math.max(1, Config.tablet_reshard_colocate_checker_convergence_batch_size);
            for (int batchStart = 0; batchStart < packGroupIds.size(); batchStart += batchSize) {
                List<Long> batch = packGroupIds.subList(batchStart,
                        Math.min(batchStart + batchSize, packGroupIds.size()));
                List<Boolean> stable = starOSAgent.queryShardGroupStable(batch, workerGroupId);
                // A size mismatch would let allMatch pass on a subset and flip the group stable while a
                // group is still migrating — fail closed so placement convergence is never faked. An
                // incomplete response is an error, not cached: retried promptly next tick.
                if (stable.size() != batch.size()) {
                    LOG.warn("placement-convergence query returned {} results for {} PACK groups in colocate "
                            + "group {}; staying unstable", stable.size(), batch.size(), colocateGroupId);
                    return false;
                }
                // Feed each fresh per-group result to the negative cache (caches not-converged, drops
                // converged) so a still-migrating group is not re-queried every tick.
                for (int i = 0; i < batch.size(); i++) {
                    convergenceCache.record(batch.get(i), Boolean.TRUE.equals(stable.get(i)));
                }
                if (!stable.stream().allMatch(Boolean.TRUE::equals)) {
                    LOG.debug("colocate group {} not yet placement-converged on StarOS; staying unstable",
                            colocateGroupId);
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            LOG.warn("placement-convergence check failed for colocate group {}; staying unstable",
                    colocateGroupId, e);
            return false;
        }
    }

    /**
     * Worker group to query for placement convergence, via the reshard-module daemon idiom
     * ({@link com.starrocks.server.WarehouseManager#getBackgroundComputeResource(long)}, as used by
     * {@link SplitTabletJob}). Resolves to the default worker group in the open-source build and to a
     * table's import-warehouse worker group in the enterprise build. The convergence query is
     * group-level while a worker group is per-table, so a representative member table is used; a
     * colocate group is expected to live in one warehouse, and any mismatch only fails safe (the group
     * stays unstable → shuffle), never a false stable. {@code isPlacementConverged} only runs after
     * {@code reconcilePackPlacement} settled, which already proved every member table is a live NORMAL
     * OlapTable this pass, and {@code getBackgroundComputeResource} resolves the table's warehouse
     * worker group (table-independent in OSS) — so the first member table is a fine representative.
     */
    private long resolveWorkerGroupId(ColocateTableIndex colocateTableIndex,
                                      List<ColocateTableIndex.GroupId> peers) {
        for (ColocateTableIndex.GroupId peerGroupId : peers) {
            List<Long> tableIds = colocateTableIndex.getAllTableIds(peerGroupId);
            if (!tableIds.isEmpty()) {
                return GlobalStateMgr.getCurrentState().getWarehouseMgr()
                        .getBackgroundComputeResource(tableIds.get(0)).getWorkerGroupId();
            }
        }
        return StarOSAgent.DEFAULT_WORKER_GROUP_ID;
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
        return reconcileTabletPackPlacement(tabletIdToRange, expectedRanges, colocateColumnCount,
                db.getFullName() + "." + table.getName());
    }

    /**
     * Reads the actual PACK shard-group membership of the given tablets from StarOS in bounded
     * batches, finds the ones whose membership disagrees with the {@link ColocateRange} their range
     * belongs to, and reassigns each into its expected PACK shard group with the minimal add/remove
     * delta. Shared by the placement backstop ({@link #reconcileTablePackPlacement}) and the
     * post-publish split path ({@code SplitTabletJob}).
     *
     * <p>Best-effort per tablet. Catches the expected StarOS checked failures — the batched
     * {@link StarOSAgent#getShardInfo} {@link StarClientException} and the per-tablet
     * {@link StarOSAgent#reassignShardGroups} {@link DdlException} — but is intentionally not
     * blanket-wrapped, so an unexpected bug still surfaces in the caller's diagnostics. Membership
     * ({@code group_ids}) is central StarMgr state, independent of the worker group, so the read uses
     * {@link StarOSAgent#DEFAULT_WORKER_GROUP_ID}.
     *
     * @return {@code true} iff nothing needed repair: the membership read covered every tablet and no
     *         tablet was misplaced. A StarOS query failure, an incomplete response (a requested tablet
     *         missing from the result — treated as an unread membership, never as "no groups", so it
     *         cannot turn into a bogus add-only reassignment), or any misplaced tablet (reassignment
     *         issued, confirmed by a re-read) ⇒ {@code false}.
     */
    static boolean reconcileTabletPackPlacement(Map<Long, Range<Tuple>> tabletIdToRange,
                                                List<ColocateRange> expectedRanges, int colocateColumnCount,
                                                String logContext) {
        StarOSAgent starOSAgent = GlobalStateMgr.getCurrentState().getStarOSAgent();
        List<Long> tabletIds = new ArrayList<>(tabletIdToRange.keySet());
        Map<Long, List<Long>> tabletIdToGroupIds = new HashMap<>();
        // Query membership in bounded batches so one very large table cannot produce an oversized RPC.
        int batchSize = Math.max(1, Config.tablet_reshard_colocate_checker_membership_batch_size);
        for (int batchStart = 0; batchStart < tabletIds.size(); batchStart += batchSize) {
            List<Long> batch = tabletIds.subList(batchStart,
                    Math.min(batchStart + batchSize, tabletIds.size()));
            try {
                for (ShardInfo shardInfo : starOSAgent.getShardInfo(batch, StarOSAgent.DEFAULT_WORKER_GROUP_ID)) {
                    tabletIdToGroupIds.put(shardInfo.getShardId(), shardInfo.getGroupIdsList());
                }
            } catch (StarClientException e) {
                LOG.warn("failed to query shard membership for PACK reconcile of {}: {}",
                        logContext, e.getMessage());
                return false;
            }
        }
        // An incomplete response is an unread membership, not "tablet has no groups": treat it as a
        // failed read and retry next cycle, so a missing StarOS entry cannot become a bogus repair.
        if (!tabletIdToGroupIds.keySet().containsAll(tabletIdToRange.keySet())) {
            LOG.warn("incomplete shard membership response during PACK reconcile of {}; retrying", logContext);
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
                LOG.info("reassigned tablet {} from PACK shard group {} to {} in {}",
                        misplaced.tabletId(), misplaced.currentPackGroupId(), misplaced.expectedPackGroupId(),
                        logContext);
            } catch (DdlException e) {
                LOG.warn("failed to reassign tablet {} to PACK shard group {} in {}: {}",
                        misplaced.tabletId(), misplaced.expectedPackGroupId(), logContext, e.getMessage());
            }
        }
        // Settled only when nothing needed repair; if reassignments were issued, stay unstable so
        // the next cycle re-reads and confirms membership before the group is flipped stable.
        return misplacedTablets.isEmpty();
    }

    /**
     * Per-table dispatch for {@link #processGroup}: looks up the table, filters out non-OlapTable
     * entries (still considered "aligned" — alignment isn't applicable), defers tables not in
     * {@code NORMAL} state, and otherwise applies the per-table convergence latch. Split/merge is
     * deterministic, so once a completed alignment attempt made no progress on this exact table layout,
     * re-issuing it would just churn tablets (the self-sustaining storm); the latch suppresses that
     * re-issue until the table's layout/data changes (or a bounded number of retries after a transient
     * abort). A transient failure (lookup / job-admission throw) records nothing, so the table is simply
     * retried next cycle — it can never suppress a peer, because the latch is per table.
     *
     * @return {@code true} iff the table is already aligned (or not an OlapTable) — no obstacle to
     *         marking the colocate group stable. {@code false} when work is still needed (misaligned,
     *         in-flight alter, latched, or a transient failure).
     */
    private boolean alignTableIfApplicable(Database db, long tableId, List<ColocateRange> expectedRanges,
                                            long expectedRangesSig, int colocateColumnCount, long colocateGroupId) {
        Table fetchedTable;
        try {
            fetchedTable = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), tableId);
        } catch (Exception e) {
            // Transient lookup failure: no latch entry recorded, so this table is retried next cycle.
            LOG.debug("table {} lookup failed in db {}, skipping", tableId, db.getId(), e);
            return false;
        }
        if (!(fetchedTable instanceof OlapTable olapTable)) {
            return true;
        }
        if (olapTable.getState() != OlapTable.OlapTableState.NORMAL) {
            // In-flight alter / reshard job — skip this cycle, revisit next. Avoids the
            // SplitTabletJob.setTableState(NORMAL -> TABLET_RESHARD) race when two jobs target the
            // same table. Leaves the table's latch entry untouched.
            return false;
        }
        try {
            long signature = tableConvergenceSignature(db, olapTable, expectedRangesSig);
            TableAlignmentAttempt prev = lastAttemptByTable.get(tableId);
            AlignmentDecision decision =
                    decideAlignment(prev, signature, prev != null && isJobAborted(prev.jobId));
            if (!decision.fire()) {
                // A completed attempt on an unchanged layout made no progress: don't re-issue until the
                // table's layout/data changes. The table stays misaligned, so the group stays unstable
                // and colocate joins fall back to a correct shuffle plan. Log once per stuck state.
                if (prev != null && !prev.suppressionLogged && isJobSettled(prev.jobId)) {
                    LOG.warn("colocate table {}.{} (group {}) alignment made no progress on an unchanged "
                            + "layout; suppressing further alignment jobs until its layout or data changes. "
                            + "The group stays unstable and colocate joins fall back to shuffle.",
                            db.getFullName(), olapTable.getName(), colocateGroupId);
                    prev.suppressionLogged = true;
                }
                return false;
            }
            return fireAlignmentIfMisaligned(db, olapTable, expectedRanges, colocateColumnCount,
                    signature, decision.nextAbortRetries());
        } catch (Exception e) {
            // Transient failure (e.g. job-admission throw): no latch entry recorded, retried next cycle.
            LOG.warn("alignment failed for table {}.{} in colocate group id {}",
                    db.getFullName(), olapTable.getName(), colocateGroupId, e);
            return false;
        }
    }

    /**
     * Inspects every visible materialized index in every physical partition of {@code table}; if every
     * tablet is already range-aligned, clears the table's latch entry and reports aligned. Otherwise
     * builds the per-tablet forced-boundaries map, fires a single alignment {@link SplitTabletJob}, and
     * records the {@code signature}/jobId so an unchanged next cycle latches.
     *
     * @return {@code true} iff every tablet in every visible index was already aligned (no job fired);
     *         {@code false} otherwise (a job was fired, or an index is misaligned with no splittable
     *         boundary this cycle).
     */
    private boolean fireAlignmentIfMisaligned(Database db, OlapTable table, List<ColocateRange> expectedRanges,
                                              int colocateColumnCount, long signature, int nextAbortRetries)
            throws StarRocksException {
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
                    // livelock. Resolve per-index here. Use getMetaId() (not getId()) — the
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
            // Aligned (or nothing splittable): drop any latch entry so a future misalignment re-arms.
            lastAttemptByTable.remove(table.getId());
            return alignedSoFar;
        }

        // The factory now only builds local state and journals the job; the StarOS shard
        // creation runs in SplitTabletJob.runPendingJob (after the journal write), so a
        // leader demotion at this point cannot leak external shards.
        TabletReshardJob job = SplitTabletJobFactory.forColocateAlignment(db, table, alignmentMap);
        GlobalStateMgr.getCurrentState().getTabletReshardJobMgr().addTabletReshardJob(job);
        lastAttemptByTable.put(table.getId(),
                new TableAlignmentAttempt(signature, job.getJobId(), nextAbortRetries));
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
