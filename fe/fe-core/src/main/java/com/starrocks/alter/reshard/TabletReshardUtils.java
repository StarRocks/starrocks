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

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class TabletReshardUtils {
    private static final Logger LOG = LogManager.getLogger(TabletReshardUtils.class);

    private static final int MAX_TABLETS_IN_ERROR_MESSAGE = 20;

    // Reshard size invariants — DO NOT change individually.
    //
    // Let T = Config.tablet_reshard_target_size.
    //   splitThreshold     = ceil(1.5 * T)   (split fires when dataSize >= splitThreshold)
    //   mergePairThreshold = ceil(0.8 * T)   (merge fires when pair sum < mergePairThreshold)
    //   mergeGroupCap      = T               (merge groups packed up to mergeGroupCap)
    //
    // The thresholds use exclusive ceilings so the strict-`<` and strict-`>=`
    // comparisons accept the full half-open intervals implied by the design
    // (pair sum < 0.8*T, dataSize >= 1.5*T) for non-integer multiples too.
    //
    // Convergence requires (assuming BE row-count split approximately preserves byte balance):
    //   2 * (1 - 0.5 / 1.5) > 4/5    // post-split min piece pair > merge pair threshold
    //   1                  < 1.5     // merge group cap < split threshold
    //
    // mergePairThreshold and mergeGroupCap are overflow-safe for any positive long T.
    // splitThreshold = T + T/2 + (T & 1) overflows when T > floor(2/3 * Long.MAX_VALUE)
    // (~6.15 EB). calcSplitCount() detects that exact boundary algebraically and falls
    // back to "no split" for inputs above it, so the wrap-around can't produce a
    // bogus positive split count.

    @VisibleForTesting
    static long splitThreshold(long target) {
        // ceil(1.5T) = T + ceil(T/2). Caller must check splitThresholdOverflows(target) first.
        return target + target / 2 + (target & 1L);
    }

    /**
     * True iff splitThreshold(target) would overflow long for the given non-negative target.
     * Algebraically: T + T/2 + (T & 1) > Long.MAX_VALUE.
     * Computed via the rearrangement T > Long.MAX_VALUE - T/2 - (T & 1), which never
     * underflows because T/2 and (T & 1) are themselves non-negative and <= Long.MAX_VALUE.
     */
    @VisibleForTesting
    static boolean splitThresholdOverflows(long target) {
        return target > Long.MAX_VALUE - target / 2 - (target & 1L);
    }

    @VisibleForTesting
    static long mergePairThreshold(long target) {
        // ceil(0.8T) = (T/5)*4 + ceil(((T%5)*4)/5). Avoids T*4 overflow.
        return (target / 5) * 4 + (((target % 5) * 4) + 4) / 5;
    }

    @VisibleForTesting
    static long mergeGroupCap(long target) {
        return target;
    }

    public static boolean needSplit(long dataSize) {
        // Reuse calcSplitCount so we never trigger when no actual split would be produced
        // (e.g., tablet_reshard_max_split_count <= 1).
        return calcSplitCount(dataSize, Config.tablet_reshard_target_size) > 1;
    }

    public static boolean needMerge(long minAdjacentTabletPairSize) {
        long target = Config.tablet_reshard_target_size;
        if (target <= 0) {
            return false;
        }
        return minAdjacentTabletPairSize < mergePairThreshold(target);
    }

    /**
     * Target tablet size while an index sits below the cluster's usable parallelism. Clamped to the
     * steady-state target so it can never exceed it: when an operator sets the minimum above the
     * target, the early threshold collapses onto the normal one and the rule becomes a no-op.
     */
    @VisibleForTesting
    static long earlySplitTargetSize() {
        return Math.min(Config.tablet_reshard_min_split_size, Config.tablet_reshard_target_size);
    }

    /**
     * Coarse "could an early split fire at this size" gate, mirroring {@link #needSplit}. Used to admit
     * a reshard candidate; the split job factory re-decides authoritatively per index.
     */
    public static boolean needEarlySplit(long dataSize) {
        if (!Config.tablet_reshard_enable_early_split) {
            return false;
        }
        long target = earlySplitTargetSize();
        // calcSplitCount reads a non-positive target as a FORCED split count (-8 means "split into 8"),
        // and tablet_reshard_min_split_size has no positivity validator, so guard it here.
        return target > 0 && calcSplitCount(dataSize, target) > 1;
    }

    /**
     * Highest tablet count the early rule may drive one index to.
     *
     * <p>MUST stay at or below the auto-merge parallelism floor. Merge acts on
     * {@code tabletCount > floor} and early split on {@code tabletCount < ceiling}, so
     * {@code ceiling <= floor} keeps the two regions disjoint and makes split/merge oscillation
     * impossible for a given node-count sample. Deriving it from {@link #parallelismFloor} keeps them
     * in lockstep even when {@code maxSplitCount < computeNodeCount}, where a plain node-count ceiling
     * would overlap the merge region. The outer min() also keeps a single-node warehouse (floor 2,
     * count 1) from splitting at all.
     */
    public static int earlySplitCeiling(int computeNodeCount, int maxSplitCount) {
        return Math.min(computeNodeCount, parallelismFloor(computeNodeCount, maxSplitCount));
    }

    /*
     * Return value > 1 if need split
     * Return value = 1 if not need split
     * Return value <= 0 if exception occurs
     */
    public static int calcSplitCount(long dataSize, long targetSize) {
        if (targetSize <= 0) {
            // A value less than 0 indicates the specified split count,
            // for internal testing.
            long splitCount = -targetSize;
            if (splitCount > Config.tablet_reshard_max_split_count) {
                return 0;
            }
            return (int) splitCount;
        }

        if (splitThresholdOverflows(targetSize)) {
            // ceil(1.5 * targetSize) does not fit in long; no possible dataSize can
            // satisfy the threshold so treat as "no split" instead of letting the
            // lower-bounded Math.max(2, ...) below produce a bogus positive count.
            return 1;
        }

        if (dataSize < splitThreshold(targetSize)) {
            return 1;
        }

        // round-to-nearest, lower-bounded at 2, fully overflow-safe
        long quotient = dataSize / targetSize;
        long remainder = dataSize - quotient * targetSize;
        long halfTargetCeil = targetSize / 2 + (targetSize & 1L);
        long n = (remainder >= halfTargetCeil) ? quotient + 1 : quotient;
        n = Math.max(2L, n);
        return (int) Math.min((long) Config.tablet_reshard_max_split_count, n);
    }

    /**
     * Number of compute nodes PROVISIONED in FE for the resource's worker group (>= 1).
     *
     * <p>Counts both Backends and ComputeNodes: {@code Backend extends ComputeNode}, a shared-data
     * cluster can be made entirely of BEs acting as workers, and HeartbeatMgr registers both as StarOS
     * workers. Deliberately NOT filtered by liveness, so a transient node restart or blocklist entry
     * does not change reshard layout decisions.
     *
     * <p>FE is the source of this mapping, not a consumer of it: FE assigns each node's
     * warehouse/worker group, persists it, and feeds that value into StarOS via addWorker(). The two
     * can diverge briefly in either direction — a node added but not yet registered is counted here and
     * not by StarOS; a node dropped after losing its starlet port is still held by StarOS and not
     * counted here until StarMgrMetaSyncer reconciles. Both are bounded by the count itself, and both
     * move the early-split ceiling and the auto-merge floor together, so their disjointness holds.
     *
     * <p>Shared by pre-split (which sizes a new partition to at least this many tablets), auto-merge
     * (whose floor derives from it) and early split (whose ceiling does), keeping all three consistent.
     */
    public static int computeNodeCount(ComputeResource computeResource) {
        long workerGroupId = computeResource.getWorkerGroupId();
        return Math.max(1, (int) GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo()
                .backendAndComputeNodeStream()
                .filter(node -> node.getWorkerGroupId() == workerGroupId)
                .count());
    }

    /**
     * Minimum number of tablets an index must keep so steady-state auto-merge does not collapse the
     * tablet count below the parallelism level pre-split established. Pure clamp logic, split out for
     * testability. When maxSplitCount < 2 there is no multi-tablet pre-split layout to preserve
     * (TabletPreSplitCoordinator#selectTabletCount requires maxSplitCount >= 2), so no floor applies.
     *
     * <p>Public because TabletStatMgr derives the auto-merge floor from a node count it resolves once
     * per scan, and {@link #earlySplitCeiling} must stay in lockstep with it.
     */
    @VisibleForTesting
    public static int parallelismFloor(int computeNodeCount, int maxSplitCount) {
        if (maxSplitCount < 2) {
            return 1;
        }
        return Math.max(2, Math.min(computeNodeCount, maxSplitCount));
    }

    /**
     * Parallelism floor for a table's auto-merge. getBackgroundComputeResource(tableId) resolves the
     * table's import warehouse in the enterprise build and the default warehouse in the open-source
     * build. Returns a value in [1, tablet_reshard_max_split_count].
     */
    public static int computeParallelismFloor(long tableId) {
        ComputeResource computeResource =
                GlobalStateMgr.getCurrentState().getWarehouseMgr().getBackgroundComputeResource(tableId);
        return parallelismFloor(computeNodeCount(computeResource), Config.tablet_reshard_max_split_count);
    }

    /**
     * Compute-node count for a table's background (reshard) warehouse; {@code 0} when the warehouse is
     * unknown or unavailable, so a caller can fall back to today's behavior.
     *
     * <p>Resolves through {@code WarehouseManager#getBackgroundComputeResource}, NOT the probe-free
     * variant: that probe is load bearing — it is why an auto-merge job is rejected before admission
     * when the warehouse has no usable worker. Using one resolution for both the early-split ceiling
     * and the auto-merge floor is also what keeps them consistent within a decision, so callers that
     * need both must derive them from a single call.
     */
    public static int safeComputeNodeCountForTable(long tableId) {
        try {
            return computeNodeCount(GlobalStateMgr.getCurrentState().getWarehouseMgr()
                    .getBackgroundComputeResource(tableId));
        } catch (RuntimeException e) {
            LOG.warn("Compute node count unavailable for table {}; reshard sizing will fall back.", tableId, e);
            return 0;
        }
    }

    // Min async vector-index build watermark over a reshard op's source tablets, so a reshard
    // child/merged tablet inherits the build frontier of its inputs (split: the single parent;
    // merge: the min across sources; identical: carried over) instead of resetting to 0. This
    // mirrors the BE merge_tablet reconciliation and keeps the async build scheduler from a
    // redundant full re-scan. Harmless (0) for non-vector-index tables.
    public static long minVectorIndexBuiltVersion(MaterializedIndex oldIndex, List<Long> oldTabletIds) {
        long min = Long.MAX_VALUE;
        for (long oldTabletId : oldTabletIds) {
            Tablet tablet = oldIndex.getTablet(oldTabletId);
            long v = (tablet instanceof LakeTablet) ? ((LakeTablet) tablet).getVectorIndexBuiltVersion() : 0L;
            min = Math.min(min, v);
        }
        return min == Long.MAX_VALUE ? 0L : min;
    }

    /**
     * Rejects a reshard whose source tablet lives in a materialized index that a previous reshard has
     * already superseded.
     *
     * <p>Every other admission check passes for such a tablet: the superseded index is still reachable
     * through {@link PhysicalPartition#getIndex}, its state is still {@code NORMAL}, the tablet is
     * still one of its tablets, and {@code TabletInvertedIndex} still maps the tablet id. Only the
     * partition's index-meta -> index-id chain records that the index has been replaced. Admitting the
     * job anyway is unrecoverable in practice: the publish resolves no live tablet
     * ("Fail to publish version for tablets:[]"), and because a publish failure is only retried, the
     * job stays in {@code RUNNING} forever with an empty error message.
     *
     * <p>This is easy to hit by accident because {@code SHOW TABLET FROM <table>} keeps listing the
     * superseded tablets alongside the live ones -- after a split the old parent is still the first
     * row -- so a script that takes a tablet id from that output can feed a stale one straight back
     * into {@code ALTER TABLE ... SPLIT/MERGE TABLET}.
     */
    public static void checkIndexNotSuperseded(PhysicalPartition physicalPartition, MaterializedIndex index,
                                               long tabletId, String dbName, String tableName)
            throws StarRocksException {
        MaterializedIndex latest = physicalPartition.getLatestIndex(index.getMetaId());
        if (latest != null && latest.getId() != index.getId()) {
            throw new StarRocksException("Tablet " + tabletId + " belongs to materialized index "
                    + index.getId() + ", which has already been superseded by index " + latest.getId()
                    + " in physical partition " + physicalPartition.getId() + " in table "
                    + dbName + '.' + tableName + ". It is no longer part of the table; the current"
                    + " tablets of that partition are " + describeTablets(latest));
        }
    }

    /**
     * Admission-time counterpart of {@link #checkIndexNotSuperseded}: an index a reshard job was
     * built against must still be the latest version of its index meta when the job reserves the
     * table.
     *
     * <p>The creation-time check cannot cover this. A job factory releases the table lock before its
     * job is handed to {@code TabletReshardJobMgr#addTabletReshardJob}, so another reshard job on the
     * same table can complete in that gap and install a new version of the very index this job is
     * about to reshard -- turning a source index that was live at creation into a superseded one.
     * The consequences are the same as admitting a superseded tablet in the first place, plus one
     * more: {@code addMaterializedIndex} would fail its "index id already exists" precondition past
     * the job's no-abort boundary.
     *
     * <p>Takes ids rather than a {@link MaterializedIndex} because the index may be gone from the
     * partition altogether by now, which this also rejects -- resharding an index that no longer
     * exists produces nothing, and installing its successor would fail the same precondition.
     */
    public static void checkIndexStillLatest(PhysicalPartition physicalPartition, long indexId, long indexMetaId,
                                             String dbName, String tableName) throws StarRocksException {
        MaterializedIndex latest = physicalPartition.getLatestIndex(indexMetaId);
        if (latest != null && latest.getId() == indexId) {
            return;
        }
        String indexDesc = "materialized index " + indexId + " in physical partition "
                + physicalPartition.getId() + " in table " + dbName + '.' + tableName;
        if (latest == null) {
            throw new StarRocksException("Cannot reshard " + indexDesc
                    + ": it was removed after this tablet reshard job was created");
        }
        throw new StarRocksException("Cannot reshard " + indexDesc
                + ": it has already been superseded by index " + latest.getId()
                + " since this tablet reshard job was created. The current tablets of that partition are "
                + describeTablets(latest));
    }

    // The replacement ids, spelled out in the message rather than pointed at with SHOW PROC: the
    // partition-level proc path requires system-level OPERATE, while triggering this rejection only
    // requires ALTER on the table, so a user who can hit the error may not be able to run the command
    // that would resolve it. Truncated because a partition can hold a lot of tablets and this goes into
    // an error string.
    private static String describeTablets(MaterializedIndex index) {
        List<Tablet> tablets = index.getTablets();
        StringBuilder sb = new StringBuilder("[");
        int shown = Math.min(tablets.size(), MAX_TABLETS_IN_ERROR_MESSAGE);
        for (int i = 0; i < shown; ++i) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(tablets.get(i).getId());
        }
        sb.append(']');
        if (shown < tablets.size()) {
            sb.append(" (first ").append(shown).append(" of ").append(tablets.size()).append(')');
        }
        return sb.toString();
    }
}
