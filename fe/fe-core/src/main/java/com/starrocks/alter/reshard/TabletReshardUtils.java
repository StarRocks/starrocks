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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.common.StarRocksException;
import com.starrocks.lake.LakeTablet;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.function.Supplier;

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
     * Target tablet size for one index: the steady-state target, or the size that would give the index
     * one tablet per slot the bound allows, whichever is smaller, floored so a nearly empty index is not
     * carved into slivers.
     *
     * <p>This target is a function of the index's SIZE, not of how many tablets it has: splitting an
     * index does not move it. It rises as the index grows and meets the steady-state target once the
     * index holds {@code bound} targets' worth of data, after which the expression collapses to the
     * steady-state target and the two rules agree. What ends the adaptive phase earlier, and usually
     * does, is the index reaching the bound -- the headroom test in
     * {@code SplitTabletJobFactory#planIndexSplits}, which owns that count.
     *
     * <p>The floor is clamped to the target. That clamp is what makes
     * {@code tablet_reshard_min_split_size >= tablet_reshard_target_size} switch the adaptive behaviour
     * off: the floor becomes the target, and the whole expression collapses to the target. Without the
     * clamp, a minimum above the target would raise the effective target above it and change the
     * steady-state rule too.
     *
     * <p>{@code bound} sits at or below the auto-merge parallelism floor -- it is that floor clamped
     * to the node count, so a single-node warehouse gets 1 where the floor is 2. Merge acts strictly
     * above the floor, so an index this rule widens to the bound is never one merge would immediately
     * narrow again. A non-positive bound (an unresolved warehouse) leaves the steady-state target
     * untouched.
     */
    public static long adaptiveTargetSize(long indexDataSize, long steadyTargetSize, int bound) {
        long configuredFloor = Config.tablet_reshard_min_split_size;
        // A non-positive minimum has no validator on it, and without a floor this expression would let
        // an index be carved to the split-count cap. Treat it as "no adaptive target", not "no floor".
        if (bound <= 0 || steadyTargetSize <= 0 || configuredFloor <= 0) {
            return steadyTargetSize;
        }
        long floor = Math.min(configuredFloor, steadyTargetSize);
        return Math.max(floor, Math.min(steadyTargetSize, indexDataSize / bound));
    }

    /**
     * Child count for the adaptive target: {@code floor(dataSize / target)} once a tablet is worth at
     * least two of them, and 1 otherwise.
     *
     * <p>Requiring two whole targets rather than the size rule's one and a half is what keeps every
     * child worth at least a target: a tablet of 1.6 targets would otherwise round to two children of
     * 0.8 each, more tablets than its share and each still under size.
     *
     * <p>Flooring bounds the children this asks for, but NOT the width of the index. The tablets it
     * declines still occupy slots and are absent from that sum, so a skewed index would be widened
     * past its bound -- {@code 8G, 1G, 1G} against a bound of four lands on five. Capping each split
     * by the slots the index has left is therefore load bearing, and belongs to the caller that knows
     * them: see {@code SplitTabletJobFactory#planIndexSplits}.
     */
    public static int adaptiveSplitCount(long dataSize, long target) {
        if (target <= 0 || dataSize / 2 < target) {
            return 1;
        }
        return (int) Math.min((long) Config.tablet_reshard_max_split_count, dataSize / target);
    }

    /*
     * Return value > 1 if need split
     * Return value = 1 if not need split
     * Return value <= 0 if exception occurs
     */
    public static int calcSplitCount(long dataSize, long targetSize) {
        return calcSplitCount(dataSize, targetSize, Config.tablet_reshard_max_split_count);
    }

    /**
     * As {@link #calcSplitCount(long, long)}, but with an explicit fan-out cap. Callers that know the
     * table pass {@link #effectiveMaxSplitCount}, which tightens the cap for a split that drags a full
     * UNSHARE rewrite behind it.
     */
    public static int calcSplitCount(long dataSize, long targetSize, int maxSplitCount) {
        if (targetSize <= 0) {
            // A value less than 0 indicates the specified split count,
            // for internal testing.
            long splitCount = -targetSize;
            if (splitCount > maxSplitCount) {
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
        // No lower bound needed: dataSize has already passed ceil(1.5 * targetSize) above, so the
        // quotient is at least 1 and the remainder is at least half the target, which rounds up. Two is
        // the smallest answer this can produce.
        long n = (remainder >= halfTargetCeil) ? quotient + 1 : quotient;
        return (int) Math.min((long) maxSplitCount, n);
    }

    /**
     * Whether a split of this table drags a full UNSHARE rewrite behind it.
     *
     * <p>True for a range-distributed primary-key table whose ORDER BY key differs from the primary
     * key: its split children cannot range-filter the parent's shared segments, so UNSHARE compaction
     * rewrites every one of them wholesale before the split completes. That rewrite is what the
     * tablet_reshard_orderby_* knobs bound. An ordinary split just leaves the children sharing the
     * parent's segments and needs no such bound.
     */
    public static boolean splitRewritesEveryShard(OlapTable table) {
        if (table == null || !table.isFileBundling()) {
            return false;
        }
        try {
            return MetaUtils.hasSeparateSortKey(table, table.getBaseIndexMetaId());
        } catch (IllegalArgumentException e) {
            // Base index meta went away underneath us; treat as an ordinary split rather than throwing
            // from a scheduling decision.
            return false;
        }
    }

    /** Per-tablet split fan-out cap for this table. */
    public static int effectiveMaxSplitCount(OlapTable table) {
        int cap = Config.tablet_reshard_max_split_count;
        int orderByCap = Config.tablet_reshard_orderby_max_split_count;
        if (orderByCap > 1 && splitRewritesEveryShard(table)) {
            cap = Math.min(cap, orderByCap);
        }
        return cap;
    }

    /**
     * Max source tablets one split job may take for this table. Unbounded unless the split drags an
     * UNSHARE rewrite behind it, in which case it defaults to the warehouse's compute-node count so a
     * single UNSHARE spreads over the cluster instead of monopolizing one partition's compaction slot.
     *
     * <p>The resource is a supplier because most calls never read it: an ordinary split answers
     * {@code Integer.MAX_VALUE} without looking, and a configured bound answers from the config.
     * Resolving one eagerly would send every split through the warehouse availability probe -- a
     * StarMgr round trip -- for an answer that does not depend on it, and would let a StarMgr blip
     * fail a split that never needed the warehouse at all.
     */
    public static int maxSplitTabletsPerJob(OlapTable table, Supplier<ComputeResource> computeResource) {
        if (!splitRewritesEveryShard(table)) {
            return Integer.MAX_VALUE;
        }
        int configured = Config.tablet_reshard_orderby_max_split_tablets_per_job;
        return configured > 0 ? configured : computeNodeCount(computeResource.get());
    }

    /**
     * Total number of compute nodes provisioned in the given warehouse resource (>= 1). Uses
     * getAllComputeNodeIds (NOT the alive/blocklist-filtered set) so a transient node restart or
     * blocklist entry does not change reshard layout decisions. Shared by pre-split (which sizes a
     * new partition to at least this many tablets) and auto-merge (whose floor is derived from it),
     * keeping the two consistent. Throws ErrorReportException (unchecked) if the warehouse resource
     * no longer exists.
     */
    public static int computeNodeCount(ComputeResource computeResource) {
        return Math.max(1, GlobalStateMgr.getCurrentState().getWarehouseMgr()
                .getAllComputeNodeIds(computeResource).size());
    }

    /**
     * Minimum number of tablets an index must keep so steady-state auto-merge does not collapse the
     * tablet count below the parallelism level pre-split established. Pure clamp logic, split out for
     * testability. When maxSplitCount < 2 there is no multi-tablet pre-split layout to preserve
     * (TabletPreSplitCoordinator#selectTabletCount requires maxSplitCount >= 2), so no floor applies.
     *
     * <p>Public because TabletStatMgr derives the auto-merge floor from a node count it resolves once
     * per scan.
     */
    @VisibleForTesting
    public static int parallelismFloor(int computeNodeCount, int maxSplitCount) {
        if (maxSplitCount < 2) {
            return 1;
        }
        return Math.max(2, Math.min(computeNodeCount, maxSplitCount));
    }

    /**
     * Tablet count the early-split rule may not carry an index past, or 0 when the node count could
     * not be resolved. It is the auto-merge parallelism floor clamped to the node count, so it is at
     * or below that floor and never above it -- merge acts strictly above the floor, so an index this
     * rule widened is never one merge would immediately narrow again. The clamp is what keeps a
     * single-node warehouse, whose floor is 2, from widening for a parallelism it does not have; the
     * floor itself is already capped by tablet_reshard_max_split_count, so a warehouse with more
     * nodes than that cap bounds at the cap rather than at its node count.
     */
    public static int adaptiveSplitBound(int computeNodeCount) {
        return adaptiveSplitBound(computeNodeCount, Config.tablet_reshard_max_split_count);
    }

    /**
     * As above, against a caller-supplied split cap. A caller that also derives the auto-merge floor
     * must take one sample of {@code tablet_reshard_max_split_count} and pass it to both: the config is
     * mutable, and a change landing between two reads yields a floor above this bound -- which is the
     * overlap that lets one scan emit a merge signal and an adaptive-split signal for the same index.
     */
    public static int adaptiveSplitBound(int computeNodeCount, int maxSplitCount) {
        return computeNodeCount == 0 ? 0
                : Math.min(computeNodeCount, parallelismFloor(computeNodeCount, maxSplitCount));
    }

    /**
     * Parallelism floor for a table's auto-merge. getBackgroundComputeResource(tableId) resolves the
     * table's import warehouse in the enterprise build and the default warehouse in the open-source
     * build. Returns a value in [1, tablet_reshard_max_split_count].
     */
    public static int computeParallelismFloor(long tableId) {
        return parallelismFloor(computeNodeCountForTable(tableId), Config.tablet_reshard_max_split_count);
    }

    /**
     * The single resolution the auto-merge floor and the adaptive-split bound share. Keeping them on
     * one path is what makes them consistent within a decision, which is what stops the two rules
     * pulling one index back and forth.
     */
    private static int computeNodeCountForTable(long tableId) {
        return computeNodeCount(GlobalStateMgr.getCurrentState().getWarehouseMgr()
                .getBackgroundComputeResource(tableId));
    }

    /**
     * Bound for a table's adaptive split, propagating a resolution failure rather than swallowing it.
     *
     * <p>A planner must not read "warehouse temporarily unavailable" as "this index needs nothing".
     * Falling back to a zero bound there yields an empty plan, which the caller is entitled to treat
     * as deterministic and latch -- and since the layout, the configuration and the signal are all
     * unchanged, the fingerprint would not move again and the table would stay suppressed for good.
     * The scan is the one caller that should degrade instead: it has a whole cluster to walk.
     */
    public static int adaptiveSplitBoundForTable(long tableId) {
        return adaptiveSplitBound(computeNodeCountForTable(tableId));
    }

    /**
     * Compute-node count for a table's background (reshard) warehouse; {@code 0} when the warehouse is
     * unknown or unavailable, so a caller can fall back to today's behavior.
     *
     * <p>Resolves through {@code WarehouseManager#getBackgroundComputeResource}, NOT the probe-free
     * variant: that probe is load bearing — it is why an auto-merge job is rejected before admission
     * when the warehouse has no usable worker. Using one resolution for both the adaptive-split bound
     * and the auto-merge floor is also what keeps them consistent within a decision, so callers that
     * need both must derive them from a single call.
     */
    public static int safeComputeNodeCountForTable(long tableId) {
        try {
            return computeNodeCountForTable(tableId);
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
