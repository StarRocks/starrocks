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

package com.starrocks.alter.reshard.presplit;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;

import java.util.List;
import java.util.Objects;

/**
 * FE-side orchestrator for Sample-Based Tablet Pre-Split. The integrating
 * load path (INSERT-from-FILES, Broker Load) calls
 * {@link #maybeAct(Database, OlapTable, long, ScanContext)} after the scan
 * side is resolved and before the load coordinator runs.
 *
 * <p>Currently a pure eligibility gate: every check that fails produces a
 * specific {@link SkipReason} for downstream bvar labels. A passing gate
 * returns {@link PreSplitOutcome.Eligible}; later stages of this feature
 * will extend the outcome shape with planning and submission results.
 */
public final class TabletPreSplitCoordinator {

    private TabletPreSplitCoordinator() {
    }

    /**
     * @param db                   table's database (carried through for the submission stage).
     * @param table                target table.
     * @param physicalPartitionId  load-target physical partition.
     * @param scanContext          integration-point scan context (used by the sampling stage).
     */
    public static PreSplitOutcome maybeAct(
            Database db, OlapTable table, long physicalPartitionId, ScanContext scanContext) {
        Objects.requireNonNull(db, "db");
        Objects.requireNonNull(table, "table");
        Objects.requireNonNull(scanContext, "scanContext");

        SkipReason gateReason = checkConfigAndSession();
        if (gateReason != null) {
            return new PreSplitOutcome.Skipped(gateReason);
        }
        if (!table.isRangeDistribution()) {
            return new PreSplitOutcome.Skipped(SkipReason.NOT_RANGE_DISTRIBUTION);
        }
        if (table.getState() != OlapTable.OlapTableState.NORMAL) {
            return new PreSplitOutcome.Skipped(SkipReason.TABLE_NOT_NORMAL);
        }
        if (table.getVisibleIndexMetas().size() != 1) {
            return new PreSplitOutcome.Skipped(SkipReason.HAS_MATERIALIZED_VIEW_OR_ROLLUP);
        }
        if (!isFirstSortKeyColumnSupported(table)) {
            return new PreSplitOutcome.Skipped(SkipReason.UNSUPPORTED_SORT_KEY);
        }

        PhysicalPartition partition = table.getPhysicalPartition(physicalPartitionId);
        if (partition == null) {
            return new PreSplitOutcome.Skipped(SkipReason.METADATA_NOT_RESOLVED);
        }
        MaterializedIndex baseIndex = partition.getIndex(table.getBaseIndexMetaId());
        if (baseIndex == null) {
            return new PreSplitOutcome.Skipped(SkipReason.METADATA_NOT_RESOLVED);
        }
        if (baseIndex.getTablets().size() != 1) {
            return new PreSplitOutcome.Skipped(SkipReason.MULTIPLE_BASE_INDEX_TABLETS);
        }
        if (baseIndex.getRowCount() > 0) {
            return new PreSplitOutcome.Skipped(SkipReason.PARTITION_NOT_EMPTY);
        }

        return new PreSplitOutcome.Eligible();
    }

    /**
     * Either Config flag being on is enough — the caller's integration commit
     * decides which Config its load path reads. Returns {@code null} when both
     * the Config gate and the session opt-out allow pre-split.
     */
    private static SkipReason checkConfigAndSession() {
        if (!Config.enable_tablet_pre_split_for_insert_from_files
                && !Config.enable_tablet_pre_split_for_broker_load) {
            return SkipReason.DISABLED_BY_CONFIG;
        }
        if (!ConnectContext.getSessionVariableOrDefault().isEnableTabletPreSplit()) {
            return SkipReason.DISABLED_BY_SESSION;
        }
        return null;
    }

    private static boolean isFirstSortKeyColumnSupported(OlapTable table) {
        List<Column> keyColumns = table.getKeyColumnsInOrder();
        if (keyColumns.isEmpty()) {
            return false;
        }
        // Deeper per-column validation (decimal precision/scale, primitive-type match against
        // sampler tuples) is BoundaryPlanner's job at plan time; this gate only needs to keep
        // composite/complex types out of the external-boundaries split path.
        return keyColumns.get(0).getType().isScalarType();
    }

    /**
     * Choose how many tablets to pre-split a load into.
     *
     * <p>Picks the larger of two lower bounds — the cluster's active compute-node
     * count (so every compute node gets at least one tablet) and the byte-volume
     * estimate ({@code ceil(estimatedTotalBytes / tablet_reshard_target_size)})
     * — then clamps to {@code [2, tablet_reshard_max_split_count]}. Two is the
     * minimum because a single-tablet result is equivalent to skipping pre-split.
     *
     * @param estimates              full-input estimates from the sampler. Only
     *                               {@link Estimates#totalBytes} is read.
     * @param activeComputeNodeCount compute nodes available to the load, after
     *                               warehouse/blocklist filtering. Must be {@code >= 1}.
     */
    static int selectTabletCount(Estimates estimates, int activeComputeNodeCount) {
        Objects.requireNonNull(estimates, "estimates");
        Preconditions.checkArgument(activeComputeNodeCount >= 1,
                "activeComputeNodeCount must be >= 1, was %s", activeComputeNodeCount);

        long byteTargetTabletCount = (long) Math.ceil(
                (double) estimates.totalBytes() / Config.tablet_reshard_target_size);
        long proposed = Math.max(activeComputeNodeCount, byteTargetTabletCount);
        long clamped = Math.max(2L, Math.min(proposed, Config.tablet_reshard_max_split_count));
        return (int) clamped;
    }
}
