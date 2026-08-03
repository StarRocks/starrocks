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

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Tablet;
import com.starrocks.sql.common.MetaUtils;

import java.util.Collection;
import java.util.List;

/**
 * Shared eligibility helpers used by the load-path hooks to short-circuit
 * before constructing the pipeline. The single-partition contract requires a
 * single-physical-partition, single-base-tablet target; these helpers encode
 * that contract in one place so every hook (and any future caller) checks the
 * same shape.
 *
 * <p>The eligibility gate inside {@link TabletPreSplitCoordinator#maybeAct}
 * re-runs equivalent checks against the resolved {@code physicalPartitionId};
 * the helpers here only let the hook skip the pipeline construction. Because
 * the single-partition hook flow short-circuits before {@code maybeAct}, the
 * skip must still be attributed to a {@link SkipReason} so operators can see
 * why pre-split did not run.
 *
 * <p><b>Two slices</b>:
 * <ul>
 *   <li>{@link #findEligibleTable} — table-only structural gate (range
 *       distribution, NORMAL state, no MV/rollup, supported sort key) used
 *       by the multi-partition coordinator's defensive re-check. Returns the
 *       failing {@link SkipReason}, or {@code null} when the table is eligible;
 *       the caller records it. The multi-partition coordinator does
 *       per-partition checks on its own under a short READ lock after
 *       pre-create.</li>
 *   <li>{@link #findEligibleTarget} — single-partition gate; performs the
 *       per-partition slice (single physical partition, single base tablet)
 *       and gates the single-partition hooks. Returns the resolved
 *       {@link EligibleTarget}, or {@code null} after recording the failing
 *       {@link SkipReason} itself.</li>
 * </ul>
 */
final class PreSplitTargets {

    private PreSplitTargets() {
    }

    /**
     * Resolved single-partition single-tablet target bundle. Both hooks pass
     * this through to the coordinator as one value.
     */
    record EligibleTarget(Database database, OlapTable olapTable, long partitionId, long oldTabletId) {
    }

    /**
     * Table-level eligibility check shared by the multi-partition path
     * ({@link TabletPreSplitCoordinator#submitForPartitionsCombined}) and the
     * single-partition path (which still relies on the equivalent inline
     * checks inside {@link TabletPreSplitCoordinator#maybeAct}). Per-partition
     * checks (empty, single-tablet) stay with the caller; the multi-partition
     * coordinator runs them per-bucket under its post-pre-create READ lock.
     *
     * <p>Returns the matching {@link SkipReason} (which the caller routes into
     * the eligibility-skip bvar) when any structural table-level check fails;
     * {@code null} when the table is eligible.
     */
    static SkipReason findEligibleTable(Database database, OlapTable table) {
        // The reshard substrate (SplitTabletJobFactory.validateTableLevel) only accepts
        // cloud-native tables. Reject here too, before the multi-partition path pre-creates
        // any partition that the factory would then reject — otherwise non-cloud-native
        // range-distributed tables (reachable in shared-nothing mode) leave orphaned
        // partitions behind.
        if (!table.isCloudNativeTableOrMaterializedView()) {
            return SkipReason.NOT_CLOUD_NATIVE;
        }
        if (!table.isRangeDistribution()) {
            return SkipReason.NOT_RANGE_DISTRIBUTION;
        }
        if (table.getState() != OlapTable.OlapTableState.NORMAL) {
            return SkipReason.TABLE_NOT_NORMAL;
        }
        if (table.getVisibleIndexMetas().size() != 1) {
            return SkipReason.HAS_MATERIALIZED_VIEW_OR_ROLLUP;
        }
        // Mirrors TabletPreSplitCoordinator.areSortKeyColumnsSupported: every
        // sort-key column must be scalar; deeper per-column validation runs
        // at plan time.
        List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(table);
        if (sortKeyColumns.isEmpty()) {
            return SkipReason.UNSUPPORTED_SORT_KEY;
        }
        for (Column column : sortKeyColumns) {
            if (!column.getType().isScalarType()) {
                return SkipReason.UNSUPPORTED_SORT_KEY;
            }
        }
        return null;
    }

    /**
     * Resolve the single-partition, single-tablet target for a load-path hook,
     * recording the eligibility-skip counter when no eligible target is found.
     *
     * <p>The recording is centralized here, rather than left to each caller,
     * because the single-partition hook flows are the only callers and they all
     * short-circuit before {@link TabletPreSplitCoordinator#maybeAct} — the gate
     * that would otherwise record the skip. Folding it in means neither hook can
     * forget it, and mirrors {@link TabletPreSplitCoordinator}'s own
     * {@code skipEligibility}. (Contrast {@link #findEligibleTable}, which stays
     * pure because its multi-partition caller needs the reason for its own
     * {@code Skipped} outcome and records it itself.)
     *
     * @return the resolved {@link EligibleTarget}, or {@code null} when the table
     *         does not have a single physical partition or its base index is
     *         missing — records {@link SkipReason#METADATA_NOT_RESOLVED} (e.g. an
     *         alter raced the load); or when the partition has zero or multiple
     *         base-index tablets — records {@link SkipReason#MULTIPLE_BASE_INDEX_TABLETS}
     *         (the common case on a re-load against an already-split partition).
     */
    static EligibleTarget findEligibleTarget(Database database, OlapTable olapTable) {
        PhysicalPartition uniquePartition = findUniquePhysicalPartition(olapTable);
        if (uniquePartition == null) {
            PreSplitMetrics.recordEligibilitySkip(SkipReason.METADATA_NOT_RESOLVED);
            return null;
        }
        MaterializedIndex baseIndex = uniquePartition.getIndex(olapTable.getBaseIndexMetaId());
        if (baseIndex == null) {
            PreSplitMetrics.recordEligibilitySkip(SkipReason.METADATA_NOT_RESOLVED);
            return null;
        }
        List<Tablet> baseTablets = baseIndex.getTablets();
        if (baseTablets.size() != 1) {
            PreSplitMetrics.recordEligibilitySkip(SkipReason.MULTIPLE_BASE_INDEX_TABLETS);
            return null;
        }
        long baseTabletId = baseTablets.get(0).getId();
        return new EligibleTarget(database, olapTable, uniquePartition.getId(), baseTabletId);
    }

    /**
     * @return the unique {@link PhysicalPartition} of {@code table}, or
     *         {@code null} when the table has zero or multiple partitions.
     *         Multi-partition loads are out of scope for the single-partition gate because per-row
     *         partition routing makes the target-partition set unknowable.
     */
    static PhysicalPartition findUniquePhysicalPartition(OlapTable table) {
        Collection<PhysicalPartition> partitions = table.getPhysicalPartitions();
        if (partitions.size() != 1) {
            return null;
        }
        return partitions.iterator().next();
    }
}
