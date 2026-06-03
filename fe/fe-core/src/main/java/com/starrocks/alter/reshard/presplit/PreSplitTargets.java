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
 * <p>Each helper returns a sentinel ({@code null} or {@code -1}) for "no
 * match" so callers can chain early returns without intermediate booleans.
 * The eligibility gate inside {@link TabletPreSplitCoordinator#maybeAct}
 * re-runs equivalent checks against the resolved {@code physicalPartitionId};
 * the helpers here only let the hook skip the pipeline construction.
 *
 * <p><b>Two slices</b>:
 * <ul>
 *   <li>{@link #findEligibleTable} — table-only structural gate (range
 *       distribution, NORMAL state, no MV/rollup, supported sort key) used
 *       by the multi-partition coordinator's defensive re-check. The
 *       multi-partition coordinator does per-partition checks on its own
 *       under a short READ lock after pre-create.</li>
 *   <li>{@link #findEligibleTarget} — single-partition gate; performs the
 *       per-partition slice (single physical partition, single base tablet)
 *       and continues to gate the existing single-partition hooks unchanged.</li>
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
     * Chain {@link #findUniquePhysicalPartition} + {@link #findSingleBaseTabletId}
     * into a single eligibility resolve. Used by every load-path hook so all
     * callers produce the same {@link EligibleTarget} shape.
     *
     * @return the resolved {@link EligibleTarget}, or {@code null} when the
     *         table has zero or multiple partitions, the base index lookup
     *         fails (e.g. an alter raced the load), or the partition has
     *         zero or multiple base-index tablets.
     */
    static EligibleTarget findEligibleTarget(Database database, OlapTable olapTable) {
        PhysicalPartition uniquePartition = findUniquePhysicalPartition(olapTable);
        if (uniquePartition == null) {
            return null;
        }
        long oldTabletId = findSingleBaseTabletId(olapTable, uniquePartition);
        if (oldTabletId < 0) {
            return null;
        }
        return new EligibleTarget(database, olapTable, uniquePartition.getId(), oldTabletId);
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

    /**
     * @return the single base-index tablet's id, or {@code -1} when the
     *         partition has zero or multiple base-index tablets.
     */
    static long findSingleBaseTabletId(OlapTable table, PhysicalPartition partition) {
        MaterializedIndex baseIndex = partition.getIndex(table.getBaseIndexMetaId());
        if (baseIndex == null) {
            return -1L;
        }
        List<Tablet> tablets = baseIndex.getTablets();
        if (tablets.size() != 1) {
            return -1L;
        }
        return tablets.get(0).getId();
    }
}
