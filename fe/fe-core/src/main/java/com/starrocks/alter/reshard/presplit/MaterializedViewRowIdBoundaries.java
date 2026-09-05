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
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.mv.RowIdStrategy;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.rule.ivm.common.IvmOpUtils;

import java.util.List;

/**
 * Decides whether an incremental materialized view's refresh can be pre-split from the derived tier,
 * and if so hands back the {@link DerivedBoundarySource} that carves it. Such a view is keyed by the
 * hidden {@code __ROW_ID__} column; when the storage engine generates that id, its value domain
 * follows from how the id is produced rather than from the data, so the cuts need no sample.
 */
final class MaterializedViewRowIdBoundaries {

    private MaterializedViewRowIdBoundaries() {
    }

    /**
     * Whether the derived tier can carve {@code table} at all. Kept separate from {@link #sourceFor} and
     * free of side effects so a caller can answer "is this a candidate?" before it resolves a target,
     * takes a lock, or consults a feature flag. Answering it later would attribute an ordinary
     * materialized view's refresh to whatever the target resolver happened to reject it for, and would
     * count every such refresh against the feature's own config gate.
     */
    static boolean isDerivable(OlapTable table) {
        return hasDerivableRowIdSortKey(table);
    }

    /**
     * Whether the targets actually RESOLVED for a submit are still the single row-id-keyed index
     * {@link #isDerivable} accepted. Separate from that check because they read different things at
     * different times: {@code isDerivable} reads the table's visible index metas before a target is
     * resolved, while this reads the resolved targets themselves. A rollup becoming visible in between
     * would otherwise hand row-id boundaries to an index whose key is a different column.
     */
    static boolean hasSoleRowIdIndexTarget(List<IndexPreSplitTarget> indexTargets) {
        return indexTargets.size() == 1 && isRowIdSortKey(indexTargets.get(0).sortKey());
    }

    /**
     * @param estimates              the refresh's estimated output; the row count is what the id space
     *                               is carved by
     * @param activeComputeNodeCount compute nodes that can hold cached id blocks
     * @return a source that derives the cuts when it is asked to plan. Callers must have established
     *         {@link #isDerivable(OlapTable)} first.
     */
    static DerivedBoundarySource sourceFor(OlapTable table, Estimates estimates, int activeComputeNodeCount) {
        return (indexTarget, requestedTabletCount) -> AutoIncrementRowIdBoundaries.plan(
                // Read when the boundaries are planned rather than when this source is resolved: the
                // derivation holds only while the counter is pristine, so the closer that read sits to
                // the split being submitted, the smaller the window in which a concurrent load could
                // have started allocating ids without this planner noticing.
                GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getCurrentAutoIncrementIdByTableId(table.getId()),
                estimates.totalRows(), requestedTabletCount, activeComputeNodeCount,
                indexTarget.sortKey().get(0));
    }

    /**
     * Whether {@code table} is an incremental materialized view whose single visible index is keyed by
     * the storage-generated row-id column alone — the one shape
     * {@link AutoIncrementRowIdBoundaries} can carve.
     */
    private static boolean hasDerivableRowIdSortKey(OlapTable table) {
        // A non-incremental view has no hidden row-id column at all, so nothing about its sort key is
        // derivable from how an id is produced.
        if (!(table instanceof MaterializedView materializedView)
                || materializedView.getRowIdStrategy() == null) {
            return false;
        }
        // QUERY_COMPUTED means the view's own query computes the row id (an encoding of its group-by
        // keys). That domain is computable but not derivable from anything the FE holds, and no
        // boundary source serves it yet.
        if (materializedView.getRowIdStrategy() != RowIdStrategy.AUTO_INCREMENT) {
            return false;
        }
        // The pipeline plans boundaries for every visible index it is handed. A second index would
        // therefore either be cut on boundaries expressed in a column that is not its key, or be left
        // unsplit and keep the single-tablet write bottleneck pre-split exists to remove.
        List<MaterializedIndexMeta> visibleIndexMetas = table.getVisibleIndexMetas();
        if (visibleIndexMetas.size() != 1) {
            return false;
        }
        // Asserted rather than assumed: an ORDER BY on a range-distributed incremental view is
        // rejected at CREATE, so the sort key is the row-id column alone today. The derived cuts are
        // row ids, though, and nothing here should depend on that rejection staying in place.
        return isRowIdSortKey(
                MetaUtils.getRangeDistributionColumns(table, visibleIndexMetas.get(0).getIndexMetaId()));
    }

    /** The hidden row-id column alone — the only sort key the derived arithmetic describes. */
    private static boolean isRowIdSortKey(List<Column> sortKey) {
        return sortKey.size() == 1 && IvmOpUtils.COLUMN_ROW_ID.equalsIgnoreCase(sortKey.get(0).getName());
    }

    /**
     * Context passed to {@link TabletPreSplitCoordinator#submitAsynchronously}, whose signature
     * requires one. The derived tier reads nothing, so there is nothing for a context to carry: this
     * exists only to satisfy that signature.
     */
    record RowIdScanContext() implements ScanContext {
    }
}
