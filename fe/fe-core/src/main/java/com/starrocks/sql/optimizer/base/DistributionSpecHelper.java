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

package com.starrocks.sql.optimizer.base;

import com.starrocks.catalog.ColocateGroupSchema;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Shared helpers for building {@link DistributionSpec} instances from catalog
 * metadata. Used by both {@code RelationTransformer.getTableDistributionSpec}
 * and {@code MvRewritePreprocessor.getTableDistributionSpec} so that the
 * range-colocate spec-construction logic has one implementation.
 */
public final class DistributionSpecHelper {
    private DistributionSpecHelper() {
    }

    /**
     * Build a skeleton {@link RangeDistributionSpec} for a range-distributed
     * olap table when it belongs to a range-colocate group.
     *
     * <p>The returned spec has colocate columns resolved through the provided
     * {@code columnMetaToColRefMap} but carries an empty
     * {@link EquivalentDescriptor} (no partitions yet). Partition-aware
     * rebuild happens later in
     * {@code OutputPropertyDeriver.visitPhysicalOlapScan}, mirroring the hash
     * pattern at {@code OutputPropertyDeriver.java:498}.
     *
     * @return a {@code RangeDistributionSpec} if the table is range-distributed
     *         and part of a range-colocate group; {@code null} otherwise.
     *         A {@code null} return lets callers fall back to
     *         {@code AnyDistributionSpec}.
     */
    public static RangeDistributionSpec buildRangeDistributionSpecSkeleton(
            OlapTable olapTable, Map<Column, ColumnRefOperator> columnMetaToColRefMap) {
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        long tableId = olapTable.getId();
        if (!colocateTableIndex.isColocateTable(tableId)) {
            return null;
        }
        ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(tableId);
        if (groupId == null || !colocateTableIndex.isRangeColocateGroup(groupId)) {
            return null;
        }
        ColocateGroupSchema groupSchema = colocateTableIndex.getGroupSchema(groupId);
        if (groupSchema == null) {
            // Metadata inconsistency (e.g. partial replay); fall back to ANY.
            return null;
        }
        List<Column> sortKeyColumns = MetaUtils.getRangeDistributionColumns(olapTable);
        int colocateCount = groupSchema.getColocateColumnCount();
        if (sortKeyColumns.size() < colocateCount) {
            // Defensive: catalog schema shorter than the colocate group expects.
            // Fall back to ANY rather than throw during planning.
            return null;
        }
        List<DistributionCol> colocateColumns = new ArrayList<>();
        for (int i = 0; i < colocateCount; i++) {
            Column column = sortKeyColumns.get(i);
            ColumnRefOperator ref = columnMetaToColRefMap.get(column);
            if (ref == null) {
                // Some query shapes (sync MV, cache stats) may project away
                // the colocate columns; fall back to ANY in the caller.
                return null;
            }
            colocateColumns.add(new DistributionCol(ref.getId(), true));
        }
        EquivalentDescriptor emptyEquiv =
                new EquivalentDescriptor(tableId, Collections.emptyList());
        return new RangeDistributionSpec(colocateColumns, emptyEquiv);
    }
}
