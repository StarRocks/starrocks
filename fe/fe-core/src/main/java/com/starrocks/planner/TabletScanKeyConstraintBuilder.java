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

package com.starrocks.planner;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.thrift.TTabletScanKeyConstraint;
import com.starrocks.thrift.TTabletScanKeyConstraintType;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds the per-tablet distribution topology that lets the BE drop scan keys which cannot live on
 * a given tablet.
 *
 * <p>Only the topology travels to the BE -- bucket ordinal, bucket count and where the distribution
 * columns sit in the scan-key tuple. The values themselves come from the scan keys the BE already
 * built from the pushed-down predicate, so nothing here depends on query parameters and prepared
 * statements need no extra plan state.
 *
 * <p>{@link #create} returns null whenever the scan is not eligible; the caller then sends no
 * constraint and the BE runs its pre-existing path.
 */
public final class TabletScanKeyConstraintBuilder {
    // Positions of the distribution columns inside the scan-key tuple, in DDL declaration order.
    private final List<Integer> distributionKeyPositions;

    private TabletScanKeyConstraintBuilder(List<Integer> distributionKeyPositions) {
        this.distributionKeyPositions = distributionKeyPositions;
    }

    /**
     * @param distributionInfo the distribution info of the partition being scanned, not the table
     *                         default: bucket layout is per partition.
     * @return null when this scan cannot be pruned (non-HASH distribution, or a distribution column
     *         that is not part of the selected index's sort key).
     */
    public static TabletScanKeyConstraintBuilder create(OlapTable table, long selectedIndexMetaId,
                                                        DistributionInfo distributionInfo) {
        if (distributionInfo == null ||
                distributionInfo.getType() != DistributionInfo.DistributionInfoType.HASH) {
            return null;
        }
        List<String> sortKeyColumnIds = sortKeyColumnIds(table, selectedIndexMetaId);
        if (sortKeyColumnIds.isEmpty()) {
            return null;
        }

        List<ColumnId> distributionColumns = ((HashDistributionInfo) distributionInfo).getDistributionColumns();
        if (distributionColumns == null || distributionColumns.isEmpty()) {
            return null;
        }

        List<Integer> positions = new ArrayList<>(distributionColumns.size());
        for (ColumnId distributionColumn : distributionColumns) {
            int position = sortKeyColumnIds.indexOf(distributionColumn.getId());
            if (position < 0) {
                // A distribution column outside the sort key never appears in a scan key, so no scan
                // key can be routed. Bail out for the whole scan rather than per range.
                return null;
            }
            positions.add(position);
        }
        return new TabletScanKeyConstraintBuilder(ImmutableList.copyOf(positions));
    }

    /**
     * @param index               the materialized index of the physical partition owning the tablet;
     *                            its tablet order defines the bucket ordinals, exactly as
     *                            {@code HashDistributionPruner} uses it.
     * @param tabletId            tablet to describe.
     * @param selectedTabletCount how many tablets of {@code index} this query scans, used only to
     *                            report whether distribution pruning narrowed the scan.
     * @return null when the tablet's bucket ordinal cannot be determined.
     */
    public TTabletScanKeyConstraint build(MaterializedIndex index, long tabletId, int selectedTabletCount) {
        // Derive both the ordinal and the bucket count from the same list HashDistributionPruner
        // indexes into, so the two can never disagree with the pruning that selected this tablet.
        List<Long> tabletIdsInOrder = index.getTabletIdsInOrder();
        int bucketNum = tabletIdsInOrder.size();
        int bucketId = tabletIdsInOrder.indexOf(tabletId);
        if (bucketId < 0 || bucketNum <= 0) {
            return null;
        }

        TTabletScanKeyConstraint constraint = new TTabletScanKeyConstraint();
        constraint.setType(TTabletScanKeyConstraintType.HASH_BUCKET);
        constraint.setDistribution_key_positions(distributionKeyPositions);
        constraint.setBucket_id(bucketId);
        constraint.setBucket_num(bucketNum);
        constraint.setHash_version(HASH_VERSION);
        // Advisory only, never load-bearing for correctness: when the scan covers every tablet we
        // cannot claim a tablet was selected because some value hashes to it, so the BE must not
        // treat an empty prune result as evidence of a broken hash contract.
        constraint.setPruning_was_exact(selectedTabletCount > 0 && selectedTabletCount < bucketNum);
        return constraint;
    }

    private static List<String> sortKeyColumnIds(OlapTable table, long selectedIndexMetaId) {
        // Mirrors how OlapScanNode#toThrift fills sort_key_column_names, which is what orders the
        // scan-key tuple on the BE. Kept as a separate read-only walk so the off path is untouched;
        // TabletScanKeyConstraintBuilderTest asserts the two stay in agreement.
        if (selectedIndexMetaId == -1) {
            return List.of();
        }
        MaterializedIndexMeta indexMeta = table.getIndexMetaByMetaId(selectedIndexMetaId);
        if (indexMeta == null) {
            return List.of();
        }
        List<String> columnIds = new ArrayList<>();
        if (indexMeta.getSortKeyIdxes() != null) {
            List<Column> schema = indexMeta.getSchema();
            for (Integer sortKeyIdx : indexMeta.getSortKeyIdxes()) {
                if (sortKeyIdx == null || sortKeyIdx < 0 || sortKeyIdx >= schema.size()) {
                    return List.of();
                }
                columnIds.add(schema.get(sortKeyIdx).getColumnId().getId());
            }
        } else {
            for (Column column : table.getSchemaByIndexMetaId(selectedIndexMetaId)) {
                if (!column.isKey()) {
                    continue;
                }
                columnIds.add(column.getColumnId().getId());
            }
        }
        return columnIds;
    }

    // Bump together with any change to how the BE derives a bucket from a scan key.
    private static final int HASH_VERSION = 1;
}
