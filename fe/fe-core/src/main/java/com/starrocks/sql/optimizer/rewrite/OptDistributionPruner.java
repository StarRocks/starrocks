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


package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.planner.HashDistributionPruner;
import com.starrocks.planner.PartitionColumnFilter;
import com.starrocks.planner.RangeDistributionPruner;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public class OptDistributionPruner {
    private static final Logger LOG = LogManager.getLogger(OptDistributionPruner.class);

    public static List<Long> pruneTabletIds(LogicalOlapScanOperator olapScanOperator,
                                            List<Long> selectedPartitionIds) {
        return computeSelectedTabletIds(olapScanOperator, selectedPartitionIds,
                olapScanOperator.getSelectedIndexMetaId());
    }

    /**
     * Computes the selected tablet ids within {@code selectedPartitionIds} from the scan's
     * distribution-column predicates. Typed against the base LogicalScanOperator because it reads
     * only generic scan state (table, predicate, column filters), nothing OLAP-scan specific.
     * {@code selectedIndexMetaId} picks the materialized index per physical partition.
     */
    private static List<Long> computeSelectedTabletIds(LogicalScanOperator scan,
                                                      List<Long> selectedPartitionIds, long selectedIndexMetaId) {
        OlapTable olapTable = (OlapTable) scan.getTable();

        List<Long> result = Lists.newArrayList();
        for (Long partitionId : selectedPartitionIds) {
            Partition partition = olapTable.getPartition(partitionId);
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                MaterializedIndex index = physicalPartition.getLatestIndex(selectedIndexMetaId);
                Collection<Long> tabletIds = distributionPrune(index, partition.getDistributionInfo(),
                        scan, olapTable.getIdToColumn());
                result.addAll(tabletIds);
            }
        }
        return result;
    }

    private static Collection<Long> distributionPrune(MaterializedIndex index, DistributionInfo distributionInfo,
                                                      LogicalScanOperator operator, Map<ColumnId, Column> idToColumn) {
        if (distributionInfo.getType() == DistributionInfo.DistributionInfoType.HASH ||
                distributionInfo.getType() == DistributionInfo.DistributionInfoType.RANGE) {
            Map<String, PartitionColumnFilter> filters = Maps.newHashMap();
            Table table = operator.getTable();
            if (table.isExprPartitionTable()) {
                // Bucketing needs to use the original predicate for hashing
                ColumnFilterConverter.convertColumnFilterWithoutExpr(operator.getPredicate(), filters, table);
            } else {
                filters = operator.getColumnFilters();
            }

            if (distributionInfo.getType() == DistributionInfo.DistributionInfoType.HASH) {
                HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
                HashDistributionPruner pruner = new HashDistributionPruner(index.getTabletIdsInOrder(),
                        MetaUtils.getColumnsByColumnIds(idToColumn, info.getDistributionColumns()),
                        filters);
                return pruner.prune();
            } else {
                RangeDistributionPruner pruner = new RangeDistributionPruner(index.getTablets(),
                        MetaUtils.getRangeDistributionColumns((OlapTable) operator.getTable()),
                        filters);
                return pruner.prune();
            }
        }

        return index.getTabletIdsInOrder();
    }
}
