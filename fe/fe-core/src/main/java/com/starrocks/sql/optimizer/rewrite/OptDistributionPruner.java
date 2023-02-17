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
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.AnalysisException;
import com.starrocks.planner.DistributionPruner;
import com.starrocks.planner.HashDistributionPruner;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;

public class OptDistributionPruner {
    private static final Logger LOG = LogManager.getLogger(OptDistributionPruner.class);

    public static List<Long> pruneTabletIds(LogicalOlapScanOperator olapScanOperator,
                                            List<Long> selectedPartitionIds) {
        OlapTable olapTable = (OlapTable) olapScanOperator.getTable();

        List<Long> result = Lists.newArrayList();
        for (Long partitionId : selectedPartitionIds) {
            Partition partition = olapTable.getPartition(partitionId);
            MaterializedIndex table = partition.getIndex(olapScanOperator.getSelectedIndexId());
            Collection<Long> tabletIds = distributionPrune(table, partition.getDistributionInfo(), olapScanOperator);
            result.addAll(tabletIds);
        }
        return result;
    }

    private static Collection<Long> distributionPrune(MaterializedIndex index, DistributionInfo distributionInfo,
                                                      LogicalOlapScanOperator operator) {
        try {
            DistributionPruner distributionPruner;
            if (distributionInfo.getType() == DistributionInfo.DistributionInfoType.HASH) {
                HashDistributionInfo info = (HashDistributionInfo) distributionInfo;
                distributionPruner = new HashDistributionPruner(index.getTabletIdsInOrder(),
                        info.getDistributionColumns(),
                        operator.getColumnFilters(),
                        info.getBucketNum());
                return distributionPruner.prune();
            }
        } catch (AnalysisException e) {
            LOG.warn("distribution prune failed. ", e);
        }

        return index.getTabletIdsInOrder();
    }
}
