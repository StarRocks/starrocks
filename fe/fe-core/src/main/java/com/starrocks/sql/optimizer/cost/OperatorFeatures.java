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

package com.starrocks.sql.optimizer.cost;

import com.google.common.collect.Lists;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.TreeNode;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.List;

// TODO: build specific features for operator
public class OperatorFeatures extends TreeNode<OperatorFeatures> {

    protected OperatorType opType;
    protected CostEstimate cost;
    protected Statistics stats;

    protected OperatorFeatures(OptExpression optExpr, CostEstimate cost, Statistics stats) {
        this.opType = optExpr.getOp().getOpType();
        this.cost = cost;
        this.stats = stats;
    }

    static OperatorFeatures build(OptExpression optExpr, CostEstimate cost, Statistics stats) {
        if (optExpr.getOp() instanceof PhysicalScanOperator) {
            return new ScanOperatorFeatures(optExpr, cost, stats);
        }
        return new OperatorFeatures(optExpr, cost, stats);
    }

    public List<Long> toVector() {
        List<Long> res = Lists.newArrayList();
        // TODO: remove this feature, which has no impact to the model
        res.add((long) stats.getOutputRowCount());
        res.add((long) cost.getMemoryCost());
        res.add((long) cost.getCpuCost());

        return res;
    }

    public static int numFeatures(OperatorType opType) {
        if (opType == OperatorType.PHYSICAL_OLAP_SCAN) {
            return 3 + 2;
        }
        return 3;
    }

    static class ScanOperatorFeatures extends OperatorFeatures {

        protected OptExpression optExpression;
        protected final Table table;
        protected final double tabletRatio;
        protected final double partitionRatio;

        protected ScanOperatorFeatures(OptExpression optExpr, CostEstimate cost, Statistics stats) {
            super(optExpr, cost, stats);
            this.optExpression = optExpr;
            PhysicalScanOperator scanOperator = (PhysicalScanOperator) optExpr.getOp();
            if (scanOperator instanceof PhysicalOlapScanOperator olapScanOperator) {
                OlapTable olapTable = (OlapTable) scanOperator.getTable();
                long selectedTablets = olapScanOperator.getSelectedTabletId().size();
                long totalTablets = olapScanOperator.getNumTabletsInSelectedPartitions();
                long selectedPartitions = olapScanOperator.getSelectedPartitionId().size();
                long totalPartitions = olapTable.getVisiblePartitions().size();
                this.table = olapTable;
                this.tabletRatio = (double) (selectedTablets + 1) / (totalTablets + 1);
                this.partitionRatio = (double) (selectedPartitions + 1) / (totalPartitions + 1);
            } else {
                this.table = scanOperator.getTable();
                this.tabletRatio = 0.0;
                this.partitionRatio = 0.0;
            }
        }

        @Override
        public List<Long> toVector() {
            List<Long> res = super.toVector();
            res.add((long) (tabletRatio * 100));
            res.add((long) (partitionRatio * 100));

            return res;
        }

        public Table getTable() {
            return table;
        }

    }
}
