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

package com.starrocks.sql.optimizer.cost.feature;

import com.google.common.collect.Lists;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.TreeNode;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.cost.CostEstimate;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;

import java.util.List;

/**
 * Features for each operator, different operators can have features of varying lengths.
 */
public class OperatorFeatures extends TreeNode<OperatorFeatures> {

    public static final int VECTOR_LENGTH = 5;

    protected OptExpression optExpression;
    protected OperatorType opType;
    protected CostEstimate cost;
    protected Statistics stats;

    public OperatorFeatures(OptExpression optExpr, CostEstimate cost, Statistics stats) {
        this.optExpression = optExpr;
        this.opType = optExpr.getOp().getOpType();
        this.cost = cost;
        this.stats = stats;
    }

    /**
     * Transform all features into a vector
     */
    public List<Long> toVector() {
        List<Long> res = Lists.newArrayList();
        res.add(encodeLargeSize((long) stats.getOutputRowCount()));
        res.add((long) stats.getAvgRowSize());
        res.add(encodeLargeSize((long) cost.getMemoryCost()));
        res.add(encodeLargeSize((long) cost.getCpuCost()));

        // LIMIT
        long limit = this.optExpression.getOp().getLimit();
        if (limit != Operator.DEFAULT_LIMIT) {
            res.add(encodeLargeSize(limit));
        } else {
            res.add(0L);
        }

        return res;
    }

    public static int vectorLength(OperatorType opType) {
        if (opType.isPhysicalScan()) {
            return ScanOperatorFeatures.VECTOR_LENGTH;
        }
        if (opType == OperatorType.PHYSICAL_HASH_JOIN) {
            return JoinOperatorFeatures.VECTOR_LENGTH;
        }
        if (opType == OperatorType.PHYSICAL_HASH_AGG) {
            return AggOperatorFeatures.VECTOR_LENGTH;
        }
        if (opType == OperatorType.PHYSICAL_DISTRIBUTION) {
            return ExchangeOperatorFeatures.VECTOR_LENGTH;
        }
        return VECTOR_LENGTH;
    }

    /**
     * Apply log transformation on the large int
     */
    public static long encodeLargeSize(long size) {
        return (long) Math.log1p(size);
    }

    public static class ScanOperatorFeatures extends OperatorFeatures {

        public static final int VECTOR_LENGTH = OperatorFeatures.VECTOR_LENGTH + 4;

        protected final Table table;
        protected final long numTablets;
        protected final long numPartitions;
        protected final int numBinaryPredicates;
        protected final int numPredicateColumns;

        public ScanOperatorFeatures(OptExpression optExpr, CostEstimate cost, Statistics stats) {
            super(optExpr, cost, stats);
            // NOTE: collapse all scans into OLAP_SCAN, to avoid explode the feature vector
            // If needed to deal with some scan operators specifically, it's better to extend this feature
            this.opType = OperatorType.PHYSICAL_OLAP_SCAN;

            PhysicalScanOperator scanOperator = (PhysicalScanOperator) optExpr.getOp();
            if (scanOperator instanceof PhysicalOlapScanOperator olapScanOperator) {
                OlapTable olapTable = (OlapTable) scanOperator.getTable();
                long selectedTablets = olapScanOperator.getSelectedTabletId().size();
                long selectedPartitions = olapScanOperator.getSelectedPartitionId().size();
                this.table = olapTable;
                this.numTablets = selectedTablets;
                this.numPartitions = selectedPartitions;
            } else {
                this.table = scanOperator.getTable();
                this.numTablets = 0;
                this.numPartitions = 0;
            }

            ScalarOperator predicate = scanOperator.getPredicate();
            if (predicate != null) {
                this.numPredicateColumns = predicate.getUsedColumns().size();
                List<BinaryPredicateOperator> collect = Utils.collect(predicate, BinaryPredicateOperator.class);
                this.numBinaryPredicates = collect.size();
            } else {
                this.numPredicateColumns = 0;
                this.numBinaryPredicates = 0;
            }
        }

        @Override
        public List<Long> toVector() {
            List<Long> res = super.toVector();
            res.add(numTablets);
            res.add(numPartitions);
            res.add((long) numPredicateColumns);
            res.add((long) numBinaryPredicates);

            return res;
        }

        public Table getTable() {
            return table;
        }

    }

    public static class JoinOperatorFeatures extends OperatorFeatures {

        public static final int VECTOR_LENGTH = OperatorFeatures.VECTOR_LENGTH + 3;

        // TODO: consider the join cardinality
        protected final double rightSize;
        protected final boolean isBroadcast;
        protected final JoinOperator joinOperator;

        public JoinOperatorFeatures(OptExpression optExpr, CostEstimate cost, Statistics stats) {
            super(optExpr, cost, stats);

            OptExpression rightOp = optExpr.getInputs().get(1);
            Statistics rightStats = rightOp.getStatistics();
            this.rightSize = rightStats.getOutputSize(rightOp.getOutputColumns());
            this.isBroadcast = rightOp.getOutputProperty().getDistributionProperty().isBroadcast();

            PhysicalHashJoinOperator hashJoin = (PhysicalHashJoinOperator) optExpr.getOp();
            this.joinOperator = hashJoin.getJoinType();
        }

        @Override
        public List<Long> toVector() {
            List<Long> res = super.toVector();
            res.add(encodeLargeSize((long) rightSize));
            res.add(isBroadcast ? 1L : 0L);
            // TODO: one-hot encoding
            res.add((long) joinOperator.ordinal());
            return res;
        }
    }

    public static class AggOperatorFeatures extends OperatorFeatures {

        public static final int VECTOR_LENGTH = OperatorFeatures.VECTOR_LENGTH + 4;

        protected final double inputRows;
        protected final int numGroupByColumns;
        protected final int numAggregations;
        protected final double aggRatio;

        public AggOperatorFeatures(OptExpression optExpr, CostEstimate cost, Statistics stats) {
            super(optExpr, cost, stats);

            PhysicalHashAggregateOperator hashAgg = (PhysicalHashAggregateOperator) optExpr.getOp();
            this.numGroupByColumns = hashAgg.getGroupBys().size();
            this.numAggregations = hashAgg.getAggregations().size();

            OptExpression input = optExpr.getInputs().get(0);
            this.inputRows = input.getStatistics().getOutputRowCount();
            this.aggRatio = (1 + inputRows) / (1 + stats.getOutputRowCount());
        }

        @Override
        public List<Long> toVector() {
            List<Long> res = super.toVector();
            res.add(encodeLargeSize((long) inputRows));
            res.add((long) numGroupByColumns);
            res.add((long) numAggregations);
            res.add((long) aggRatio);
            return res;
        }

    }

    public static class ExchangeOperatorFeatures extends OperatorFeatures {

        public static final int VECTOR_LENGTH = OperatorFeatures.VECTOR_LENGTH + 1;

        private final DistributionSpec.DistributionType distributionType;

        public ExchangeOperatorFeatures(OptExpression optExpr, CostEstimate cost, Statistics stats) {
            super(optExpr, cost, stats);

            PhysicalDistributionOperator op = (PhysicalDistributionOperator) optExpr.getOp();
            this.distributionType = op.getDistributionSpec().getType();
        }

        @Override
        public List<Long> toVector() {
            List<Long> res = super.toVector();
            // TODO: apply one-hot encoding
            res.add((long) distributionType.ordinal());
            return res;
        }
    }

}
