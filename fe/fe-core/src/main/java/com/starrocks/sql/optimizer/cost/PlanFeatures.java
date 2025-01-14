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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.TreeNode;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import org.apache.commons.collections.CollectionUtils;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Features for physical plan
 */
public class PlanFeatures {

    private static final ImmutableSet<OperatorType> EXCLUDE_OPERATORS = ImmutableSet.of(
            OperatorType.PHYSICAL_MERGE_JOIN,
            OperatorType.PHYSICAL_ICEBERG_SCAN,
            OperatorType.PHYSICAL_HIVE_SCAN,
            OperatorType.PHYSICAL_FILE_SCAN,
            OperatorType.PHYSICAL_ICEBERG_EQUALITY_DELETE_SCAN,
            OperatorType.PHYSICAL_HUDI_SCAN,
            OperatorType.PHYSICAL_DELTALAKE_SCAN,
            OperatorType.PHYSICAL_PAIMON_SCAN,
            OperatorType.PHYSICAL_ODPS_SCAN,
            OperatorType.PHYSICAL_ICEBERG_METADATA_SCAN,
            OperatorType.PHYSICAL_KUDU_SCAN,
            OperatorType.PHYSICAL_SCHEMA_SCAN,
            OperatorType.PHYSICAL_MYSQL_SCAN,
            OperatorType.PHYSICAL_META_SCAN,
            OperatorType.PHYSICAL_ES_SCAN,
            OperatorType.PHYSICAL_JDBC_SCAN,
            OperatorType.PHYSICAL_STREAM_SCAN,
            OperatorType.PHYSICAL_STREAM_JOIN,
            OperatorType.PHYSICAL_STREAM_AGG,
            OperatorType.PHYSICAL_TABLE_FUNCTION_TABLE_SCAN
    );

    // A trivial implement of feature extracting
    // TODO: implement sophisticated feature extraction methods
    public static FeatureVector flattenFeatures(OptExpression plan) {
        Extractor extractor = new Extractor();
        PlanTreeBuilder builder = new PlanTreeBuilder();
        OperatorWithFeatures root = plan.getOp().accept(extractor, plan, builder);

        // summarize by operator type
        Map<OperatorType, SummarizedFeature> sumVector = Maps.newHashMap();
        sumByOperatorType(root, sumVector);
        FeatureVector result = new FeatureVector();

        // Plan features
        // top-3 tables
        final int TOP_N_TABLES = 3;
        List<Long> topTables = Lists.newArrayList(0L, 0L, 0L);
        SummarizedFeature scanOperators = sumVector.get(OperatorType.PHYSICAL_OLAP_SCAN);
        if (scanOperators != null && CollectionUtils.isNotEmpty(scanOperators.tableSet)) {
            topTables.addAll(scanOperators.tableSet.stream()
                    .sorted(Comparator.comparing(x -> ((OlapTable) x).getRowCount()).reversed())
                    .limit(TOP_N_TABLES)
                    .map(Table::getId)
                    .toList());
            topTables = topTables.subList(topTables.size() - TOP_N_TABLES, topTables.size());
        }
        result.add(topTables);

        // Add operator features
        for (int start = OperatorType.PHYSICAL.ordinal();
                start < OperatorType.SCALAR.ordinal();
                start++) {
            OperatorType opType = OperatorType.values()[start];
            if (EXCLUDE_OPERATORS.contains(opType)) {
                continue;
            }
            SummarizedFeature vector = sumVector.get(opType);
            if (vector != null) {
                result.add(vector.finish());
            } else {
                result.add(SummarizedFeature.empty(opType));
            }
        }

        return result;
    }

    private static void sumByOperatorType(OperatorWithFeatures tree, Map<OperatorType, SummarizedFeature> sum) {
        List<Long> vector = tree.toVector();
        OperatorType opType = tree.features.opType;
        SummarizedFeature exist = sum.computeIfAbsent(opType, (x) -> new SummarizedFeature(opType));
        exist.merge(tree);

        // recursive
        for (var child : tree.getChildren()) {
            sumByOperatorType(child, sum);
        }
    }

    private static class SummarizedFeature {
        OperatorType opType;
        int count = 0;
        FeatureVector vector;
        Set<Table> tableSet;

        SummarizedFeature(OperatorType type) {
            this.opType = type;
            if (type == OperatorType.PHYSICAL_OLAP_SCAN) {
                this.tableSet = Sets.newHashSet();
            }
        }

        public void merge(OperatorWithFeatures node) {
            this.count++;
            if (this.vector == null) {
                this.vector = new FeatureVector(node.features.toVector());
            } else {
                // A + B => C
                List<Long> vector1 = node.features.toVector();
                for (int i = 0; i < vector.vector.size(); i++) {
                    this.vector.vector.set(i, this.vector.vector.get(i) + vector1.get(i));
                }
            }
            if (node.features instanceof ScanOperatorFeatures scanNode) {
                this.tableSet.add(scanNode.getTable());
            }
        }

        public FeatureVector finish() {
            List<Long> result = Lists.newArrayList();
            result.add((long) opType.ordinal());
            result.add((long) count);
            if (vector != null) {
                result.addAll(vector.vector);
            }
            return new FeatureVector(result);
        }

        public static FeatureVector empty(OperatorType type) {
            List<Long> result = Lists.newArrayList();
            result.add((long) type.ordinal());
            result.add((long) 0);
            for (int i = 0; i < OperatorFeatures.numFeatures(type); i++) {
                result.add(0L);
            }
            return new FeatureVector(result);
        }

    }

    public static class FeatureVector {
        List<Long> vector = Lists.newArrayList();
        Set<Table> tables = Sets.newHashSet();

        public FeatureVector() {
        }

        public FeatureVector(List<Long> vector) {
            this.vector = vector;
        }

        public String toFeatureString() {
            return Joiner.on(",").join(vector);
        }

        public void add(Collection<Long> vector) {
            this.vector.addAll(vector);
        }

        public void add(FeatureVector vector) {
            if (vector.vector != null) {
                this.vector.addAll(vector.vector);
            }
        }
    }

    // The tree structure of plan
    static class OperatorWithFeatures extends TreeNode<OperatorWithFeatures> {
        int planNodeId;
        OperatorFeatures features;

        public static OperatorWithFeatures build(int planNodeId, OperatorFeatures features) {
            OperatorWithFeatures res = new OperatorWithFeatures();
            res.planNodeId = planNodeId;
            res.features = features;
            return res;
        }

        public List<Long> toVector() {
            return features.toVector();
        }
    }

    static class PlanTreeBuilder {

    }

    // TODO: build specific features for operator
    public static class OperatorFeatures {

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

    static class Extractor extends OptExpressionVisitor<OperatorWithFeatures, PlanTreeBuilder> {

        @Override
        public OperatorWithFeatures visit(OptExpression optExpression, PlanTreeBuilder context) {
            OperatorType opType = optExpression.getOp().getOpType();
            Statistics stats = optExpression.getStatistics();
            CostEstimate cost = CostModel.calculateCostEstimate(new ExpressionContext(optExpression));

            OperatorFeatures features = OperatorFeatures.build(optExpression, cost, stats);
            OperatorWithFeatures node = OperatorWithFeatures.build(optExpression.getOp().getPlanNodeId(), features);

            // recursive visit
            for (var child : optExpression.getInputs()) {
                OperatorWithFeatures childNode = visit(child, context);
                node.addChild(childNode);
            }

            return node;
        }

    }
}
