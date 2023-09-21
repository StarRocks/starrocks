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


package com.starrocks.sql;

import com.google.common.collect.Lists;
import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.cost.CostEstimate;
import com.starrocks.sql.optimizer.cost.CostModel;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDecodeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalEsScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalExceptOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHiveScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHudiScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalLimitOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalNoCTEOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSetOperation;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionTableScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.CloneOperator;
import com.starrocks.sql.optimizer.operator.scalar.CollectionElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamAggOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamJoinOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamScanOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class Explain {
    public static String toString(OptExpression root, List<ColumnRefOperator> outputColumns) {
        String outputBuilder = "- Output => [" + outputColumns.stream().map(c -> new ExpressionPrinter().print(c))
                .collect(Collectors.joining(", ")) + "]";

        OperatorStr optStrings = new OperatorPrinter().visit(root, new OperatorPrinter.ExplainContext(1));
        OperatorStr rootOperatorStr = new OperatorStr(outputBuilder, 0, Lists.newArrayList(optStrings));
        return rootOperatorStr.toString();
    }

    public static CostEstimate buildCost(OptExpression optExpression) {
        CostEstimate totalCost = CostModel.calculateCostEstimate(new ExpressionContext(optExpression));
        for (OptExpression child : optExpression.getInputs()) {
            CostEstimate curCost = buildCost(child);
            totalCost = CostEstimate.addCost(totalCost, curCost);
        }
        return totalCost;
    }

    private static class OperatorStr {
        private final String operatorString;
        private final int step;
        private final List<OperatorStr> children;

        public OperatorStr(String strBuilder, int step, List<OperatorStr> children) {
            this.operatorString = strBuilder;
            this.step = step;
            this.children = children;
        }

        public String toString() {
            StringBuilder output;
            output = new StringBuilder(
                    String.join("", Collections.nCopies(step, "    ")) + operatorString);
            for (OperatorStr str : children) {
                String s = output.toString();
                if (!s.endsWith("\n")) {
                    output.append("\n");
                }
                output.append(str);
            }
            return output.toString();
        }
    }

    public static class OperatorPrinter
            extends OptExpressionVisitor<OperatorStr, OperatorPrinter.ExplainContext> {

        static class ExplainContext {
            Integer step;

            public ExplainContext(Integer step) {
                this.step = step;
            }
        }

        public OperatorStr visit(OptExpression optExpression) {
            throw new StarRocksPlannerException("not implement operator : " + optExpression.getOp(),
                    ErrorType.INTERNAL_ERROR);
        }

        @Override
        public OperatorStr visit(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            return optExpression.getOp().accept(this, optExpression, context);
        }

        List<OperatorStr> buildChildOperatorStr(OptExpression optExpression, int step) {
            List<OperatorStr> childString = new ArrayList<>();
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OperatorStr operatorStr =
                        visit(optExpression.inputAt(childIdx), new OperatorPrinter.ExplainContext(step + 1));
                childString.add(operatorStr);
            }
            return childString;
        }

        @Override
        public OperatorStr visitPhysicalOlapScan(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalOlapScanOperator scan = (PhysicalOlapScanOperator) optExpression.getOp();

            StringBuilder sb = new StringBuilder("- SCAN [")
                    .append(((OlapTable) scan.getTable()).getIndexNameById(scan.getSelectedIndexId()))
                    .append("]")
                    .append(buildOutputColumns(scan,
                            "[" + scan.getOutputColumns().stream().map(new ExpressionPrinter()::print)
                                    .collect(Collectors.joining(", ")) + "]"))
                    .append("\n");

            if (scan.getTable().isMaterializedView()) {
                buildOperatorProperty(sb, "MaterializedView: true", context.step);
            }
            buildCostEstimate(sb, optExpression, context.step);

            int totalTabletsNum = 0;
            for (Long partitionId : scan.getSelectedPartitionId()) {
                final Partition partition = ((OlapTable) scan.getTable()).getPartition(partitionId);
                final MaterializedIndex selectedTable = partition.getIndex(scan.getSelectedIndexId());
                totalTabletsNum += selectedTable.getTablets().size();
            }
            String partitionAndBucketInfo = "partitionRatio: " +
                    scan.getSelectedPartitionId().size() +
                    "/" +
                    ((OlapTable) scan.getTable()).getPartitions().size() +
                    ", tabletRatio: " +
                    scan.getSelectedTabletId().size() +
                    "/" +
                    totalTabletsNum;
            buildOperatorProperty(sb, partitionAndBucketInfo, context.step);
            buildCommonProperty(sb, scan, context.step);
            return new OperatorStr(sb.toString(), context.step, Collections.emptyList());
        }

        @Override
        public OperatorStr visitPhysicalHiveScan(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalHiveScanOperator scan = (PhysicalHiveScanOperator) optExpression.getOp();

            StringBuilder sb = new StringBuilder("- HIVE-SCAN [")
                    .append(((HiveTable) scan.getTable()).getTableName())
                    .append("]")
                    .append(buildOutputColumns(scan,
                            "[" + scan.getOutputColumns().stream().map(new ExpressionPrinter()::print)
                                    .collect(Collectors.joining(", ")) + "]"))
                    .append("\n");
            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, scan, context.step);
            return new OperatorStr(sb.toString(), context.step, Collections.emptyList());
        }

        @Override
        public OperatorStr visitPhysicalIcebergScan(OptExpression optExpression,
                                                    OperatorPrinter.ExplainContext context) {
            PhysicalIcebergScanOperator scan = (PhysicalIcebergScanOperator) optExpression.getOp();

            StringBuilder sb = new StringBuilder("- ICEBERG-SCAN [")
                    .append(((IcebergTable) scan.getTable()).getRemoteTableName())
                    .append("]")
                    .append(buildOutputColumns(scan,
                            "[" + scan.getOutputColumns().stream().map(new ExpressionPrinter()::print)
                                    .collect(Collectors.joining(", ")) + "]"))
                    .append("\n");
            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, scan, context.step);
            return new OperatorStr(sb.toString(), context.step, Collections.emptyList());
        }

        public OperatorStr visitPhysicalHudiScan(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalHudiScanOperator scan = (PhysicalHudiScanOperator) optExpression.getOp();

            StringBuilder sb = new StringBuilder("- Hudi-SCAN [")
                    .append(((HudiTable) scan.getTable()).getTableName())
                    .append("]")
                    .append(buildOutputColumns(scan,
                            "[" + scan.getOutputColumns().stream().map(new ExpressionPrinter()::print)
                                    .collect(Collectors.joining(", ")) + "]"))
                    .append("\n");
            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, scan, context.step);
            return new OperatorStr(sb.toString(), context.step, Collections.emptyList());
        }

        public OperatorStr visitPhysicalSchemaScan(OptExpression optExpression,
                                                   OperatorPrinter.ExplainContext context) {
            PhysicalSchemaScanOperator scan = (PhysicalSchemaScanOperator) optExpression.getOp();

            StringBuilder sb = new StringBuilder("- SCHEMA-SCAN [")
                    .append(scan.getTable().getName())
                    .append("]")
                    .append(buildOutputColumns(scan,
                            "[" + scan.getOutputColumns().stream().map(new ExpressionPrinter()::print)
                                    .collect(Collectors.joining(", ")) + "]"))
                    .append("\n");
            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, scan, context.step);
            return new OperatorStr(sb.toString(), context.step, Collections.emptyList());
        }

        @Override
        public OperatorStr visitPhysicalMysqlScan(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalMysqlScanOperator scan = (PhysicalMysqlScanOperator) optExpression.getOp();

            StringBuilder sb = new StringBuilder("- MYSQL-SCAN [")
                    .append(scan.getTable().getName())
                    .append("]")
                    .append(buildOutputColumns(scan,
                            "[" + scan.getOutputColumns().stream().map(new ExpressionPrinter()::print)
                                    .collect(Collectors.joining(", ")) + "]"))
                    .append("\n");
            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, scan, context.step);
            return new OperatorStr(sb.toString(), context.step, Collections.emptyList());
        }

        @Override
        public OperatorStr visitPhysicalEsScan(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalEsScanOperator scan = (PhysicalEsScanOperator) optExpression.getOp();

            StringBuilder sb = new StringBuilder("- ES-SCAN [")
                    .append(scan.getTable().getName())
                    .append("]")
                    .append(buildOutputColumns(scan,
                            "[" + scan.getOutputColumns().stream().map(new ExpressionPrinter()::print)
                                    .collect(Collectors.joining(", ")) + "]"))
                    .append("\n");

            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, scan, context.step);
            return new OperatorStr(sb.toString(), context.step, Collections.emptyList());
        }

        public OperatorStr visitPhysicalMetaScan(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalMetaScanOperator scan = (PhysicalMetaScanOperator) optExpression.getOp();

            StringBuilder sb = new StringBuilder("- META-SCAN [")
                    .append(scan.getTable().getName())
                    .append("]")
                    .append(buildOutputColumns(scan,
                            "[" + scan.getOutputColumns().stream().map(new ExpressionPrinter()::print)
                                    .collect(Collectors.joining(", ")) + "]"))
                    .append("\n");

            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, scan, context.step);
            return new OperatorStr(sb.toString(), context.step, Collections.emptyList());
        }

        public OperatorStr visitPhysicalJDBCScan(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalJDBCScanOperator scan = (PhysicalJDBCScanOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder("- JDBC-SCAN [")
                    .append(scan.getTable().getName())
                    .append("]")
                    .append(buildOutputColumns(scan,
                            "[" + scan.getOutputColumns().stream().map(new ExpressionPrinter()::print)
                                    .collect(Collectors.joining(", ")) + "]"))
                    .append("\n");
            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, scan, context.step);
            return new OperatorStr(sb.toString(), context.step, Collections.emptyList());
        }

        @Override
        public OperatorStr visitPhysicalTopN(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            OperatorStr child = visit(optExpression.getInputs().get(0), new ExplainContext(context.step + 1));
            PhysicalTopNOperator topn = (PhysicalTopNOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder();
            if (topn.getLimit() == Operator.DEFAULT_LIMIT) {
                sb.append("- SORT(");
                if (topn.getSortPhase().equals(SortPhase.FINAL) && !topn.isSplit()) {
                    sb.append("GLOBAL)");
                } else {
                    sb.append(topn.getSortPhase()).append(")");
                }
            } else {
                sb.append("- TOP-").append(topn.getLimit()).append("(").append(topn.getSortPhase()).append(")");
            }
            sb.append(topn.getOrderSpec().getOrderDescs());
            sb.append(buildOutputColumns(topn, ""));
            sb.append("\n");

            buildCostEstimate(sb, optExpression, context.step);
            return new OperatorStr(sb.toString(), context.step, Collections.singletonList(child));
        }

        @Override
        public OperatorStr visitPhysicalDistribution(OptExpression optExpression,
                                                     OperatorPrinter.ExplainContext context) {
            OperatorStr child = visit(optExpression.getInputs().get(0), new ExplainContext(context.step + 1));
            PhysicalDistributionOperator exchange = (PhysicalDistributionOperator) optExpression.getOp();

            StringBuilder sb = new StringBuilder();
            if (exchange.getDistributionSpec() instanceof HashDistributionSpec) {
                HashDistributionDesc desc =
                        ((HashDistributionSpec) exchange.getDistributionSpec()).getHashDistributionDesc();
                sb.append("- EXCHANGE(SHUFFLE) ");
                sb.append(desc.getExplainInfo());
            } else {
                sb.append("- EXCHANGE(").append(exchange.getDistributionSpec()).append(")");
            }
            sb.append("\n");

            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, exchange, context.step);
            return new OperatorStr(sb.toString(), context.step, Collections.singletonList(child));
        }

        @Override
        public OperatorStr visitPhysicalHashJoin(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            return visitPhysicalJoin(optExpression, context);
        }

        @Override
        public OperatorStr visitPhysicalMergeJoin(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            return visitPhysicalJoin(optExpression, context);
        }

        @Override
        public OperatorStr visitPhysicalNestLoopJoin(OptExpression optExpression,
                                                     OperatorPrinter.ExplainContext context) {
            return visitPhysicalJoin(optExpression, context);
        }

        public OperatorStr visitPhysicalJoin(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            OperatorStr left = visit(optExpression.getInputs().get(0), new ExplainContext(context.step + 1));
            OperatorStr right = visit(optExpression.getInputs().get(1), new ExplainContext(context.step + 1));

            PhysicalJoinOperator join = (PhysicalJoinOperator) optExpression.getOp();
            StringBuilder sb =
                    new StringBuilder("- ").append(join.getJoinAlgo()).append("/").append(join.getJoinType());
            if (!join.getJoinType().isCrossJoin()) {
                sb.append(" [").append(new ExpressionPrinter().print(join.getOnPredicate())).append("]");
            }
            sb.append(buildOutputColumns(join, ""));
            sb.append("\n");
            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, join, context.step);
            return new OperatorStr(sb.toString(), context.step, Arrays.asList(left, right));
        }

        @Override
        public OperatorStr visitPhysicalAssertOneRow(OptExpression optExpression,
                                                     OperatorPrinter.ExplainContext context) {
            PhysicalAssertOneRowOperator assertOneRow = (PhysicalAssertOneRowOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder(
                    "- ASSERT " + assertOneRow.getAssertion().name() + " " + assertOneRow.getCheckRows());
            sb.append("\n");
            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, assertOneRow, context.step);
            return new OperatorStr(sb.toString(), context.step, buildChildOperatorStr(optExpression, context.step));
        }

        @Override
        public OperatorStr visitPhysicalHashAggregate(OptExpression optExpression,
                                                      OperatorPrinter.ExplainContext context) {
            OperatorStr child = visit(optExpression.getInputs().get(0), new ExplainContext(context.step + 1));
            PhysicalHashAggregateOperator aggregate = (PhysicalHashAggregateOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder("- AGGREGATE(").append(aggregate.getType()).append(") ");
            sb.append("[").append(aggregate.getGroupBys().stream().map(c -> new ExpressionPrinter().print(c))
                    .collect(Collectors.joining(", "))).append("]");

            sb.append(buildOutputColumns(aggregate, ""));
            sb.append("\n");

            buildCostEstimate(sb, optExpression, context.step);

            for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregate.getAggregations().entrySet()) {
                String analyticCallString =
                        new ExpressionPrinter().print(entry.getKey()) + " := " +
                                new ExpressionPrinter().print(entry.getValue());
                buildOperatorProperty(sb, analyticCallString, context.step);
            }

            buildCommonProperty(sb, aggregate, context.step);
            return new OperatorStr(sb.toString(), context.step, Collections.singletonList(child));
        }

        @Override
        public OperatorStr visitPhysicalAnalytic(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalWindowOperator analytic = (PhysicalWindowOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder();

            sb.append("- ANALYTIC [");
            sb.append(" partition by (");
            for (ScalarOperator partitionExpression : analytic.getPartitionExpressions()) {
                sb.append(new ExpressionPrinter().print(partitionExpression));
                sb.append(", ");
            }
            sb.delete(sb.length() - 2, sb.length());

            sb.append("), order by (");
            for (Ordering orderByElement : analytic.getOrderByElements()) {
                sb.append(orderByElement.toString());
                sb.append(", ");
            }
            sb.delete(sb.length() - 2, sb.length());
            sb.append(")]");
            sb.append("\n");

            buildCostEstimate(sb, optExpression, context.step);

            for (Map.Entry<ColumnRefOperator, CallOperator> entry : analytic.getAnalyticCall().entrySet()) {
                String analyticCallString =
                        new ExpressionPrinter().print(entry.getKey()) + " := " +
                                new ExpressionPrinter().print(entry.getValue())
                                + " " + (analytic.getAnalyticWindow() == null ? AnalyticWindow.DEFAULT_WINDOW.toSql() :
                                analytic.getAnalyticWindow().toSql());
                buildOperatorProperty(sb, analyticCallString, context.step);
            }

            buildCommonProperty(sb, analytic, context.step);
            return new OperatorStr(sb.toString(), context.step, buildChildOperatorStr(optExpression, context.step));
        }

        void buildSetExplain(OptExpression optExpression, StringBuilder sb, OperatorPrinter.ExplainContext context,
                             PhysicalSetOperation setOperation) {
            sb.append(buildOutputColumns(setOperation, setOperation.getOutputColumnRefOp().toString()));
            sb.append("\n");
            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, setOperation, context.step);
        }

        @Override
        public OperatorStr visitPhysicalUnion(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalUnionOperator union = (PhysicalUnionOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder("- UNION");
            buildSetExplain(optExpression, sb, context, union);
            return new OperatorStr(sb.toString(), context.step, buildChildOperatorStr(optExpression, context.step));
        }

        @Override
        public OperatorStr visitPhysicalExcept(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalExceptOperator except = (PhysicalExceptOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder("- EXCEPT");
            buildSetExplain(optExpression, sb, context, except);
            return new OperatorStr(sb.toString(), context.step, buildChildOperatorStr(optExpression, context.step));
        }

        @Override
        public OperatorStr visitPhysicalIntersect(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalIntersectOperator intersect = (PhysicalIntersectOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder("- INTERSECT");
            buildSetExplain(optExpression, sb, context, intersect);
            return new OperatorStr(sb.toString(), context.step, buildChildOperatorStr(optExpression, context.step));
        }

        @Override
        public OperatorStr visitPhysicalValues(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalValuesOperator values = (PhysicalValuesOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder();
            if (values.getRows().isEmpty()) {
                sb.append("- EMPTY");
            } else {
                sb.append("- VALUES");
                StringBuilder valuesRow = new StringBuilder();
                for (List<ScalarOperator> row : values.getRows()) {
                    valuesRow.append("{");
                    valuesRow.append(
                            row.stream().map(new ExpressionPrinter()::print).collect(Collectors.joining(", ")));
                    valuesRow.append("}, ");
                }
                valuesRow.delete(valuesRow.length() - 2, valuesRow.length());
                sb.append(buildOutputColumns(values, valuesRow.toString()));
            }
            sb.append("\n");

            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, values, context.step);
            return new OperatorStr(sb.toString(), context.step, new ArrayList<>());
        }

        @Override
        public OperatorStr visitPhysicalRepeat(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalRepeatOperator repeat = (PhysicalRepeatOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder("- REPEAT ");
            sb.append("[");
            sb.append(repeat.getRepeatColumnRef().stream().map(groupingSets -> "[" +
                    groupingSets.stream().map(new ExpressionPrinter()::print).collect(Collectors.joining(", ")) + "]"
            ).collect(Collectors.joining(", ")));
            sb.append("]");

            Set<ColumnRefOperator> outputColumnRef = new HashSet<>(repeat.getOutputGrouping());
            for (List<ColumnRefOperator> s : repeat.getRepeatColumnRef()) {
                outputColumnRef.addAll(s);
            }
            sb.append(buildOutputColumns(repeat, "[" + outputColumnRef.stream().map(new ExpressionPrinter()::print)
                    .collect(Collectors.joining(", ")) + "]"));
            sb.append("\n");

            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, repeat, context.step);
            return new OperatorStr(sb.toString(), context.step, buildChildOperatorStr(optExpression, context.step));
        }

        @Override
        public OperatorStr visitPhysicalFilter(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalFilterOperator filter = (PhysicalFilterOperator) optExpression.getOp();
            return new OperatorStr("- PREDICATE [" + filter.getPredicate() + "]", context.step,
                    buildChildOperatorStr(optExpression, context.step));
        }

        @Override
        public OperatorStr visitPhysicalTableFunction(OptExpression optExpression,
                                                      OperatorPrinter.ExplainContext context) {
            PhysicalTableFunctionOperator tableFunction = (PhysicalTableFunctionOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder("- TABLE FUNCTION [" + tableFunction.getFn().functionName() + "]");
            sb.append(buildOutputColumns(tableFunction,
                    "[" + new ColumnRefSet(tableFunction.getOutputColRefs()) + "]"));
            sb.append("\n");

            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, tableFunction, context.step);
            return new OperatorStr(sb.toString(), context.step, buildChildOperatorStr(optExpression, context.step));
        }

        @Override
        public OperatorStr visitPhysicalDecode(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalDecodeOperator decode = (PhysicalDecodeOperator) optExpression.getOp();

            StringBuilder sb = new StringBuilder("- DECODE ")
                    .append(buildOutputColumns(decode,
                            "[" + decode.getDictToStrings().keySet().stream().map(Object::toString)
                                    .collect(Collectors.joining(", ")) + "]"))
                    .append("\n");

            for (Map.Entry<Integer, Integer> kv : decode.getDictToStrings().entrySet()) {
                buildOperatorProperty(sb, kv.getValue().toString() + " := " + kv.getKey().toString(), context.step);
            }

            for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : decode.getStringFunctions().entrySet()) {
                buildOperatorProperty(sb, new ExpressionPrinter().print(kv.getKey()) + " := " +
                        new ExpressionPrinter().print(kv.getValue()), context.step);
            }

            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, decode, context.step);
            return new OperatorStr(sb.toString(), context.step, buildChildOperatorStr(optExpression, context.step));
        }

        @Override
        public OperatorStr visitPhysicalLimit(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalLimitOperator limit = (PhysicalLimitOperator) optExpression.getOp();

            return new OperatorStr("- LIMIT [" + limit.getLimit() + "]", context.step,
                    buildChildOperatorStr(optExpression, context.step));
        }

        @Override
        public OperatorStr visitPhysicalCTEAnchor(OptExpression optExpression, ExplainContext context) {
            OperatorStr left = visit(optExpression.getInputs().get(0), new ExplainContext(context.step + 1));
            OperatorStr right = visit(optExpression.getInputs().get(1), new ExplainContext(context.step + 1));

            PhysicalCTEAnchorOperator anchor = (PhysicalCTEAnchorOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder();
            sb.append("- CTEAnchor[").append(anchor.getCteId()).append("]\n");
            buildCostEstimate(sb, optExpression, context.step);
            return new OperatorStr(sb.toString(), context.step, Arrays.asList(left, right));
        }

        @Override
        public OperatorStr visitPhysicalCTEProduce(OptExpression optExpression, ExplainContext context) {
            PhysicalCTEProduceOperator produce = (PhysicalCTEProduceOperator) optExpression.getOp();
            return new OperatorStr("- CTEProduce[" + produce.getCteId() + "]", context.step,
                    buildChildOperatorStr(optExpression, context.step));
        }

        @Override
        public OperatorStr visitPhysicalNoCTE(OptExpression optExpression, ExplainContext context) {
            PhysicalNoCTEOperator noop = (PhysicalNoCTEOperator) optExpression.getOp();
            return new OperatorStr("- CTENoOp[" + noop.getCteId() + "]", context.step,
                    buildChildOperatorStr(optExpression, context.step));
        }

        @Override
        public OperatorStr visitPhysicalCTEConsume(OptExpression optExpression, ExplainContext context) {
            PhysicalCTEConsumeOperator consume = (PhysicalCTEConsumeOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder("- CTEConsume[" + consume.getCteId() + "]\n");

            ExpressionPrinter printer = new ExpressionPrinter();
            for (Map.Entry<ColumnRefOperator, ColumnRefOperator> kv : consume.getCteOutputColumnRefMap().entrySet()) {
                String expression = "" + printer.print(kv.getKey()) + " := " + printer.print(kv.getValue());
                buildOperatorProperty(sb, expression, context.step);
            }
            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, consume, context.step);
            return new OperatorStr(sb.toString(), context.step,
                    buildChildOperatorStr(optExpression, context.step));
        }

        @Override
        public OperatorStr visitPhysicalStreamAgg(OptExpression optExpression, ExplainContext context) {
            OperatorStr child = visit(optExpression.inputAt(0), new ExplainContext(context.step + 1));
            PhysicalStreamAggOperator aggregate = (PhysicalStreamAggOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder();
            sb.append("- StreamAgg[").append(aggregate.getGroupBys().stream().map(c -> new ExpressionPrinter().print(c))
                    .collect(Collectors.joining(", "))).append("]");
            sb.append(buildOutputColumns(aggregate, ""));
            sb.append("\n");

            buildCostEstimate(sb, optExpression, context.step);

            for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregate.getAggregations().entrySet()) {
                String analyticCallString =
                        new ExpressionPrinter().print(entry.getKey()) + " := " +
                                new ExpressionPrinter().print(entry.getValue());
                buildOperatorProperty(sb, analyticCallString, context.step);
            }

            buildCommonProperty(sb, aggregate, context.step);
            return new OperatorStr(sb.toString(), context.step, Collections.singletonList(child));
        }

        @Override
        public OperatorStr visitPhysicalStreamJoin(OptExpression optExpression, ExplainContext context) {
            OperatorStr left = visit(optExpression.getInputs().get(0), new ExplainContext(context.step + 1));
            OperatorStr right = visit(optExpression.getInputs().get(1), new ExplainContext(context.step + 1));

            PhysicalStreamJoinOperator join = (PhysicalStreamJoinOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder("- StreamJoin/").append(join.getJoinType());
            if (!join.getJoinType().isCrossJoin()) {
                sb.append(" [").append(new ExpressionPrinter().print(join.getOnPredicate())).append("]");
            }
            sb.append(buildOutputColumns(join, ""));
            sb.append("\n");
            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, join, context.step);
            return new OperatorStr(sb.toString(), context.step, Arrays.asList(left, right));
        }

        @Override
        public OperatorStr visitPhysicalStreamScan(OptExpression optExpression, ExplainContext context) {
            PhysicalStreamScanOperator scan = (PhysicalStreamScanOperator) optExpression.getOp();

            StringBuilder sb = new StringBuilder("- StreamScan [")
                    .append(((OlapTable) scan.getTable()).getName())
                    .append("]")
                    .append(buildOutputColumns(scan,
                            "[" + scan.getOutputColumns().stream().map(new ExpressionPrinter()::print)
                                    .collect(Collectors.joining(", ")) + "]"))
                    .append("\n");

            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, scan, context.step);

            return new OperatorStr(sb.toString(), context.step, Collections.emptyList());
        }

        @Override
        public OperatorStr visitPhysicalTableFunctionTableScan(OptExpression optExpr, ExplainContext context) {
            PhysicalTableFunctionTableScanOperator scan = (PhysicalTableFunctionTableScanOperator) optExpr.getOp();
            StringBuilder sb = new StringBuilder("- TableFunctionScan[")
                    .append(scan.getTable().toString())
                    .append("]")
                    .append(buildOutputColumns(scan,
                            "[" + scan.getOutputColumns().stream().map(new ExpressionPrinter()::print)
                                    .collect(Collectors.joining(", ")) + "]"))
                    .append("\n");
            buildCostEstimate(sb, optExpr, context.step);
            buildCommonProperty(sb, scan, context.step);

            return new OperatorStr(sb.toString(), context.step, Collections.emptyList());
        }
    }

    public static class ExpressionPrinter
            extends ScalarOperatorVisitor<String, Void> {

        String print(ScalarOperator scalarOperator) {
            return scalarOperator.accept(this, null);
        }

        @Override
        public String visit(ScalarOperator scalarOperator, Void context) {
            return scalarOperator.accept(this, null);
        }

        @Override
        public String visitConstant(ConstantOperator literal, Void context) {
            if (literal.getType().isDatetime()) {
                LocalDateTime time = (LocalDateTime) Optional.ofNullable(literal.getValue()).orElse(LocalDateTime.MIN);
                if (time.getNano() > 0) {
                    return String.format("%04d-%02d-%02d %02d:%02d:%02d.%6d",
                            time.getYear(), time.getMonthValue(), time.getDayOfMonth(),
                            time.getHour(), time.getMinute(), time.getSecond(), time.getNano() / 1000);
                } else {
                    return String.format("%04d-%02d-%02d %02d:%02d:%02d",
                            time.getYear(), time.getMonthValue(), time.getDayOfMonth(),
                            time.getHour(), time.getMinute(), time.getSecond());
                }
            } else if (literal.getType().isDate()) {
                LocalDateTime time = (LocalDateTime) Optional.ofNullable(literal.getValue()).orElse(LocalDateTime.MIN);
                return String.format("%04d-%02d-%02d", time.getYear(), time.getMonthValue(), time.getDayOfMonth());
            } else if (literal.getType().isStringType()) {
                return "'" + literal.getValue() + "'";
            }

            return String.valueOf(literal.getValue());
        }

        @Override
        public String visitVariableReference(ColumnRefOperator variable, Void context) {
            return variable.getId() + ":" + variable.getName();
        }

        @Override
        public String visitArray(ArrayOperator array, Void context) {
            return array.getChildren().stream().map(this::print).collect(Collectors.joining(", "));
        }

        @Override
        public String visitCollectionElement(CollectionElementOperator collectSubOp, Void context) {
            return collectSubOp.getChildren().stream().map(this::print).collect(Collectors.joining(", "));
        }

        @Override
        public String visitCall(CallOperator call, Void context) {
            String fnName = call.getFnName();

            switch (fnName) {
                case FunctionSet.ADD:
                    return print(call.getChild(0)) + " + " + print(call.getChild(1));
                case FunctionSet.SUBTRACT:
                    return print(call.getChild(0)) + " - " + print(call.getChild(1));
                case FunctionSet.MULTIPLY:
                    return print(call.getChild(0)) + " * " + print(call.getChild(1));
                case FunctionSet.DIVIDE:
                    return print(call.getChild(0)) + " / " + print(call.getChild(1));
            }

            return fnName + "(" + call.getChildren().stream().map(this::print).collect(Collectors.joining(", ")) + ")";
        }

        @Override
        public String visitBetweenPredicate(BetweenPredicateOperator predicate, Void context) {
            StringBuilder sb = new StringBuilder();
            sb.append(print(predicate.getChild(0))).append(" ");

            if (predicate.isNotBetween()) {
                sb.append("NOT ");
            }

            sb.append("BETWEEN ");
            sb.append(predicate.getChild(1)).append(" AND ").append(predicate.getChild(2));
            return sb.toString();
        }

        @Override
        public String visitCloneOperator(CloneOperator operator, Void context) {
            return "CLONE(" + print(operator.getChild(0)) + ")";
        }

        @Override
        public String visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            return print(predicate.getChild(0)) + " " + predicate.getBinaryType().toString() + " " +
                    print(predicate.getChild(1));
        }

        @Override
        public String visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            if (CompoundPredicateOperator.CompoundType.NOT.equals(predicate.getCompoundType())) {
                return "NOT " + print(predicate.getChild(0));
            } else if (CompoundPredicateOperator.CompoundType.AND.equals(predicate.getCompoundType())) {

                String leftPredicate;
                if (predicate.getChild(0) instanceof CompoundPredicateOperator
                        && ((CompoundPredicateOperator) predicate.getChild(0)).getCompoundType().equals(
                        CompoundPredicateOperator.CompoundType.OR)) {
                    leftPredicate = "(" + print(predicate.getChild(0)) + ")";
                } else {
                    leftPredicate = print(predicate.getChild(0));
                }

                String rightPredicate;
                if (predicate.getChild(1) instanceof CompoundPredicateOperator
                        && ((CompoundPredicateOperator) predicate.getChild(1)).getCompoundType().equals(
                        CompoundPredicateOperator.CompoundType.OR)) {
                    rightPredicate = "(" + print(predicate.getChild(1)) + ")";
                } else {
                    rightPredicate = print(predicate.getChild(1));
                }

                return leftPredicate + " " + predicate.getCompoundType().toString() + " " + rightPredicate;
            } else {
                return print(predicate.getChild(0)) + " " + predicate.getCompoundType().toString() + " " +
                        print(predicate.getChild(1));
            }
        }

        @Override
        public String visitExistsPredicate(ExistsPredicateOperator predicate, Void context) {
            StringBuilder strBuilder = new StringBuilder();
            if (predicate.isNotExists()) {
                strBuilder.append("NOT ");

            }
            strBuilder.append("EXISTS ");
            strBuilder.append(print(predicate.getChild(0)));
            return strBuilder.toString();
        }

        @Override
        public String visitInPredicate(InPredicateOperator predicate, Void context) {
            StringBuilder sb = new StringBuilder();
            sb.append(print(predicate.getChild(0))).append(" ");
            if (predicate.isNotIn()) {
                sb.append("NOT ");
            }

            sb.append("IN (");
            sb.append(predicate.getChildren().stream().skip(1).map(this::print).collect(Collectors.joining(", ")));
            sb.append(")");
            return sb.toString();
        }

        @Override
        public String visitIsNullPredicate(IsNullPredicateOperator predicate, Void context) {
            if (!predicate.isNotNull()) {
                return new ExpressionPrinter().print(predicate.getChild(0)) + " IS NULL";
            } else {
                return new ExpressionPrinter().print(predicate.getChild(0)) + " IS NOT NULL";
            }
        }

        @Override
        public String visitLikePredicateOperator(LikePredicateOperator predicate, Void context) {
            if (LikePredicateOperator.LikeType.LIKE.equals(predicate.getLikeType())) {
                return print(predicate.getChild(0)) + " LIKE " + print(predicate.getChild(1));
            }

            return print(predicate.getChild(0)) + " REGEXP " + print(predicate.getChild(1));
        }

        @Override
        public String visitCastOperator(CastOperator operator, Void context) {
            return "cast(" + print(operator.getChild(0)) + " as " + operator.getType().toSql() + ")";
        }

        @Override
        public String visitCaseWhenOperator(CaseWhenOperator operator, Void context) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("CASE ");
            if (operator.hasCase()) {
                stringBuilder.append(new ExpressionPrinter().print(operator.getCaseClause())).append(" ");
            }

            for (int i = 0; i < operator.getWhenClauseSize(); i++) {
                stringBuilder.append("WHEN ").append(new ExpressionPrinter().print(operator.getWhenClause(i)))
                        .append(" ");
                stringBuilder.append("THEN ").append(new ExpressionPrinter().print(operator.getThenClause(i)))
                        .append(" ");
            }

            if (operator.hasElse()) {
                stringBuilder.append("ELSE ").append(new ExpressionPrinter().print(operator.getElseClause()))
                        .append(" ");
            }

            stringBuilder.append("END");
            return stringBuilder.toString();
        }

        @Override
        public String visitDictMappingOperator(DictMappingOperator operator, Void context) {
            return operator.toString();
        }
    }

    static void buildCostEstimate(StringBuilder sb, OptExpression optExpression, int step) {
        CostEstimate cost = CostModel.calculateCostEstimate(new ExpressionContext(optExpression));

        if (optExpression.getStatistics().getColumnStatistics().values().stream()
                .allMatch(ColumnStatistic::isUnknown)) {
            buildOperatorProperty(sb, "Estimates: {" +
                    "row: " + (long) optExpression.getStatistics().getOutputRowCount() +
                    ", cpu: ?, memory: ?, network: ?, cost: " + optExpression.getCost() + "}", step);
        } else {
            buildOperatorProperty(sb, "Estimates: {" +
                    "row: " + (long) optExpression.getStatistics().getOutputRowCount() +
                    ", cpu: " + String.format("%.2f", cost.getCpuCost()) +
                    ", memory: " + String.format("%.2f", cost.getMemoryCost()) +
                    ", network: " + String.format("%.2f", cost.getNetworkCost()) +
                    ", cost: " + String.format("%.2f", optExpression.getCost()) +
                    "}", step);
        }
    }

    static StringBuilder buildOutputColumns(PhysicalOperator operator, String outputColumns) {
        StringBuilder sb = new StringBuilder();
        if (operator.getProjection() != null) {
            sb.append(" => ");
            sb.append("[");
            for (ColumnRefOperator columnRefOperator : operator.getProjection().getOutputColumns()) {
                sb.append(new ExpressionPrinter().print(columnRefOperator));
                sb.append(", ");
            }
            sb.delete(sb.length() - 2, sb.length());
            sb.append("]");
        } else if (!outputColumns.isEmpty()) {
            sb.append(" => ");
            sb.append(outputColumns);
        }
        return sb;
    }

    static void buildOperatorProperty(StringBuilder sb, String property, int step) {
        sb.append(String.join("", Collections.nCopies(step + 2, "    ")))
                .append(property).append("\n");
    }

    static void buildPredicate(StringBuilder sb, PhysicalOperator operator, int step) {
        if (operator.getPredicate() != null) {
            buildOperatorProperty(sb, "predicate: " + new ExpressionPrinter().print(operator.getPredicate()), step);
        }
    }

    static void buildLimit(StringBuilder sb, PhysicalOperator operator, int step) {
        if (operator.getLimit() >= 0) {
            buildOperatorProperty(sb, "limit: " + operator.getLimit(), step);
        }
    }

    static void buildExpressionProject(StringBuilder sb, PhysicalOperator operator, int step) {
        if (operator.getProjection() != null) {
            for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : operator.getProjection().getColumnRefMap()
                    .entrySet()) {
                StringBuilder expression = new StringBuilder("");
                if (!kv.getKey().equals(kv.getValue())) {
                    expression.append(new ExpressionPrinter().print(kv.getKey())).append(" := ")
                            .append(new ExpressionPrinter().print(kv.getValue()));
                    buildOperatorProperty(sb, expression.toString(), step);
                }
            }

            for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : operator.getProjection()
                    .getCommonSubOperatorMap().entrySet()) {
                StringBuilder expression = new StringBuilder("");
                if (!kv.getKey().equals(kv.getValue())) {
                    expression.append(new ExpressionPrinter().print(kv.getKey())).append(" := ")
                            .append(new ExpressionPrinter().print(kv.getValue()))
                            .append(", ");
                    buildOperatorProperty(sb, expression.toString(), step);
                }
            }
        }
    }

    static void buildCommonProperty(StringBuilder sb, PhysicalOperator operator, int step) {
        buildExpressionProject(sb, operator, step);
        buildPredicate(sb, operator, step);
        buildLimit(sb, operator, step);
    }
}