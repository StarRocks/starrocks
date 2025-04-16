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
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HudiTable;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
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
import com.starrocks.sql.optimizer.cost.feature.FeatureExtractor;
import com.starrocks.sql.optimizer.cost.feature.PlanFeatures;
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
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamAggOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamJoinOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamScanOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Explain {
    private static final ExpressionPrinter<Void> EXPR_PRINTER = new ExpressionPrinter<>();

    public static String toString(OptExpression root, List<ColumnRefOperator> outputColumns) {
        String outputBuilder = "- Output => [" + outputColumns.stream().map(EXPR_PRINTER::print)
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

    public static PlanFeatures buildFeatures(OptExpression optExpr) {
        return FeatureExtractor.extractFeatures(optExpr);
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

    public static class OperatorPrinter extends OptExpressionVisitor<OperatorStr, OperatorPrinter.ExplainContext> {
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
        public OperatorStr visitPhysicalProject(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            return visit(optExpression.getInputs().get(0), context);
        }

        @Override
        public OperatorStr visitPhysicalOlapScan(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalOlapScanOperator scan = (PhysicalOlapScanOperator) optExpression.getOp();

            StringBuilder sb = new StringBuilder("- SCAN [")
                    .append(((OlapTable) scan.getTable()).getIndexNameById(scan.getSelectedIndexId()))
                    .append("]")
                    .append(buildOutputColumns(scan,
                            "[" + scan.getOutputColumns().stream().map(EXPR_PRINTER::print)
                                    .collect(Collectors.joining(", ")) + "]"))
                    .append("\n");

            if (scan.getTable().isMaterializedView()) {
                buildOperatorProperty(sb, "MaterializedView: true", context.step);
            }
            buildCostEstimate(sb, optExpression, context.step);

            long totalTabletsNum = scan.getNumTabletsInSelectedPartitions();
            String partitionAndBucketInfo = "partitionRatio: " +
                    scan.getSelectedPartitionId().size() +
                    "/" +
                    ((OlapTable) scan.getTable()).getVisiblePartitionNames().size() +
                    ", tabletRatio: " +
                    scan.getSelectedTabletId().size() +
                    "/" +
                    totalTabletsNum;
            buildOperatorProperty(sb, partitionAndBucketInfo, context.step);
            if (scan.getGtid() > 0) {
                buildOperatorProperty(sb, "gtid: " + scan.getGtid(), context.step);
            }
            buildCommonProperty(sb, scan, context.step);
            return new OperatorStr(sb.toString(), context.step, Collections.emptyList());
        }

        @Override
        public OperatorStr visitPhysicalHiveScan(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalHiveScanOperator scan = (PhysicalHiveScanOperator) optExpression.getOp();

            StringBuilder sb = new StringBuilder("- HIVE-SCAN [")
                    .append(((HiveTable) scan.getTable()).getCatalogTableName())
                    .append("]")
                    .append(buildOutputColumns(scan,
                            "[" + scan.getOutputColumns().stream().map(EXPR_PRINTER::print)
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
                    .append(((IcebergTable) scan.getTable()).getCatalogTableName())
                    .append("]")
                    .append(buildOutputColumns(scan,
                            "[" + scan.getOutputColumns().stream().map(EXPR_PRINTER::print)
                                    .collect(Collectors.joining(", ")) + "]"))
                    .append("\n");
            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, scan, context.step);
            return new OperatorStr(sb.toString(), context.step, Collections.emptyList());
        }

        public OperatorStr visitPhysicalHudiScan(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            PhysicalHudiScanOperator scan = (PhysicalHudiScanOperator) optExpression.getOp();

            StringBuilder sb = new StringBuilder("- Hudi-SCAN [")
                    .append(((HudiTable) scan.getTable()).getCatalogTableName())
                    .append("]")
                    .append(buildOutputColumns(scan,
                            "[" + scan.getOutputColumns().stream().map(EXPR_PRINTER::print)
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
                            "[" + scan.getOutputColumns().stream().map(EXPR_PRINTER::print)
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
                            "[" + scan.getOutputColumns().stream().map(EXPR_PRINTER::print)
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
                            "[" + scan.getOutputColumns().stream().map(EXPR_PRINTER::print)
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
                            "[" + scan.getOutputColumns().stream().map(EXPR_PRINTER::print)
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
                            "[" + scan.getOutputColumns().stream().map(EXPR_PRINTER::print)
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
                sb.append(" [").append(EXPR_PRINTER.print(join.getOnPredicate())).append("]");
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
            sb.append("[").append(aggregate.getGroupBys().stream().map(EXPR_PRINTER::print)
                    .collect(Collectors.joining(", "))).append("]");

            sb.append(buildOutputColumns(aggregate, ""));
            sb.append("\n");

            buildCostEstimate(sb, optExpression, context.step);

            for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregate.getAggregations().entrySet()) {
                String analyticCallString =
                        EXPR_PRINTER.print(entry.getKey()) + " := " +
                                EXPR_PRINTER.print(entry.getValue());
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
                sb.append(EXPR_PRINTER.print(partitionExpression));
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
                        EXPR_PRINTER.print(entry.getKey()) + " := " +
                                EXPR_PRINTER.print(entry.getValue())
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
                            row.stream().map(EXPR_PRINTER::print)
                                    .collect(Collectors.joining(", ")));
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
                    groupingSets.stream().map(EXPR_PRINTER::print).collect(Collectors.joining(", ")) + "]"
            ).collect(Collectors.joining(", ")));
            sb.append("]");

            Set<ColumnRefOperator> outputColumnRef = new HashSet<>(repeat.getOutputGrouping());
            for (List<ColumnRefOperator> s : repeat.getRepeatColumnRef()) {
                outputColumnRef.addAll(s);
            }
            sb.append(buildOutputColumns(repeat, "[" + outputColumnRef.stream().map(EXPR_PRINTER::print)
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
                            "[" + decode.getDictIdToStringsId().keySet().stream().map(Object::toString)
                                    .collect(Collectors.joining(", ")) + "]"))
                    .append("\n");

            for (Map.Entry<Integer, Integer> kv : decode.getDictIdToStringsId().entrySet()) {
                buildOperatorProperty(sb, kv.getValue().toString() + " := " + kv.getKey().toString(), context.step);
            }

            for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : decode.getStringFunctions().entrySet()) {
                buildOperatorProperty(sb, EXPR_PRINTER.print(kv.getKey()) + " := " +
                        EXPR_PRINTER.print(kv.getValue()), context.step);
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

            for (Map.Entry<ColumnRefOperator, ColumnRefOperator> kv : consume.getCteOutputColumnRefMap().entrySet()) {
                String expression = EXPR_PRINTER.print(kv.getKey()) + " := " + EXPR_PRINTER.print(kv.getValue());
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
            sb.append("- StreamAgg[").append(aggregate.getGroupBys().stream().map(EXPR_PRINTER::print)
                    .collect(Collectors.joining(", "))).append("]");
            sb.append(buildOutputColumns(aggregate, ""));
            sb.append("\n");

            buildCostEstimate(sb, optExpression, context.step);

            for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregate.getAggregations().entrySet()) {
                String analyticCallString =
                        EXPR_PRINTER.print(entry.getKey()) + " := " +
                                EXPR_PRINTER.print(entry.getValue());
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
                sb.append(" [").append(EXPR_PRINTER.print(join.getOnPredicate())).append("]");
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
                            "[" + scan.getOutputColumns().stream().map(EXPR_PRINTER::print)
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
                            "[" + scan.getOutputColumns().stream().map(EXPR_PRINTER::print)
                                    .collect(Collectors.joining(", ")) + "]"))
                    .append("\n");
            buildCostEstimate(sb, optExpr, context.step);
            buildCommonProperty(sb, scan, context.step);

            return new OperatorStr(sb.toString(), context.step, Collections.emptyList());
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
                sb.append(EXPR_PRINTER.print(columnRefOperator));
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
            buildOperatorProperty(sb, "predicate: " + EXPR_PRINTER.print(operator.getPredicate()), step);
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
                StringBuilder expression = new StringBuilder();
                if (!kv.getKey().equals(kv.getValue())) {
                    expression.append(EXPR_PRINTER.print(kv.getKey())).append(" := ")
                            .append(EXPR_PRINTER.print(kv.getValue()));
                    buildOperatorProperty(sb, expression.toString(), step);
                }
            }

            for (Map.Entry<ColumnRefOperator, ScalarOperator> kv : operator.getProjection()
                    .getCommonSubOperatorMap().entrySet()) {
                StringBuilder expression = new StringBuilder();
                if (!kv.getKey().equals(kv.getValue())) {
                    expression.append(EXPR_PRINTER.print(kv.getKey())).append(" := ")
                            .append(EXPR_PRINTER.print(kv.getValue()))
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