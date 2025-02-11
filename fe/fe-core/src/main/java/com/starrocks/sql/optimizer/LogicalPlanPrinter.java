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


package com.starrocks.sql.optimizer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMetaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSplitConsumeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSplitProduceOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamAggOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamJoinOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LogicalPlanPrinter {
    private LogicalPlanPrinter() {
    }

    public static String print(OptExpression root) {
        return print(root, false);

    }

    public static String print(OptExpression root, boolean isPrintTableName) {
        OperatorStr optStrings = new OperatorPrinter(isPrintTableName).visit(root);
        return optStrings.toString();
    }
    public static String print(OptExpression root, boolean isPrintTableName, boolean isPrintColumnRef) {
        OperatorStr optStrings = new OperatorPrinter(isPrintTableName, isPrintColumnRef).visit(root);
        return optStrings.toString();
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
            StringBuilder output = new StringBuilder(
                    String.join("", Collections.nCopies(step, "    ")) + operatorString);
            for (OperatorStr str : children) {
                output.append("\n");
                output.append(str);
            }
            return output.toString();
        }
    }

    private static class OperatorPrinter
            extends OptExpressionVisitor<OperatorStr, Integer> {
        // To not disturb old tests, add a flag to determine whether to print table/mv names.
        private final boolean isPrintTableName;
        private final boolean isPrintColumnRef;
        private Function<ScalarOperator, String> scalarOperatorStringFunction;

        public OperatorPrinter(boolean printTableName, boolean isPrintColumnRef) {
            this.isPrintTableName = printTableName;
            this.isPrintColumnRef = isPrintColumnRef;
            this.scalarOperatorStringFunction =
                    isPrintColumnRef ? ScalarOperator::toString : ScalarOperator::debugString;
        }

        public OperatorPrinter(boolean isPrintTableName) {
            this(isPrintTableName, false);
        }

        public OperatorStr visit(OptExpression optExpression) {
            return visit(optExpression, 0);
        }

        private OperatorStr visitDefault(OptExpression optExpression, Integer step) {
            int nextStep = step + 1;
            List<OperatorStr> children =
                    optExpression.getInputs().stream().map(input -> visit(input, nextStep)).collect(
                            Collectors.toList());
            String opName = optExpression.getOp().getClass().getSimpleName();
            List<String> nameComponents = Lists.newArrayList();
            StringBuilder comp = new StringBuilder();
            for (char ch : opName.toCharArray()) {
                if (Character.isUpperCase(ch)) {
                    if (comp.length() > 0) {
                        nameComponents.add(comp.toString().toLowerCase());
                        comp.setLength(0);
                    }
                    comp.append(ch);
                } else {
                    comp.append(ch);
                }
            }
            String lastComp = comp.toString().toLowerCase();
            if (!lastComp.isEmpty() && !lastComp.equals("operator")) {
                nameComponents.add(lastComp);
            }
            String name = String.join(" ", nameComponents);
            return new OperatorStr(name, step, children);
        }

        @Override
        public OperatorStr visit(OptExpression optExpression, Integer step) {
            return optExpression.getOp().accept(this, optExpression, step);
        }

        @Override
        public OperatorStr visitLogicalTreeAnchor(OptExpression optExpression, Integer step) {
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            return new OperatorStr("logical tree anchor", step, Collections.singletonList(child));
        }

        @Override
        public OperatorStr visitLogicalUnion(OptExpression optExpression, Integer step) {
            int nextStep = step + 1;
            List<OperatorStr> children =
                    optExpression.getInputs().stream().map(input -> visit(input, nextStep)).collect(
                            Collectors.toList());
            return new OperatorStr("logical union", step, children);
        }

        @Override
        public OperatorStr visitLogicalExcept(OptExpression optExpression, Integer step) {
            return visitDefault(optExpression, step);
        }

        @Override
        public OperatorStr visitLogicalTableFunction(OptExpression optExpression, Integer step) {
            return visitDefault(optExpression, step);
        }

        @Override
        public OperatorStr visitLogicalRepeat(OptExpression optExpression, Integer step) {
            int nextStep = step + 1;
            List<OperatorStr> children =
                    optExpression.getInputs().stream().map(input -> visit(input, nextStep)).collect(
                            Collectors.toList());
            return new OperatorStr("logical repeat", step, children);
        }

        @Override
        public OperatorStr visitLogicalIntersect(OptExpression optExpression, Integer step) {
            return visitDefault(optExpression, step);
        }

        @Override
        public OperatorStr visitLogicalCTEAnchor(OptExpression optExpression, Integer step) {
            OperatorStr leftChild = visit(optExpression.getInputs().get(0), step + 1);
            OperatorStr rightChild = visit(optExpression.getInputs().get(1), step + 1);

            return new OperatorStr("logical cte anchor", step, Arrays.asList(leftChild, rightChild));
        }

        @Override
        public OperatorStr visitLogicalCTEProduce(OptExpression optExpression, Integer step) {
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            return new OperatorStr("logical cte produce", step, Collections.singletonList(child));
        }

        @Override
        public OperatorStr visitLogicalCTEConsume(OptExpression optExpression, Integer step) {
            if (optExpression.getInputs().isEmpty()) {
                return new OperatorStr("logical cte consume", step, Collections.emptyList());
            }
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            return new OperatorStr("logical cte consume", step, Collections.singletonList(child));
        }

        @Override
        public OperatorStr visitLogicalTableScan(OptExpression optExpression, Integer step) {
            if (!isPrintColumnRef) {
                return new OperatorStr("logical scan", step, Collections.emptyList());
            }
            LogicalScanOperator scanOperator = optExpression.getOp().cast();
            return new OperatorStr("logical scan(" +
                    scanOperator.getColRefToColumnMetaMap().keySet().stream().map(col -> "" + col).collect(
                            Collectors.joining(", ")) + ")", step, Collections.emptyList());
        }

        @Override
        public OperatorStr visitLogicalValues(OptExpression optExpression, Integer step) {
            if (!isPrintColumnRef) {
                return new OperatorStr("logical values", step, Collections.emptyList());
            }
            LogicalValuesOperator valuesOperator = optExpression.getOp().cast();
            return new OperatorStr("logical value(" + valuesOperator.getColumnRefSet()
                    .stream().map(col -> "" + col).collect(Collectors.joining(", ")) + ")",
                    step, Collections.emptyList());
        }

        @Override
        public OperatorStr visitLogicalProject(OptExpression optExpression, Integer step) {
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            LogicalProjectOperator project = (LogicalProjectOperator) optExpression.getOp();
            return new OperatorStr("logical project (" +
                    project.getColumnRefMap().values().stream().map(scalarOperatorStringFunction::apply)
                            .collect(Collectors.joining(",")) + ")",
                    step, Collections.singletonList(child));
        }

        @Override
        public OperatorStr visitLogicalFilter(OptExpression optExpression, Integer step) {
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            LogicalFilterOperator filter = (LogicalFilterOperator) optExpression.getOp();
            return new OperatorStr("logical filter (" + filter.getPredicate().debugString() + ")",
                    step, ImmutableList.of(child));
        }

        @Override
        public OperatorStr visitLogicalLimit(OptExpression optExpression, Integer step) {
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            LogicalLimitOperator limit = (LogicalLimitOperator) optExpression.getOp();
            return new OperatorStr("logical limit" + " (" + limit.getLimit() + ")",
                    step, Collections.singletonList(child));
        }

        @Override
        public OperatorStr visitLogicalAggregate(OptExpression optExpression, Integer step) {
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            LogicalAggregationOperator aggregate = (LogicalAggregationOperator) optExpression.getOp();
            return new OperatorStr("logical aggregate ("
                    + aggregate.getGroupingKeys().stream().map(scalarOperatorStringFunction)
                    .collect(Collectors.joining(",")) + ") ("
                    + aggregate.getAggregations().values().stream().map(scalarOperatorStringFunction).
                    collect(Collectors.joining(",")) + ")",
                    step, Collections.singletonList(child));
        }

        @Override
        public OperatorStr visitLogicalTopN(OptExpression optExpression, Integer step) {
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            LogicalTopNOperator sort = (LogicalTopNOperator) optExpression.getOp();
            return new OperatorStr("logical sort" + " (" +
                    (sort.getOrderByElements().stream().map(Ordering::getColumnRef).collect(Collectors.toList())
                            .stream().map(ScalarOperator::debugString).collect(Collectors.joining(",")))
                    + ")", step, Collections.singletonList(child));
        }

        @Override
        public OperatorStr visitLogicalJoin(OptExpression optExpression, Integer step) {
            OperatorStr leftChild = visit(optExpression.getInputs().get(0), step + 1);
            OperatorStr rightChild = visit(optExpression.getInputs().get(1), step + 1);

            LogicalJoinOperator join = (LogicalJoinOperator) optExpression.getOp();

            StringBuilder sb = new StringBuilder();
            sb.append("logical ").append(join.getJoinType().toString().toLowerCase());
            if (join.getOnPredicate() != null) {
                sb.append(" (").append(scalarOperatorStringFunction.apply(join.getOnPredicate())).append(")");
            }

            return new OperatorStr(sb.toString(), step, Arrays.asList(leftChild, rightChild));
        }

        @Override
        public OperatorStr visitLogicalWindow(OptExpression optExpression, Integer step) {
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            LogicalWindowOperator window = optExpression.getOp().cast();
            String windowCallStr = window.getWindowCall().entrySet().stream()
                    .map(e -> String.format("%d: %s", e.getKey().getId(),
                            scalarOperatorStringFunction.apply(e.getValue())))
                    .collect(Collectors.joining(", "));
            String windowDefStr = window.getAnalyticWindow() != null ? window.getAnalyticWindow().toSql() : "NONE";
            String partitionByStr = window.getPartitionExpressions().stream()
                    .map(scalarOperatorStringFunction).collect(Collectors.joining(", "));
            String orderByStr = window.getOrderByElements().stream().map(Ordering::toString)
                    .collect(Collectors.joining(", "));
            return new OperatorStr("logical window( calls=[" +
                    windowCallStr + "], window=" +
                    windowDefStr + ", partitionBy=" +
                    partitionByStr + ", orderBy=" + orderByStr + ")", step, Collections.singletonList(child));
        }

        @Override
        public OperatorStr visitLogicalApply(OptExpression optExpression, Integer step) {
            OperatorStr leftChild = visit(optExpression.getInputs().get(0), step + 1);
            OperatorStr rightChild = visit(optExpression.getInputs().get(1), step + 1);

            LogicalApplyOperator apply = (LogicalApplyOperator) optExpression.getOp();
            return new OperatorStr("logical apply " +
                    "(" + apply.getSubqueryOperator().debugString() + ")",
                    step, Arrays.asList(leftChild, rightChild));
        }

        @Override
        public OperatorStr visitLogicalAssertOneRow(OptExpression optExpression, Integer step) {
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            LogicalAssertOneRowOperator assertOneRow = (LogicalAssertOneRowOperator) optExpression.getOp();
            return new OperatorStr(
                    "logical assert " + assertOneRow.getAssertion().name() + " " + assertOneRow.getCheckRows(), step,
                    Collections.singletonList(child));
        }

        /**
         * Physical operator visitor
         */
        public OperatorStr visitPhysicalOlapScan(OptExpression optExpression, Integer step) {
            PhysicalOlapScanOperator scan = (PhysicalOlapScanOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder("SCAN (");
            if (isPrintTableName) {
                Table scanTable = scan.getTable();
                String tableName = scan.getTable().getName();
                if (scanTable.isMaterializedView()) {
                    sb.append("mv[").append(tableName).append("] ");
                } else {
                    sb.append("table[").append(tableName).append("] ");
                }
            }
            sb.append("columns").append(scan.getColRefToColumnMetaMap().keySet());
            sb.append(" predicate[").append(scan.getPredicate()).append("]");
            sb.append(")");
            if (scan.getLimit() >= 0) {
                sb.append(" Limit ").append(scan.getLimit());
            }
            return new OperatorStr(sb.toString(), step, Collections.emptyList());
        }

        @Override
        public OperatorStr visitPhysicalMetaScan(OptExpression optExpression, Integer step) {
            PhysicalMetaScanOperator scan = (PhysicalMetaScanOperator) optExpression.getOp();
            String sb = "META SCAN (" + "columns" + scan.getUsedColumns() + ")";
            return new OperatorStr(sb, step, Collections.emptyList());
        }

        @Override
        public OperatorStr visitPhysicalSchemaScan(OptExpression optExpression, Integer step) {
            PhysicalSchemaScanOperator scan = (PhysicalSchemaScanOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder("SCAN (");
            sb.append("columns").append(scan.getUsedColumns());
            sb.append(" predicate[").append(scan.getPredicate()).append("]");
            sb.append(")");
            if (scan.getLimit() >= 0) {
                sb.append(" Limit ").append(scan.getLimit());
            }
            return new OperatorStr(sb.toString(), step, Collections.emptyList());
        }

        @Override
        public OperatorStr visitPhysicalMysqlScan(OptExpression optExpression, Integer step) {
            PhysicalMysqlScanOperator scan = (PhysicalMysqlScanOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder("SCAN (");
            sb.append("columns").append(scan.getUsedColumns());
            sb.append(" predicate[").append(scan.getPredicate()).append("]");
            sb.append(")");
            if (scan.getLimit() >= 0) {
                sb.append(" Limit ").append(scan.getLimit());
            }
            return new OperatorStr(sb.toString(), step, Collections.emptyList());
        }

        private OperatorStr visitScanCommon(OptExpression optExpression, Integer step, String scanName) {
            PhysicalScanOperator scan = (PhysicalScanOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder(scanName + " (");
            sb.append("columns").append(scan.getUsedColumns());
            sb.append(" predicate[").append(scan.getPredicate()).append("]");
            sb.append(")");
            if (scan.getLimit() >= 0) {
                sb.append(" Limit ").append(scan.getLimit());
            }
            return new OperatorStr(sb.toString(), step, Collections.emptyList());
        }

        @Override
        public OperatorStr visitPhysicalJDBCScan(OptExpression optExpression, Integer step) {
            return visitScanCommon(optExpression, step, "JDBC SCAN");
        }

        @Override
        public OperatorStr visitPhysicalHiveScan(OptExpression optExpression, Integer step) {
            return visitScanCommon(optExpression, step, "HIVE SCAN");
        }

        @Override
        public OperatorStr visitPhysicalIcebergScan(OptExpression optExpression, Integer step) {
            return visitScanCommon(optExpression, step, "ICEBERG SCAN");
        }

        @Override
        public OperatorStr visitPhysicalIcebergMetadataScan(OptExpression optExpression, Integer step) {
            return visitScanCommon(optExpression, step, "ICEBERG METADATA SCAN");
        }

        @Override
        public OperatorStr visitPhysicalIcebergEqualityDeleteScan(OptExpression optExpression, Integer step) {
            return visitScanCommon(optExpression, step, "ICEBERG EQUALITY DELETE SCAN");
        }

        @Override
        public OperatorStr visitPhysicalPaimonScan(OptExpression optExpression, Integer step) {
            return visitScanCommon(optExpression, step, "PAIMON SCAN");
        }

        public OperatorStr visitPhysicalProject(OptExpression optExpression, Integer step) {
            return visit(optExpression.getInputs().get(0), step);
        }

        public OperatorStr visitPhysicalHashAggregate(OptExpression optExpression, Integer step) {
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            PhysicalHashAggregateOperator aggregate = (PhysicalHashAggregateOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder("AGGREGATE ([").append(aggregate.getType()).append("]");
            sb.append(" aggregate [" + aggregate.getAggregations() + "]");
            sb.append(" group by [" + aggregate.getGroupBys() + "]");
            sb.append(" having [" + aggregate.getPredicate() + "]");
            return new OperatorStr(sb.toString(), step, Collections.singletonList(child));
        }

        public OperatorStr visitPhysicalTopN(OptExpression optExpression, Integer step) {
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            PhysicalTopNOperator topn = (PhysicalTopNOperator) optExpression.getOp();
            String sb = "TOP-N (" + "order by [" + topn.getOrderSpec().getOrderDescs() + "]" +
                    ")";
            return new OperatorStr(sb, step, Collections.singletonList(child));
        }

        public OperatorStr visitPhysicalDistribution(OptExpression optExpression, Integer step) {
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            PhysicalDistributionOperator exchange = (PhysicalDistributionOperator) optExpression.getOp();

            if (exchange.getDistributionSpec() instanceof HashDistributionSpec) {
                HashDistributionDesc desc =
                        ((HashDistributionSpec) exchange.getDistributionSpec()).getHashDistributionDesc();
                String s = desc.getSourceType() == HashDistributionDesc.SourceType.LOCAL ? "LOCAL" : "SHUFFLE";
                return new OperatorStr("EXCHANGE " + s + desc.getExplainInfo(), step, Collections.singletonList(child));
            }

            return new OperatorStr("EXCHANGE " + exchange.getDistributionSpec(), step,
                    Collections.singletonList(child));
        }

        public OperatorStr visitPhysicalHashJoin(OptExpression optExpression, Integer step) {
            return visitPhysicalJoin(optExpression, step);
        }

        public OperatorStr visitPhysicalMergeJoin(OptExpression optExpression, Integer step) {
            return visitPhysicalJoin(optExpression, step);
        }

        public OperatorStr visitPhysicalNestLoopJoin(OptExpression optExpression, Integer step) {
            return visitPhysicalJoin(optExpression, step);
        }

        public OperatorStr visitPhysicalJoin(OptExpression optExpression, Integer step) {
            OperatorStr leftChild = visit(optExpression.getInputs().get(0), step + 1);
            OperatorStr rightChild = visit(optExpression.getInputs().get(1), step + 1);

            PhysicalJoinOperator join = (PhysicalJoinOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder().append(join.getJoinType()).append(" (");
            sb.append("join-predicate [").append(join.getOnPredicate()).append("] ");
            sb.append("post-join-predicate [").append(join.getPredicate()).append("]");
            sb.append(")");

            return new OperatorStr(sb.toString(), step, Arrays.asList(leftChild, rightChild));
        }

        @Override
        public OperatorStr visitPhysicalAssertOneRow(OptExpression optExpression, Integer step) {
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            PhysicalAssertOneRowOperator assertOneRow = (PhysicalAssertOneRowOperator) optExpression.getOp();
            return new OperatorStr(
                    "ASSERT " + assertOneRow.getAssertion().name() + " " + assertOneRow.getCheckRows(), step,
                    Collections.singletonList(child));
        }

        @Override
        public OperatorStr visitPhysicalAnalytic(OptExpression optExpression, Integer step) {
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            PhysicalWindowOperator analytic = (PhysicalWindowOperator) optExpression.getOp();
            return new OperatorStr("ANALYTIC (" +
                    analytic.getAnalyticCall().toString() + " " +
                    analytic.getPartitionExpressions() + " " +
                    analytic.getOrderByElements() + " " +
                    (analytic.getAnalyticWindow() == null ? "" : analytic.getAnalyticWindow().toSql()) +
                    ")", step, Collections.singletonList(child));
        }

        @Override
        public OperatorStr visitPhysicalUnion(OptExpression optExpression, Integer step) {
            List<OperatorStr> children = new ArrayList<>();
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OperatorStr operatorStr = visit(optExpression.inputAt(childIdx), step + 1);
                children.add(operatorStr);
            }

            return new OperatorStr("UNION", step, children);
        }

        @Override
        public OperatorStr visitPhysicalExcept(OptExpression optExpression, Integer step) {
            List<OperatorStr> children = new ArrayList<>();
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OperatorStr operatorStr = visit(optExpression.inputAt(childIdx), step + 1);
                children.add(operatorStr);
            }

            return new OperatorStr("EXCEPT", step, children);
        }

        @Override
        public OperatorStr visitPhysicalIntersect(OptExpression optExpression, Integer step) {
            List<OperatorStr> children = new ArrayList<>();
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OperatorStr operatorStr = visit(optExpression.inputAt(childIdx), step + 1);
                children.add(operatorStr);
            }

            return new OperatorStr("INTERSECT", step, children);
        }

        @Override
        public OperatorStr visitPhysicalValues(OptExpression optExpression, Integer step) {
            PhysicalValuesOperator values = (PhysicalValuesOperator) optExpression.getOp();
            StringBuilder valuesStr = new StringBuilder("VALUES ");

            for (List<ScalarOperator> row : values.getRows()) {
                valuesStr.append("(");
                valuesStr.append(row.stream().map(ScalarOperator::debugString).collect(Collectors.joining(",")));
                valuesStr.append("),");
            }
            valuesStr.delete(valuesStr.length() - 1, valuesStr.length());

            return new OperatorStr(valuesStr.toString(), step, Collections.emptyList());
        }

        @Override
        public OperatorStr visitPhysicalRepeat(OptExpression optExpression, Integer step) {
            List<OperatorStr> children = new ArrayList<>();
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OperatorStr operatorStr = visit(optExpression.inputAt(childIdx), step + 1);
                children.add(operatorStr);
            }

            PhysicalRepeatOperator repeat = (PhysicalRepeatOperator) optExpression.getOp();

            return new OperatorStr("REPEAT " + repeat.getRepeatColumnRef(), step, children);
        }

        @Override
        public OperatorStr visitPhysicalFilter(OptExpression optExpression, Integer step) {
            List<OperatorStr> children = new ArrayList<>();
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OperatorStr operatorStr = visit(optExpression.inputAt(childIdx), step + 1);
                children.add(operatorStr);
            }

            PhysicalFilterOperator filter = (PhysicalFilterOperator) optExpression.getOp();

            return new OperatorStr("PREDICATE " + filter.getPredicate(), step, children);
        }

        @Override
        public OperatorStr visitPhysicalTableFunction(OptExpression optExpression, Integer step) {
            List<OperatorStr> children = new ArrayList<>();
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OperatorStr operatorStr = visit(optExpression.inputAt(childIdx), step + 1);
                children.add(operatorStr);
            }

            PhysicalTableFunctionOperator tableFunction = (PhysicalTableFunctionOperator) optExpression.getOp();

            String s = "TABLE FUNCTION (" + tableFunction.getFn().functionName() + ")";
            if (tableFunction.getLimit() != -1) {
                s += " LIMIT " + tableFunction.getLimit();
            }

            return new OperatorStr(s, step, children);
        }

        @Override
        public OperatorStr visitPhysicalDecode(OptExpression optExpression, Integer step) {
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            return new OperatorStr("Decode", step, Collections.singletonList(child));
        }

        @Override
        public OperatorStr visitPhysicalLimit(OptExpression optExpression, Integer step) {
            return visit(optExpression.getInputs().get(0), step);
        }

        @Override
        public OperatorStr visitPhysicalCTEAnchor(OptExpression optExpression, Integer step) {
            OperatorStr leftChild = visit(optExpression.getInputs().get(0), step + 1);
            OperatorStr rightChild = visit(optExpression.getInputs().get(1), step + 1);

            PhysicalCTEAnchorOperator op = (PhysicalCTEAnchorOperator) optExpression.getOp();
            String sb = "CTEAnchor(cteid=" + op.getCteId() + ")";
            return new OperatorStr(sb, step, Arrays.asList(leftChild, rightChild));
        }

        @Override
        public OperatorStr visitPhysicalCTEProduce(OptExpression optExpression, Integer step) {
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            PhysicalCTEProduceOperator op = (PhysicalCTEProduceOperator) optExpression.getOp();
            String sb = "CTEProducer(cteid=" + op.getCteId() + ")";
            return new OperatorStr(sb, step, Collections.singletonList(child));
        }

        @Override
        public OperatorStr visitPhysicalCTEConsume(OptExpression optExpression, Integer step) {
            PhysicalCTEConsumeOperator op = (PhysicalCTEConsumeOperator) optExpression.getOp();
            String sb = "CTEConsumer(cteid=" + op.getCteId() + ")";
            return new OperatorStr(sb, step, Collections.emptyList());
        }

        @Override
        public OperatorStr visitPhysicalNoCTE(OptExpression optExpression, Integer step) {
            return visit(optExpression.getInputs().get(0), step);
        }

        @Override
        public OperatorStr visitPhysicalStreamScan(OptExpression optExpression, Integer step) {
            // TODO
            return new OperatorStr("PhysicalStreamScan", step, Collections.emptyList());
        }

        @Override
        public OperatorStr visitPhysicalStreamJoin(OptExpression optExpression, Integer step) {
            OperatorStr leftChild = visit(optExpression.getInputs().get(0), step + 1);
            OperatorStr rightChild = visit(optExpression.getInputs().get(1), step + 1);

            PhysicalStreamJoinOperator join = (PhysicalStreamJoinOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder().append("StreamJoin/").append(join.getJoinType()).append(" (");
            sb.append("join-predicate [").append(join.getOnPredicate()).append("] ");
            sb.append("post-join-predicate [").append(join.getPredicate()).append("]");
            sb.append(")");

            return new OperatorStr(sb.toString(), step, Arrays.asList(leftChild, rightChild));
        }

        @Override
        public OperatorStr visitPhysicalStreamAgg(OptExpression optExpression, Integer step) {
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            PhysicalStreamAggOperator aggregate = (PhysicalStreamAggOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder("StreamAgg ");
            sb.append(" aggregate [" + aggregate.getAggregations() + "]");
            sb.append(" group by [" + aggregate.getGroupBys() + "]");
            sb.append(" having [" + aggregate.getPredicate() + "]");
            return new OperatorStr(sb.toString(), step, Collections.singletonList(child));
        }

        @Override
        public OperatorStr visitPhysicalConcatenater(OptExpression optExpression, Integer step) {
            return visitPhysicalUnion(optExpression, step);
        }

        @Override
        public OperatorStr visitPhysicalSplitProducer(OptExpression optExpression, Integer step) {
            OperatorStr child = visit(optExpression.getInputs().get(0), step + 1);

            PhysicalSplitProduceOperator op = (PhysicalSplitProduceOperator) optExpression.getOp();
            String sb = "SplitProducer(splitId=" + op.getSplitId() + ")";
            return new OperatorStr(sb, step, Collections.singletonList(child));
        }

        @Override
        public OperatorStr visitPhysicalSplitConsumer(OptExpression optExpression, Integer step) {
            PhysicalSplitConsumeOperator op = (PhysicalSplitConsumeOperator) optExpression.getOp();
            String sb = "SplitConsumer(cteid=" + op.getSplitId() + ")";
            return new OperatorStr(sb, step, Collections.emptyList());
        }
    }
}
