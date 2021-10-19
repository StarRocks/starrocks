// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer;

import com.google.common.collect.ImmutableList;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalMysqlScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class OperatorStrings {
    public String printOperator(OptExpression root) {
        OperatorStr optStrings = new OperatorPrinter().visit(root);
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

    public static class OperatorPrinter
            extends OptExpressionVisitor<OperatorStr, Integer> {

        public OperatorStr visit(OptExpression optExpression) {
            return visit(optExpression, 0);
        }

        @Override
        public OperatorStr visit(OptExpression optExpression, Integer step) {
            return optExpression.getOp().accept(this, optExpression, step);
        }

        @Override
        public OperatorStr visitLogicalTableScan(OptExpression optExpression, Integer step) {
            return new OperatorStr("logical scan", step, Collections.emptyList());
        }

        @Override
        public OperatorStr visitLogicalProject(OptExpression optExpression, Integer step) {
            OperatorStr strBuilder = visit(optExpression.getInputs().get(0), step + 1);

            LogicalProjectOperator project = (LogicalProjectOperator) optExpression.getOp();

            return new OperatorStr("logical project (" +
                    project.getColumnRefMap().values().stream().map(ScalarOperator::debugString)
                            .collect(Collectors.joining(",")) + ")"
                    , step, Collections.singletonList(strBuilder));
        }

        @Override
        public OperatorStr visitLogicalFilter(OptExpression optExpression, Integer step) {
            OperatorStr strBuilder = visit(optExpression.getInputs().get(0), step + 1);

            LogicalFilterOperator filter = (LogicalFilterOperator) optExpression.getOp();
            return new OperatorStr("logical filter (" + filter.getPredicate().debugString() + ")",
                    step, ImmutableList.of(strBuilder));
        }

        @Override
        public OperatorStr visitLogicalLimit(OptExpression optExpression, Integer step) {
            OperatorStr strBuilder = visit(optExpression.getInputs().get(0), step + 1);

            LogicalLimitOperator limit = (LogicalLimitOperator) optExpression.getOp();
            return new OperatorStr("logical limit" + " (" + limit.getLimit() + ")",
                    step, Collections.singletonList(strBuilder));
        }

        @Override
        public OperatorStr visitLogicalAggregate(OptExpression optExpression, Integer step) {
            OperatorStr strBuilder = visit(optExpression.getInputs().get(0), step + 1);

            LogicalAggregationOperator aggregate = (LogicalAggregationOperator) optExpression.getOp();
            return new OperatorStr("logical aggregate ("
                    + aggregate.getGroupingKeys().stream().map(ScalarOperator::debugString)
                    .collect(Collectors.joining(",")) + ") ("
                    + aggregate.getAggregations().values().stream().map(CallOperator::debugString).
                    collect(Collectors.joining(",")) + ")"
                    , step, Collections.singletonList(strBuilder));
        }

        @Override
        public OperatorStr visitLogicalTopN(OptExpression optExpression, Integer step) {
            OperatorStr strBuilder = visit(optExpression.getInputs().get(0), step + 1);

            LogicalTopNOperator sort = (LogicalTopNOperator) optExpression.getOp();
            return new OperatorStr("logical sort" + " (" +
                    (sort.getOrderByElements().stream().map(Ordering::getColumnRef).collect(Collectors.toList())
                            .stream().map(ScalarOperator::debugString).collect(Collectors.joining(",")))
                    + ")", step, Collections.singletonList(strBuilder));
        }

        @Override
        public OperatorStr visitLogicalJoin(OptExpression optExpression, Integer step) {
            OperatorStr left = visit(optExpression.getInputs().get(0), step + 1);
            OperatorStr right = visit(optExpression.getInputs().get(1), step + 1);

            LogicalJoinOperator join = (LogicalJoinOperator) optExpression.getOp();

            StringBuilder sb = new StringBuilder();
            sb.append("logical ").append(join.getJoinType().toString().toLowerCase());
            if (join.getOnPredicate() != null) {
                sb.append(" (").append(join.getOnPredicate().debugString()).append(")");
            }

            return new OperatorStr(sb.toString(), step, Arrays.asList(left, right));
        }

        @Override
        public OperatorStr visitLogicalApply(OptExpression optExpression, Integer step) {
            OperatorStr left = visit(optExpression.getInputs().get(0), step + 1);
            OperatorStr right = visit(optExpression.getInputs().get(1), step + 1);

            LogicalApplyOperator apply = (LogicalApplyOperator) optExpression.getOp();
            return new OperatorStr("logical apply " +
                    "(" + apply.getSubqueryOperator().debugString() + ")"
                    , step, Arrays.asList(left, right));
        }

        @Override
        public OperatorStr visitLogicalAssertOneRow(OptExpression optExpression, Integer step) {
            OperatorStr left = visit(optExpression.getInputs().get(0), step + 1);

            LogicalAssertOneRowOperator assertOneRow = (LogicalAssertOneRowOperator) optExpression.getOp();
            return new OperatorStr(
                    "logical assert " + assertOneRow.getAssertion().name() + " " + assertOneRow.getCheckRows(), step,
                    Collections.singletonList(left));
        }

        /**
         * Physical operator visitor
         */
        public OperatorStr visitPhysicalOlapScan(OptExpression optExpression, Integer step) {
            PhysicalOlapScanOperator scan = (PhysicalOlapScanOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder("SCAN (");
            sb.append("columns").append(scan.getOutputColumns());
            sb.append(" predicate[").append(scan.getPredicate()).append("]");
            sb.append(")");
            if (scan.getLimit() >= 0) {
                sb.append(" Limit ").append(scan.getLimit());
            }
            return new OperatorStr(sb.toString(), step, Collections.emptyList());
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
                return new OperatorStr("EXCHANGE " + s + desc.getColumns(), step, Collections.singletonList(child));
            }

            return new OperatorStr("EXCHANGE " + exchange.getDistributionSpec(), step,
                    Collections.singletonList(child));
        }

        public OperatorStr visitPhysicalHashJoin(OptExpression optExpression, Integer step) {
            OperatorStr left = visit(optExpression.getInputs().get(0), step + 1);
            OperatorStr right = visit(optExpression.getInputs().get(1), step + 1);

            PhysicalHashJoinOperator join = (PhysicalHashJoinOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder("").append(join.getJoinType()).append(" (");
            sb.append("join-predicate [").append(join.getJoinPredicate()).append("] ");
            sb.append("post-join-predicate [").append(join.getPredicate()).append("]");
            sb.append(")");

            return new OperatorStr(sb.toString(), step, Arrays.asList(left, right));
        }

        @Override
        public OperatorStr visitPhysicalAssertOneRow(OptExpression optExpression, Integer step) {
            OperatorStr left = visit(optExpression.getInputs().get(0), step + 1);

            PhysicalAssertOneRowOperator assertOneRow = (PhysicalAssertOneRowOperator) optExpression.getOp();
            return new OperatorStr(
                    "ASSERT " + assertOneRow.getAssertion().name() + " " + assertOneRow.getCheckRows(), step,
                    Collections.singletonList(left));
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
            List<OperatorStr> childString = new ArrayList<>();
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OperatorStr operatorStr = visit(optExpression.inputAt(childIdx), step + 1);
                childString.add(operatorStr);
            }

            return new OperatorStr("UNION", step, childString);
        }

        @Override
        public OperatorStr visitPhysicalExcept(OptExpression optExpression, Integer step) {
            List<OperatorStr> childString = new ArrayList<>();
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OperatorStr operatorStr = visit(optExpression.inputAt(childIdx), step + 1);
                childString.add(operatorStr);
            }

            return new OperatorStr("EXCEPT", step, childString);
        }

        @Override
        public OperatorStr visitPhysicalIntersect(OptExpression optExpression, Integer step) {
            List<OperatorStr> childString = new ArrayList<>();
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OperatorStr operatorStr = visit(optExpression.inputAt(childIdx), step + 1);
                childString.add(operatorStr);
            }

            return new OperatorStr("INTERSECT", step, childString);
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

            return new OperatorStr(valuesStr.toString(), step, new ArrayList<>());
        }

        @Override
        public OperatorStr visitPhysicalRepeat(OptExpression optExpression, Integer step) {
            List<OperatorStr> childString = new ArrayList<>();
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OperatorStr operatorStr = visit(optExpression.inputAt(childIdx), step + 1);
                childString.add(operatorStr);
            }

            PhysicalRepeatOperator repeat = (PhysicalRepeatOperator) optExpression.getOp();

            return new OperatorStr("REPEAT " + repeat.getRepeatColumnRef(), step, new ArrayList<>(childString));
        }

        @Override
        public OperatorStr visitPhysicalFilter(OptExpression optExpression, Integer step) {
            List<OperatorStr> childString = new ArrayList<>();
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OperatorStr operatorStr = visit(optExpression.inputAt(childIdx), step + 1);
                childString.add(operatorStr);
            }

            PhysicalFilterOperator filter = (PhysicalFilterOperator) optExpression.getOp();

            return new OperatorStr("PREDICATE " + filter.getPredicate(), step, new ArrayList<>(childString));
        }

        @Override
        public OperatorStr visitPhysicalTableFunction(OptExpression optExpression, Integer step) {
            List<OperatorStr> childString = new ArrayList<>();
            for (int childIdx = 0; childIdx < optExpression.getInputs().size(); ++childIdx) {
                OperatorStr operatorStr = visit(optExpression.inputAt(childIdx), step + 1);
                childString.add(operatorStr);
            }

            PhysicalTableFunctionOperator tableFunction = (PhysicalTableFunctionOperator) optExpression.getOp();

            String s = "TABLE FUNCTION (" + tableFunction.getFn().functionName() + ")";
            if (tableFunction.getLimit() != -1) {
                s += " LIMIT " + tableFunction.getLimit();
            }

            return new OperatorStr(s, step, new ArrayList<>(childString));
        }
        @Override
        public OperatorStr visitPhysicalLimit(OptExpression optExpression, Integer step) {
            return visit(optExpression.getInputs().get(0), step);
        }
    }
}
