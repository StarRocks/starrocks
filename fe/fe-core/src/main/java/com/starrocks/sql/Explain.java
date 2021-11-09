// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql;

import com.google.common.collect.Lists;
import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.cost.CostEstimate;
import com.starrocks.sql.optimizer.cost.CostModel;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.physical.PhysicalAssertOneRowOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalExceptOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalFilterOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIntersectOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalRepeatOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalSetOperation;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTableFunctionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalUnionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalValuesOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArrayElementOperator;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ExistsPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
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
        String outputBuilder = "- Output => " + outputColumns.stream().map(c -> new ExpressionPrinter().print(c))
                .collect(Collectors.joining(", "));

        OperatorStr optStrings = new OperatorPrinter().visit(root, new OperatorPrinter.ExplainContext(1));
        OperatorStr rootOperatorStr = new OperatorStr(outputBuilder, 0, Lists.newArrayList(optStrings));
        return rootOperatorStr.toString();
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
                    .append(buildOutputColumns(scan, scan.getOutputColumns().toString()))
                    .append("\n");

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
        public OperatorStr visitPhysicalTopN(OptExpression optExpression, OperatorPrinter.ExplainContext context) {
            OperatorStr child = visit(optExpression.getInputs().get(0), new ExplainContext(context.step + 1));
            PhysicalTopNOperator topn = (PhysicalTopNOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder();
            if (topn.getLimit() == -1) {
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
                sb.append(desc.getColumns());
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
            OperatorStr left = visit(optExpression.getInputs().get(0), new ExplainContext(context.step + 1));
            OperatorStr right = visit(optExpression.getInputs().get(1), new ExplainContext(context.step + 1));

            PhysicalHashJoinOperator join = (PhysicalHashJoinOperator) optExpression.getOp();
            StringBuilder sb = new StringBuilder("- ").append(join.getJoinType());
            if (!join.getJoinType().isCrossJoin()) {
                sb.append(" [").append(new ExpressionPrinter().print(join.getJoinPredicate())).append("]");
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
            sb.append(aggregate.getGroupBys().stream().map(c -> new ExpressionPrinter().print(c))
                    .collect(Collectors.joining(", ")));

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
                    valuesRow.append(row.stream().map(new ExpressionPrinter()::print).collect(Collectors.joining(", ")));
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
            StringBuilder sb = new StringBuilder("- REPEAT");

            Set<ColumnRefOperator> outputColumnRef = new HashSet<>(repeat.getOutputGrouping());
            for (Set<ColumnRefOperator> s : repeat.getRepeatColumnRef()) {
                outputColumnRef.addAll(s);
            }
            sb.append(buildOutputColumns(repeat, outputColumnRef.toString()));
            sb.append("\n");

            buildCostEstimate(sb, optExpression, context.step);

            StringBuilder groupingSets = new StringBuilder("grouping_sets: {");
            for (Set<ColumnRefOperator> grouping : repeat.getRepeatColumnRef()) {
                groupingSets.append(grouping.toString());
                groupingSets.append(", ");
            }
            groupingSets.delete(groupingSets.length() - 2, groupingSets.length());
            groupingSets.append("}");
            buildOperatorProperty(sb, groupingSets.toString(), context.step);

            for (int i = 0; i < repeat.getOutputGrouping().size(); ++i) {
                String groupingIds = repeat.getOutputGrouping().get(i) + " := " +
                        repeat.getGroupingIds().get(i);
                buildOperatorProperty(sb, groupingIds, context.step);
            }

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
            sb.append(buildOutputColumns(tableFunction, ""));
            sb.append("\n");

            buildCostEstimate(sb, optExpression, context.step);
            buildCommonProperty(sb, tableFunction, context.step);
            return new OperatorStr(sb.toString(), context.step, buildChildOperatorStr(optExpression, context.step));
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
                return String.format("%04d-%02d-%02d %02d:%02d:%02d",
                        time.getYear(), time.getMonthValue(), time.getDayOfMonth(),
                        time.getHour(), time.getMinute(), time.getSecond());
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
        public String visitArrayElement(ArrayElementOperator array, Void context) {
            return array.getChildren().stream().map(this::print).collect(Collectors.joining(", "));
        }

        @Override
        public String visitCall(CallOperator call, Void context) {
            String fnName = call.getFnName();

            switch (fnName) {
                case "add":
                    return print(call.getChild(0)) + " + " + print(call.getChild(1));
                case "subtract":
                    return print(call.getChild(0)) + " - " + print(call.getChild(1));
                case "multiply":
                    return print(call.getChild(0)) + " * " + print(call.getChild(1));
                case "divide":
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
        public String visitBinaryPredicate(BinaryPredicateOperator predicate, Void context) {
            return print(predicate.getChild(0)) + " " + predicate.getBinaryType().toString() + " " +
                    print(predicate.getChild(1));
        }

        @Override
        public String visitCompoundPredicate(CompoundPredicateOperator predicate, Void context) {
            if (CompoundPredicateOperator.CompoundType.NOT.equals(predicate.getCompoundType())) {
                return "NOT " + print(predicate.getChild(0));
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

        public String visitLikePredicateOperator(LikePredicateOperator predicate, Void context) {
            if (LikePredicateOperator.LikeType.LIKE.equals(predicate.getLikeType())) {
                return print(predicate.getChild(0)) + " LIKE " + print(predicate.getChild(1));
            }

            return print(predicate.getChild(0)) + " REGEXP " + print(predicate.getChild(1));
        }

        public String visitCastOperator(CastOperator operator, Void context) {
            return "cast(" + print(operator.getChild(0)) + " as " + operator.getType().toSql() + ")";
        }

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
    }

    static void buildCostEstimate(StringBuilder sb, OptExpression optExpression, int step) {
        CostEstimate cost = CostModel.calculateCostEstimate(new ExpressionContext(optExpression));

        if (optExpression.getStatistics().getColumnStatistics().values().stream()
                .allMatch(ColumnStatistic::isUnknown)) {
            buildOperatorProperty(sb, "Estimates: {" +
                    "row: " + (int) optExpression.getStatistics().getOutputRowCount() +
                    ", cpu: ?, memory: ?, network: ?}", step);
        } else {
            buildOperatorProperty(sb, "Estimates: {" +
                    "row: " + (int) optExpression.getStatistics().getOutputRowCount() +
                    ", cpu: " + String.format("%.2f", cost.getCpuCost()) +
                    ", memory: " + String.format("%.2f", cost.getMemoryCost()) +
                    ", network: " + String.format("%.2f", cost.getNetworkCost()) +
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