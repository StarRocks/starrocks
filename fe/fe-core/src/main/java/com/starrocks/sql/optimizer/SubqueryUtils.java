// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.analyzer.DecimalV3FunctionAnalyzer;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;

import java.util.List;
import java.util.Map;

public class SubqueryUtils {

    public static String EXIST_NON_EQ_PREDICATE = "Not support Non-EQ correlation predicate in correlation subquery";

    public static String NONE_CORRELATED_PREDICATE = "Not support none correlation predicate in correlation subquery";

    private static Function getAggregateFunction(String functionName, Type[] argTypes) {
        Function func = Expr.getBuiltinFunction(functionName, argTypes,
                Function.CompareMode.IS_IDENTICAL);
        if (argTypes.length > 0 && argTypes[0].isDecimalV3()) {
            func =
                    DecimalV3FunctionAnalyzer.rectifyAggregationFunction((AggregateFunction) func,
                            argTypes[0],
                            argTypes[0]);
        }
        return func;
    }

    // ApplyNode doesn't need to check the number of subquery's return rows
    // when the correlation predicate meets these requirements:
    // 1. All predicate is Binary.EQ
    // 2. Only a child contains outer table's column
    // @todo: only check contains, not all
    public static boolean checkAllIsBinaryEQ(List<ScalarOperator> correlationPredicate,
                                             List<ColumnRefOperator> correlationColumnRefs) {
        for (ScalarOperator predicate : correlationPredicate) {
            if (!OperatorType.BINARY.equals(predicate.getOpType())) {
                return false;
            }

            BinaryPredicateOperator bpo = ((BinaryPredicateOperator) predicate);
            if (!BinaryPredicateOperator.BinaryType.EQ.equals(bpo.getBinaryType())) {
                return false;
            }

            ScalarOperator left = bpo.getChild(0);
            ScalarOperator right = bpo.getChild(1);

            boolean correlationLeft = Utils.containAnyColumnRefs(correlationColumnRefs, left);
            boolean correlationRight = Utils.containAnyColumnRefs(correlationColumnRefs, right);

            if (correlationLeft == correlationRight) {
                return false;
            }
        }

        return true;
    }

    public static boolean checkAllIsBinaryEQ(List<ScalarOperator> correlationPredicate) {
        for (ScalarOperator predicate : correlationPredicate) {
            if (!OperatorType.BINARY.equals(predicate.getOpType())) {
                return false;
            }

            BinaryPredicateOperator bpo = ((BinaryPredicateOperator) predicate);
            if (!BinaryPredicateOperator.BinaryType.EQ.equals(bpo.getBinaryType())) {
                return false;
            }
        }
        return true;
    }

    public static CallOperator createCountRowsOperator() {
        Function count = getAggregateFunction(FunctionSet.COUNT, new Type[] {Type.BIGINT});
        return new CallOperator(FunctionSet.COUNT, Type.BIGINT, Lists.newArrayList(ConstantOperator.createBigint(1)),
                count, false);
    }

    public static CallOperator createCountRowsOperator(ScalarOperator column) {
        Function count = getAggregateFunction(FunctionSet.COUNT, new Type[] {Type.BIGINT});
        return new CallOperator(FunctionSet.COUNT, Type.BIGINT, Lists.newArrayList(column), count, false);
    }

    public static CallOperator createAnyValueOperator(ScalarOperator column) {
        Function anyValueFn = getAggregateFunction(FunctionSet.ANY_VALUE, new Type[] {column.getType()});
        return new CallOperator(FunctionSet.ANY_VALUE, column.getType(), Lists.newArrayList(column), anyValueFn);
    }

    public static boolean isUnCorrelationScalarSubquery(LogicalApplyOperator apply) {
        if (!apply.isScalar()) {
            return false;
        }

        if (!apply.getCorrelationColumnRefs().isEmpty()) {
            return false;
        }

        // only un-correlation scalar subquery
        return apply.getUnCorrelationSubqueryPredicateColumns() != null &&
                !apply.getUnCorrelationSubqueryPredicateColumns().isEmpty();
    }

    // check the ApplyNode's children contains correlation subquery
    public static boolean containsCorrelationSubquery(OptExpression expression) {
        if (expression.getOp().isLogical() && OperatorType.LOGICAL_APPLY.equals(expression.getOp().getOpType())) {
            LogicalApplyOperator apply = (LogicalApplyOperator) expression.getOp();

            if (apply.getCorrelationColumnRefs().isEmpty()) {
                return false;
            }

            // only check right child
            return checkPredicateContainColumnRef(apply.getCorrelationColumnRefs(), expression.getInputs().get(1));
        }
        return false;
    }

    // GroupExpression
    private static boolean checkPredicateContainColumnRef(List<ColumnRefOperator> cro, OptExpression expression) {
        LogicalOperator logicalOperator = (LogicalOperator) expression.getOp();

        if (Utils.containAnyColumnRefs(cro, logicalOperator.getPredicate())) {
            return true;
        }

        for (OptExpression child : expression.getInputs()) {
            if (checkPredicateContainColumnRef(cro, child)) {
                return true;
            }
        }

        return false;
    }

    /**
     * rewrite the predicate and collect info according to your operatorShuttle
     * @param correlationPredicate
     * @param scalarOperatorShuttle
     * @return
     */
    public static ScalarOperator rewritePredicateAndExtractColumnRefs(
            ScalarOperator correlationPredicate, BaseScalarOperatorShuttle scalarOperatorShuttle) {
        if (correlationPredicate == null) {
            return null;
        }
        return correlationPredicate.clone().accept(scalarOperatorShuttle, null);
    }

    public static boolean containsExpr(Map<ColumnRefOperator, ScalarOperator> innerRefMap) {
        return innerRefMap.values().stream().anyMatch(e -> !e.isColumnRef());
    }

    public static Map<ColumnRefOperator, ScalarOperator> updateOutputColumns(
            OptExpression input, Map<ColumnRefOperator, ScalarOperator> columns, OptimizerContext context) {
        Map<ColumnRefOperator, ScalarOperator> outPutColumns = Maps.newHashMap();
        context.getColumnRefFactory().getColumnRefs(input.getOutputColumns()).stream()
                .forEach(c -> outPutColumns.put(c, c));
        outPutColumns.putAll(columns);
        return outPutColumns;
    }
}
