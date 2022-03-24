// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;

public class FunctionAnalyzer {

    public static void analyze(FunctionCallExpr functionCallExpr) {
        if (functionCallExpr.getFn() instanceof AggregateFunction) {
            analyzeBuiltinAggFunction(functionCallExpr);
        }

        if (functionCallExpr.getParams().isStar() && !(functionCallExpr.getFn() instanceof AggregateFunction)) {
            throw new SemanticException("Cannot pass '*' to scalar function.");
        }

        FunctionName fnName = functionCallExpr.getFnName();
        if (fnName.getFunction().equals(FunctionSet.DATE_TRUNC)) {
            if (!(functionCallExpr.getChild(0) instanceof StringLiteral)) {
                throw new SemanticException("date_trunc requires first parameter must be a string constant");
            }
            final StringLiteral fmtLiteral = (StringLiteral) functionCallExpr.getChild(0);

            if (functionCallExpr.getChild(1).getType().isDatetime()) {

                if (!Lists.newArrayList("year", "quarter", "month", "week", "day", "hour", "minute", "second")
                        .contains(fmtLiteral.getStringValue())) {
                    throw new SemanticException("date_trunc function can't support argument other than " +
                            "year|quarter|month|week|day|hour|minute|second");
                }
            } else if (functionCallExpr.getChild(1).getType().isDate()) {
                if (!Lists.newArrayList("year", "quarter", "month", "week", "day")
                        .contains(fmtLiteral.getStringValue())) {
                    throw new SemanticException("date_trunc function can't support argument other than " +
                            "year|quarter|month|week|day");
                }
            }
        }

        if (fnName.getFunction().equals(FunctionSet.ARRAY_DIFFERENCE)) {
            Preconditions.checkState(functionCallExpr.getChildren().size() == 1);
            Preconditions.checkState(functionCallExpr.getChild(0).getType().isArrayType());
            ArrayType arrayType = (ArrayType) functionCallExpr.getChild(0).getType();
            if (!arrayType.hasNumericItem()) {
                throw new SemanticException("array_difference function only support numeric array types");
            }
        }

    }

    private static void analyzeBuiltinAggFunction(FunctionCallExpr functionCallExpr) {
        FunctionName fnName = functionCallExpr.getFnName();
        FunctionParams fnParams = functionCallExpr.getParams();

        if (fnParams.isStar() && !fnName.getFunction().equals(FunctionSet.COUNT)) {
            throw new SemanticException("'*' can only be used in conjunction with COUNT: " + functionCallExpr.toSql());
        }

        if (fnName.getFunction().equals(FunctionSet.COUNT)) {
            // for multiple exprs count must be qualified with distinct
            if (functionCallExpr.getChildren().size() > 1 && !fnParams.isDistinct()) {
                throw new SemanticException(
                        "COUNT must have DISTINCT for multiple arguments: " + functionCallExpr.toSql());
            }
            return;
        }

        if (fnName.getFunction().equals(FunctionSet.GROUP_CONCAT)) {
            if (functionCallExpr.getChildren().size() > 2 || functionCallExpr.getChildren().isEmpty()) {
                throw new SemanticException(
                        "group_concat requires one or two parameters: " + functionCallExpr.toSql());
            }

            if (fnParams.isDistinct()) {
                throw new SemanticException("group_concat does not support DISTINCT");
            }

            Expr arg0 = functionCallExpr.getChild(0);
            if (!arg0.getType().isStringType() && !arg0.getType().isNull()) {
                throw new SemanticException(
                        "group_concat requires first parameter to be of getType() STRING: " + functionCallExpr.toSql());
            }

            if (functionCallExpr.getChildren().size() == 2) {
                Expr arg1 = functionCallExpr.getChild(1);
                if (!arg1.getType().isStringType() && !arg1.getType().isNull()) {
                    throw new SemanticException(
                            "group_concat requires second parameter to be of getType() STRING: " +
                                    functionCallExpr.toSql());
                }
            }
            return;
        }

        if (fnName.getFunction().equals(FunctionSet.LAG)
                || fnName.getFunction().equals(FunctionSet.LEAD)) {
            if (!functionCallExpr.isAnalyticFnCall()) {
                throw new SemanticException(fnName.getFunction() + " only used in analytic function");
            } else {
                if (functionCallExpr.getChildren().size() > 2) {
                    if (!functionCallExpr.getChild(2).isConstant()) {
                        throw new SemanticException(
                                "The default parameter (parameter 3) of LAG must be a constant: "
                                        + functionCallExpr.toSql());
                    }
                }
                return;
            }
        }

        if (fnName.getFunction().equals(FunctionSet.DENSE_RANK)
                || fnName.getFunction().equals(FunctionSet.RANK)
                || fnName.getFunction().equals(FunctionSet.ROW_NUMBER)
                || fnName.getFunction().equals(FunctionSet.FIRST_VALUE)
                || fnName.getFunction().equals(FunctionSet.LAST_VALUE)
                || fnName.getFunction().equals(FunctionSet.FIRST_VALUE_REWRITE)) {
            if (!functionCallExpr.isAnalyticFnCall()) {
                throw new SemanticException(fnName.getFunction() + " only used in analytic function");
            }
        }

        // Function's arg can't be null for the following functions.
        Expr arg = functionCallExpr.getChild(0);
        if (arg == null) {
            return;
        }

        if (fnName.getFunction().equals(FunctionSet.ARRAY_AGG)) {
            if (fnParams.isDistinct()) {
                throw new SemanticException("array_agg does not support DISTINCT");
            }
            if (arg.getType().isDecimalV3()) {
                throw new SemanticException("array_agg does not support DecimalV3");
            }
        }

        if (fnName.getFunction().equals(FunctionSet.ARRAY_OVERLAP)) {
            if (functionCallExpr.getChildren().size() != 2) {
                throw new SemanticException("array_overlap only support 2 parameters");
            }
        }

        if (fnName.getFunction().equals(FunctionSet.RETENTION)) {
            if (!arg.getType().isArrayType()) {
                throw new SemanticException("retention only support Array<BOOLEAN>");
            }
            ArrayType type = (ArrayType) arg.getType();
            if (!type.getItemType().isBoolean()) {
                throw new SemanticException("retention only support Array<BOOLEAN>");
            }
            // For Array<BOOLEAN> that have different size, we just extend result array to Compatible with it
        }

        // SUM and AVG cannot be applied to non-numeric types
        if ((fnName.getFunction().equals(FunctionSet.SUM)
                || fnName.getFunction().equals(FunctionSet.AVG))
                && ((!arg.getType().isNumericType() && !arg.getType().isBoolean() && !arg.getType().isNull() &&
                !(arg instanceof NullLiteral)) ||
                !arg.getType().canApplyToNumeric())) {
            throw new SemanticException(
                    fnName.getFunction() + " requires a numeric parameter: " + functionCallExpr.toSql());
        }
        if (fnName.getFunction().equals(FunctionSet.SUM_DISTINCT)
                && ((!arg.getType().isNumericType() && !arg.getType().isNull() && !(arg instanceof NullLiteral)) ||
                !arg.getType().canApplyToNumeric())) {
            throw new SemanticException(
                    "SUM_DISTINCT requires a numeric parameter: " + functionCallExpr.toSql());
        }

        if ((fnName.getFunction().equals(FunctionSet.MIN)
                || fnName.getFunction().equals(FunctionSet.MAX)
                || fnName.getFunction().equals(FunctionSet.NDV)
                || fnName.getFunction().equals(FunctionSet.APPROX_COUNT_DISTINCT))
                && !arg.getType().canApplyToNumeric()) {
            throw new SemanticException(Type.OnlyMetricTypeErrorMsg);
        }

        if ((fnName.getFunction().equals(FunctionSet.BITMAP_UNION_INT) && !arg.getType().isIntegerType())) {
            throw new SemanticException("BITMAP_UNION_INT params only support Integer getType()");
        }

        if (fnName.getFunction().equals(FunctionSet.INTERSECT_COUNT)) {
            if (functionCallExpr.getChildren().size() <= 2) {
                throw new SemanticException("intersect_count(bitmap_column, column_to_filter, filter_values) " +
                        "function requires at least three parameters");
            }

            Type inputType = functionCallExpr.getChild(0).getType();
            if (!inputType.isBitmapType()) {
                throw new SemanticException(
                        "intersect_count function first argument should be of BITMAP getType(), but was " + inputType);
            }

            if (functionCallExpr.getChild(1).isConstant()) {
                throw new SemanticException("intersect_count function filter_values arg must be column");
            }

            for (int i = 2; i < functionCallExpr.getChildren().size(); i++) {
                if (!functionCallExpr.getChild(i).isConstant()) {
                    throw new SemanticException("intersect_count function filter_values arg must be constant");
                }
            }
            return;
        }

        if (fnName.getFunction().equals(FunctionSet.BITMAP_COUNT)
                || fnName.getFunction().equals(FunctionSet.BITMAP_UNION)
                || fnName.getFunction().equals(FunctionSet.BITMAP_UNION_COUNT)
                || fnName.getFunction().equals(FunctionSet.BITMAP_INTERSECT)) {
            if (functionCallExpr.getChildren().size() != 1) {
                throw new SemanticException(fnName + " function could only have one child");
            }
            Type inputType = functionCallExpr.getChild(0).getType();
            if (!inputType.isBitmapType()) {
                throw new SemanticException(
                        fnName + " function's argument should be of BITMAP getType(), but was " + inputType);
            }
            return;
        }

        if ((fnName.getFunction().equals(FunctionSet.HLL_UNION_AGG)
                || fnName.getFunction().equals(FunctionSet.HLL_UNION)
                || fnName.getFunction().equals(FunctionSet.HLL_CARDINALITY)
                || fnName.getFunction().equals(FunctionSet.HLL_RAW_AGG))
                && !arg.getType().isHllType()) {
            throw new SemanticException(
                    "HLL_UNION_AGG, HLL_RAW_AGG and HLL_CARDINALITY's params must be hll column");
        }

        if (fnName.getFunction().equals(FunctionSet.MIN)
                || fnName.getFunction().equals(FunctionSet.MAX)
                || fnName.getFunction().equals(FunctionSet.NDV)
                || fnName.getFunction().equals(FunctionSet.HLL_UNION_AGG)) {
            fnParams.setIsDistinct(false);  // DISTINCT is meaningless here
        }

        if (fnName.getFunction().equals(FunctionSet.PERCENTILE_APPROX)) {
            if (functionCallExpr.getChildren().size() != 2 && functionCallExpr.getChildren().size() != 3) {
                throw new SemanticException("percentile_approx(expr, DOUBLE [, B]) requires two or three parameters");
            }
            if (!functionCallExpr.getChild(1).isConstant()) {
                throw new SemanticException("percentile_approx requires second parameter must be a constant : "
                        + functionCallExpr.toSql());
            }
            if (functionCallExpr.getChildren().size() == 3) {
                if (!functionCallExpr.getChild(2).isConstant()) {
                    throw new SemanticException("percentile_approx requires the third parameter must be a constant : "
                            + functionCallExpr.toSql());
                }
            }
        }
    }
}
