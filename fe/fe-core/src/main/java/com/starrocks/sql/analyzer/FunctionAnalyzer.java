// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.collect.Lists;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.Type;

public class FunctionAnalyzer {

    // geo function whose name starts_with 'st_' and is not 'st_astext' generates invalid utf8 strings. so it cannot
    // be used as a argument to a string-typed parameter of a function.
    // 1. select reverse(st_circle(...)) unacceptable
    // 2. select reverse(st_astext(st_circle(...)) acceptable
    // 3. select reverse(cast(st_circle(...) as varchar)) unacceptable
    // 4. select reverse(cast(cast(st_circle(...) as varchar) as varchar))) unacceptable
    public static void checkGeoFunctionGeneratedInvalidUtf8(FunctionCallExpr node) throws SemanticException {
        Function fn = node.getFn();
        String fnName = fn.functionName().toLowerCase();
        // A geo function can call another geo function
        if (fnName.startsWith(FunctionSet.GEO_FUNCTION_PREFIX) || !(fn instanceof ScalarFunction)) {
            return;
        }
        int numChildren = node.getChildren().size();
        Type[] args = fn.getArgs();
        for (int i = 0; i < numChildren; ++i) {
            Type childType;
            // variadic functions
            if (i > args.length - 1) {
                if (!fn.hasVarArgs()) {
                    continue;
                }
                childType = fn.getVarArgsType();
            } else {
                childType = fn.getArgs()[i];
            }

            if (!childType.isStringType()) {
                continue;
            }

            // strip cast(expr as varchar)
            Expr child = node.getChild(i);
            while (child instanceof CastExpr) {
                Type grandsonType = child.getChild(0).getType();
                if (grandsonType.isStringType()) {
                    child = child.getChild(0);
                } else {
                    break;
                }
            }
            if (child instanceof FunctionCallExpr) {
                FunctionCallExpr fnCallChild = (FunctionCallExpr) child;
                String name = fnCallChild.getFn().functionName().toLowerCase();
                // non-geo scalar function only can call st_astext function as its string-typed argument.
                if (!name.equals(FunctionSet.ST_ASTEXT) && name.startsWith(FunctionSet.GEO_FUNCTION_PREFIX)
                        && fnCallChild.getFn().getReturnType().isStringType()) {
                    throw new SemanticException(String.format("Function '%s' cannot invoke '%s'", fn.functionName(), name));
                }
            }
        }
        return;
    }

    public static void analyze(FunctionCallExpr functionCallExpr) {
        checkGeoFunctionGeneratedInvalidUtf8(functionCallExpr);
        if (functionCallExpr.getFn() instanceof AggregateFunction) {
            analyzeBuiltinAggFunction(functionCallExpr);
        }

        if (functionCallExpr.getParams().isStar() && !(functionCallExpr.getFn() instanceof AggregateFunction)) {
            throw new SemanticException("Cannot pass '*' to scalar function.");
        }

        FunctionName fnName = functionCallExpr.getFnName();
        if (fnName.getFunction().equalsIgnoreCase("date_trunc")) {
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

    }

    private static void analyzeBuiltinAggFunction(FunctionCallExpr functionCallExpr) {
        FunctionName fnName = functionCallExpr.getFnName();
        FunctionParams fnParams = functionCallExpr.getParams();

        if (fnParams.isStar() && !fnName.getFunction().equalsIgnoreCase(FunctionSet.COUNT)) {
            throw new SemanticException("'*' can only be used in conjunction with COUNT: " + functionCallExpr.toSql());
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.COUNT)) {
            // for multiple exprs count must be qualified with distinct
            if (functionCallExpr.getChildren().size() > 1 && !fnParams.isDistinct()) {
                throw new SemanticException(
                        "COUNT must have DISTINCT for multiple arguments: " + functionCallExpr.toSql());
            }
            return;
        }

        if (fnName.getFunction().equalsIgnoreCase("group_concat")) {
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

        if (fnName.getFunction().equalsIgnoreCase("lag")
                || fnName.getFunction().equalsIgnoreCase("lead")) {
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

        if (fnName.getFunction().equalsIgnoreCase("dense_rank")
                || fnName.getFunction().equalsIgnoreCase("rank")
                || fnName.getFunction().equalsIgnoreCase("row_number")
                || fnName.getFunction().equalsIgnoreCase("first_value")
                || fnName.getFunction().equalsIgnoreCase("last_value")
                || fnName.getFunction().equalsIgnoreCase("first_value_rewrite")) {
            if (!functionCallExpr.isAnalyticFnCall()) {
                throw new SemanticException(fnName.getFunction() + " only used in analytic function");
            }
        }

        // Function's arg can't be null for the following functions.
        Expr arg = functionCallExpr.getChild(0);
        if (arg == null) {
            return;
        }

        // SUM and AVG cannot be applied to non-numeric types
        if ((fnName.getFunction().equalsIgnoreCase("sum")
                || fnName.getFunction().equalsIgnoreCase("avg"))
                && ((!arg.getType().isNumericType() && !arg.getType().isBoolean() && !arg.getType().isNull() &&
                !(arg instanceof NullLiteral)) ||
                arg.getType().isOnlyMetricType())) {
            throw new SemanticException(
                    fnName.getFunction() + " requires a numeric parameter: " + functionCallExpr.toSql());
        }
        if (fnName.getFunction().equalsIgnoreCase("sum_distinct")
                && ((!arg.getType().isNumericType() && !arg.getType().isNull() && !(arg instanceof NullLiteral)) ||
                arg.getType().isOnlyMetricType())) {
            throw new SemanticException(
                    "SUM_DISTINCT requires a numeric parameter: " + functionCallExpr.toSql());
        }

        if ((fnName.getFunction().equalsIgnoreCase(FunctionSet.MIN)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.MAX)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.NDV)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.APPROX_COUNT_DISTINCT))
                && arg.getType().isOnlyMetricType()) {
            throw new SemanticException(Type.OnlyMetricTypeErrorMsg);
        }

        if ((fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_UNION_INT) && !arg.getType().isIntegerType())) {
            throw new SemanticException("BITMAP_UNION_INT params only support Integer getType()");
        }

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.INTERSECT_COUNT)) {
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

        if (fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_COUNT)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_UNION)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_UNION_COUNT)
                || fnName.getFunction().equalsIgnoreCase(FunctionSet.BITMAP_INTERSECT)) {
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

        if ((fnName.getFunction().equalsIgnoreCase("HLL_UNION_AGG")
                || fnName.getFunction().equalsIgnoreCase("HLL_UNION")
                || fnName.getFunction().equalsIgnoreCase("HLL_CARDINALITY")
                || fnName.getFunction().equalsIgnoreCase("HLL_RAW_AGG"))
                && !arg.getType().isHllType()) {
            throw new SemanticException(
                    "HLL_UNION_AGG, HLL_RAW_AGG and HLL_CARDINALITY's params must be hll column");
        }

        if (fnName.getFunction().equalsIgnoreCase("min")
                || fnName.getFunction().equalsIgnoreCase("max")) {
            fnParams.setIsDistinct(false);  // DISTINCT is meaningless here
        } else if (fnName.getFunction().equalsIgnoreCase("DISTINCT_PC")
                || fnName.getFunction().equalsIgnoreCase("DISTINCT_PCSA")
                || fnName.getFunction().equalsIgnoreCase("NDV")
                || fnName.getFunction().equalsIgnoreCase("HLL_UNION_AGG")) {
            fnParams.setIsDistinct(false);
        }

        if (fnName.getFunction().equalsIgnoreCase("percentile_approx")) {
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
