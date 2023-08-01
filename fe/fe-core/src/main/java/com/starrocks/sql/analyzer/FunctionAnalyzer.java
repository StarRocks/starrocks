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

package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;

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

            // The uppercase parameters stored in the metadata are converted to lowercase parameters,
            // because BE does not support uppercase parameters
            final String lowerParam = ((StringLiteral) functionCallExpr.getChild(0)).getValue().toLowerCase();
            functionCallExpr.setChild(0, new StringLiteral(lowerParam));

            if (functionCallExpr.getChild(1).getType().isDatetime()) {

                if (!Lists.newArrayList("year", "quarter", "month", "week", "day", "hour", "minute", "second")
                        .contains(lowerParam)) {
                    throw new SemanticException("date_trunc function can't support argument other than " +
                            "year|quarter|month|week|day|hour|minute|second");
                }
            } else if (functionCallExpr.getChild(1).getType().isDate()) {
                if (!Lists.newArrayList("year", "quarter", "month", "week", "day")
                        .contains(lowerParam)) {
                    throw new SemanticException("date_trunc function can't support argument other than " +
                            "year|quarter|month|week|day");
                }
            }
        }

        if (fnName.getFunction().equals(FunctionSet.ARRAY_DIFFERENCE)) {
            Preconditions.checkState(functionCallExpr.getChildren().size() == 1);
            if (!functionCallExpr.getChild(0).getType().isNull()) {
                Preconditions.checkState(functionCallExpr.getChild(0).getType().isArrayType());
                ArrayType arrayType = (ArrayType) functionCallExpr.getChild(0).getType();
                if (!arrayType.hasNumericItem() && !arrayType.isBooleanType() && !arrayType.isNullTypeItem()) {
                    throw new SemanticException("array_difference function only support numeric array types");
                }
            }
        }

        if (fnName.getFunction().equals(FunctionSet.ARRAY_MAP)) {
            Preconditions.checkState(functionCallExpr.getChildren().size() > 1,
                    "array_map should have at least two inputs");
            Preconditions.checkState(functionCallExpr.getChild(0).getChild(0) != null,
                    "array_map's lambda function can not be null");
            // the normalized high_order functions:
            // high-order function(lambda_func(lambda_expr, lambda_arguments), input_arrays),
            // which puts various arguments/inputs at the tail e.g.,
            // array_map(x+y <- (x,y), arr1, arr2)
            functionCallExpr.setType(new ArrayType(functionCallExpr.getChild(0).getChild(0).getType()));
        } else if (fnName.getFunction().equals(FunctionSet.MAP_APPLY)) {
            Preconditions.checkState(functionCallExpr.getChildren().size() > 1,
                    "map_apply should have at least two inputs");
            Preconditions.checkState(functionCallExpr.getChild(0).getChild(0) != null,
                    "map_apply's lambda function can not be null");
            functionCallExpr.setType(functionCallExpr.getChild(0).getChild(0).getType());
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

        if (FunctionSet.onlyAnalyticUsedFunctions.contains(fnName.getFunction())) {
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
        }

        if (fnName.getFunction().equals(FunctionSet.ARRAYS_OVERLAP)) {
            if (functionCallExpr.getChildren().size() != 2) {
                throw new SemanticException("arrays_overlap only support 2 parameters");
            }
        }

        if (fnName.getFunction().equals(FunctionSet.ARRAY_FILTER)) {
            if (functionCallExpr.getChildren().size() != 2) {
                throw new SemanticException("array_filter only support 2 parameters");
            }
        }

        if (fnName.getFunction().equals(FunctionSet.ARRAY_SORTBY)) {
            if (functionCallExpr.getChildren().size() != 2) {
                throw new SemanticException("array_sortby only support 2 parameters");
            }
        }

        if (fnName.getFunction().equals(FunctionSet.RETENTION)) {
            if (!arg.getType().isArrayType()) {
                throw new SemanticException("retention only support Array<BOOLEAN>");
            }
            ArrayType type = (ArrayType) arg.getType();
            if (!type.getItemType().isNull() && !type.getItemType().isBoolean()) {
                throw new SemanticException("retention only support Array<BOOLEAN>");
            }
            // For Array<BOOLEAN> that have different size, we just extend result array to Compatible with it
        }

        if (fnName.getFunction().equals(FunctionSet.WINDOW_FUNNEL)) {
            Expr modeArg = functionCallExpr.getChild(2);
            if (modeArg instanceof IntLiteral) {
                IntLiteral modeIntLiteral = (IntLiteral) modeArg;
                long modeValue = modeIntLiteral.getValue();
                if (modeValue < 0 || modeValue > 7) {
                    throw new SemanticException("mode argument's range must be [0-7]");
                }
            } else {
                throw new SemanticException("mode argument must be numerical type");
            }

            Expr windowArg = functionCallExpr.getChild(0);
            if (windowArg instanceof IntLiteral) {
                IntLiteral windowIntLiteral = (IntLiteral) windowArg;
                long windowValue = windowIntLiteral.getValue();
                if (windowValue < 0) {
                    throw new SemanticException("window argument must >= 0");
                }
            } else {
                throw new SemanticException("window argument must be numerical type");
            }

            Expr timeExpr = functionCallExpr.getChild(1);
            if (timeExpr.isConstant()) {
                throw new SemanticException("time arg must be column");
            }
        }

        if (fnName.getFunction().equals(FunctionSet.MAX_BY)) {
            if (functionCallExpr.getChildren().size() != 2 || functionCallExpr.getChildren().isEmpty()) {
                throw new SemanticException(
                        "max_by requires two parameters: " + functionCallExpr.toSql());
            }

            if (functionCallExpr.getChild(0).isConstant() || functionCallExpr.getChild(1).isConstant()) {
                throw new SemanticException("max_by function args must be column");
            }

            fnParams.setIsDistinct(false);  // DISTINCT is meaningless here

            Type sortKeyType = functionCallExpr.getChild(1).getType();
            if (!sortKeyType.canApplyToNumeric()) {
                throw new SemanticException(Type.ONLY_METRIC_TYPE_ERROR_MSG);
            }

            return;
        }

        // SUM and AVG cannot be applied to non-numeric types
        if ((fnName.getFunction().equals(FunctionSet.SUM)
                || fnName.getFunction().equals(FunctionSet.AVG))
                && ((!arg.getType().isNumericType() && !arg.getType().isBoolean()
                && !arg.getType().isStringType() && !arg.getType().isNull() &&
                !(arg instanceof NullLiteral)) ||
                !arg.getType().canApplyToNumeric())) {
            throw new SemanticException(
                    fnName.getFunction() + " requires a numeric parameter: " + functionCallExpr.toSql());
        }

        if ((fnName.getFunction().equals(FunctionSet.MIN)
                || fnName.getFunction().equals(FunctionSet.MAX)
                || fnName.getFunction().equals(FunctionSet.NDV)
                || fnName.getFunction().equals(FunctionSet.APPROX_COUNT_DISTINCT))
                && !arg.getType().canApplyToNumeric()) {
            throw new SemanticException(Type.ONLY_METRIC_TYPE_ERROR_MSG);
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

        if (fnName.getFunction().equals(FunctionSet.BITMAP_AGG)) {
            if (functionCallExpr.getChildren().size() != 1) {
                throw new SemanticException(fnName + " function could only have one child");
            }
            Type inputType = functionCallExpr.getChild(0).getType();
            if (!inputType.isIntegerType() && !inputType.isBoolean() && !inputType.isLargeIntType()
                    && !inputType.isStringType()) {
                throw new SemanticException(
                        fnName + " function's argument should be of int type or bool type or string type, but was "
                                + inputType);
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
            if (!functionCallExpr.getChild(0).getType().isNumericType()) {
                throw new SemanticException(
                        "percentile_approx requires the first parameter's type is numeric type");
            }
            if (!functionCallExpr.getChild(1).getType().isNumericType() ||
                    !functionCallExpr.getChild(1).isConstant()) {
                throw new SemanticException(
                        "percentile_approx requires the second parameter's type is numeric constant type");
            }

            if (functionCallExpr.getChildren().size() == 3) {
                if (!functionCallExpr.getChild(2).getType().isNumericType() ||
                        !functionCallExpr.getChild(2).isConstant()) {
                    throw new SemanticException(
                            "percentile_approx requires the third parameter's type is numeric constant type");
                }
            }
        }

        if (fnName.getFunction().equals(FunctionSet.EXCHANGE_BYTES) ||
                fnName.getFunction().equals(FunctionSet.EXCHANGE_SPEED)) {
            if (ConnectContext.get().getSessionVariable().getNewPlannerAggStage() != 1) {
                throw new SemanticException(fnName.getFunction() + " should run in new_planner_agg_stage = 1.");
            }
        }

        if (fnName.getFunction().equals(FunctionSet.COVAR_POP) || fnName.getFunction().equals(FunctionSet.COVAR_SAMP) ||
                fnName.getFunction().equals(FunctionSet.CORR)) {
            if (functionCallExpr.getChildren().size() != 2) {
                throw new SemanticException(fnName + " function should have two args", functionCallExpr.getPos());
            }
            if (functionCallExpr.getChild(0).isConstant() || functionCallExpr.getChild(1).isConstant()) {
                throw new SemanticException(fnName + " function 's args must be column");
            }
        }
    }
}
