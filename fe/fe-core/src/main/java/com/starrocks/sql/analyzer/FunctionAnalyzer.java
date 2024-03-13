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
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.ExprUtil;
import com.starrocks.qe.ConnectContext;

public class FunctionAnalyzer {

    public static void analyze(FunctionCallExpr functionCallExpr) {
        if (functionCallExpr.getFn() instanceof AggregateFunction) {
            analyzeBuiltinAggFunction(functionCallExpr);
        }

        if (functionCallExpr.getParams().isStar() && !(functionCallExpr.getFn() instanceof AggregateFunction)) {
            throw new SemanticException("Cannot pass '*' to scalar function.", functionCallExpr.getPos());
        }

        FunctionName fnName = functionCallExpr.getFnName();
        if (fnName.getFunction().equals(FunctionSet.DATE_TRUNC)) {
            if (!(functionCallExpr.getChild(0) instanceof StringLiteral)) {
                throw new SemanticException("date_trunc requires first parameter must be a string constant",
                        functionCallExpr.getChild(0).getPos());
            }

            // The uppercase parameters stored in the metadata are converted to lowercase parameters,
            // because BE does not support uppercase parameters
            final String lowerParam = ((StringLiteral) functionCallExpr.getChild(0)).getValue().toLowerCase();
            functionCallExpr.setChild(0, new StringLiteral(lowerParam, functionCallExpr.getChild(0).getPos()));

            if (functionCallExpr.getChild(1).getType().isDatetime()) {

                if (!Lists.newArrayList("year", "quarter", "month", "week", "day", "hour", "minute", "second")
                        .contains(lowerParam)) {
                    throw new SemanticException("date_trunc function can't support argument other than " +
                            "year|quarter|month|week|day|hour|minute|second", functionCallExpr.getChild(0).getPos());
                }
            } else if (functionCallExpr.getChild(1).getType().isDate()) {
                if (!Lists.newArrayList("year", "quarter", "month", "week", "day")
                        .contains(lowerParam)) {
                    throw new SemanticException("date_trunc function can't support argument other than " +
                            "year|quarter|month|week|day", functionCallExpr.getChild(1).getPos());
                }
            }
        }

        if (fnName.getFunction().equals(FunctionSet.ARRAY_DIFFERENCE)) {
            Preconditions.checkState(functionCallExpr.getChildren().size() == 1);
            if (!functionCallExpr.getChild(0).getType().isNull()) {
                Preconditions.checkState(functionCallExpr.getChild(0).getType().isArrayType());
                ArrayType arrayType = (ArrayType) functionCallExpr.getChild(0).getType();
                if (!arrayType.hasNumericItem() && !arrayType.isBooleanType() && !arrayType.isNullTypeItem()) {
                    throw new SemanticException("array_difference function only support numeric array types",
                            functionCallExpr.getChild(0).getPos());
                }
            }
        }

        if (fnName.getFunction().equals(FunctionSet.ARRAY_MAP)) {
            Preconditions.checkState(functionCallExpr.getChildren().size() > 1,
                    "array_map should have at least two inputs", functionCallExpr.getPos());
            Preconditions.checkState(functionCallExpr.getChild(0).getChild(0) != null,
                    "array_map's lambda function can not be null", functionCallExpr.getPos());
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
            throw new SemanticException("'*' can only be used in conjunction with COUNT: " + functionCallExpr.toSql(),
                    functionCallExpr.getPos());
        }

        if (fnName.getFunction().equals(FunctionSet.COUNT)) {
            // for multiple exprs count must be qualified with distinct
            if (functionCallExpr.getChildren().size() > 1 && !fnParams.isDistinct()) {
                throw new SemanticException(
                        "COUNT must have DISTINCT for multiple arguments: " + functionCallExpr.toSql(),
                        functionCallExpr.getPos());
            }
            return;
        }

        if (fnName.getFunction().equals(FunctionSet.COUNT_IF) && fnParams.isDistinct()) {
            throw new SemanticException("COUNT_IF does not support DISTINCT", functionCallExpr.getPos());
        }

        if (fnName.getFunction().equals(FunctionSet.GROUP_CONCAT)) {
            if (functionCallExpr.getChildren().size() - fnParams.getOrderByElemNum() < 2) {
                throw new SemanticException(
                        "group_concat requires at least one parameter: " + functionCallExpr.toSql(),
                        functionCallExpr.getPos());
            }

            int sepPos = functionCallExpr.getParams().exprs().size() - functionCallExpr.getParams().getOrderByElemNum() - 1;
            Expr arg1 = functionCallExpr.getChild(sepPos);
            if (!arg1.getType().isStringType() && !arg1.getType().isNull()) {
                throw new SemanticException(
                        "group_concat requires separator to be of getType() STRING: " +
                                functionCallExpr.toSql(), arg1.getPos());
            }
            return;
        }

        if (fnName.getFunction().equals(FunctionSet.LAG)
                || fnName.getFunction().equals(FunctionSet.LEAD)) {
            if (!functionCallExpr.isAnalyticFnCall()) {
                throw new SemanticException(fnName.getFunction() + " only used in analytic function", functionCallExpr.getPos());
            } else {
                if (functionCallExpr.getChildren().size() > 2) {
                    if (!functionCallExpr.getChild(2).isConstant()) {
                        throw new SemanticException(
                                "The default parameter (parameter 3) of LAG must be a constant: "
                                        + functionCallExpr.toSql(), functionCallExpr.getChild(2).getPos());
                    }
                }
                return;
            }
        }

        if (fnName.getFunction().equals(FunctionSet.SESSION_NUMBER)) {
            if (!functionCallExpr.getChild(1).isConstant()) {
                throw new SemanticException(
                        "The delta parameter (parameter 2) of SESSION_NUMBER must be a constant: "
                                + functionCallExpr.toSql(), functionCallExpr.getChild(1).getPos());
            }
        }

        if (FunctionSet.onlyAnalyticUsedFunctions.contains(fnName.getFunction())) {
            if (!functionCallExpr.isAnalyticFnCall()) {
                throw new SemanticException(fnName.getFunction() + " only used in analytic function", functionCallExpr.getPos());
            }
        }

        // Function's arg can't be null for the following functions.
        Expr arg = functionCallExpr.getChild(0);
        if (arg == null) {
            return;
        }

        if (fnName.getFunction().equals(FunctionSet.RETENTION)) {
            if (!arg.getType().isArrayType()) {
                throw new SemanticException("retention only support Array<BOOLEAN>", arg.getPos());
            }
            ArrayType type = (ArrayType) arg.getType();
            if (!type.getItemType().isNull() && !type.getItemType().isBoolean()) {
                throw new SemanticException("retention only support Array<BOOLEAN>", arg.getPos());
            }
            // For Array<BOOLEAN> that have different size, we just extend result array to Compatible with it
        }

        if (fnName.getFunction().equals(FunctionSet.WINDOW_FUNNEL)) {
            Expr modeArg = functionCallExpr.getChild(2);
            if (modeArg instanceof IntLiteral) {
                IntLiteral modeIntLiteral = (IntLiteral) modeArg;
                long modeValue = modeIntLiteral.getValue();
                if (modeValue < 0 || modeValue > 7) {
                    throw new SemanticException("mode argument's range must be [0-7]", modeArg.getPos());
                }
            } else {
                throw new SemanticException("mode argument must be numerical type", modeArg.getPos());
            }

            Expr windowArg = functionCallExpr.getChild(0);
            if (windowArg instanceof IntLiteral) {
                IntLiteral windowIntLiteral = (IntLiteral) windowArg;
                long windowValue = windowIntLiteral.getValue();
                if (windowValue < 0) {
                    throw new SemanticException("window argument must >= 0", windowArg.getPos());
                }
            } else {
                throw new SemanticException("window argument must be numerical type", windowArg.getPos());
            }

            Expr timeExpr = functionCallExpr.getChild(1);
            if (timeExpr.isConstant()) {
                throw new SemanticException("time arg must be column", timeExpr.getPos());
            }
        }

        if (fnName.getFunction().equals(FunctionSet.MAX_BY) || fnName.getFunction().equals(FunctionSet.MIN_BY)) {
            if (functionCallExpr.getChildren().size() != 2 || functionCallExpr.getChildren().isEmpty()) {
                throw new SemanticException(
                        fnName.getFunction() + " requires two parameters: " + functionCallExpr.toSql(),
                        functionCallExpr.getPos());
            }

            if (functionCallExpr.getChild(0).isConstant() || functionCallExpr.getChild(1).isConstant()) {
                throw new SemanticException(
                        fnName.getFunction() + " function args must be column", functionCallExpr.getPos());
            }

            fnParams.setIsDistinct(false);  // DISTINCT is meaningless here

            Type sortKeyType = functionCallExpr.getChild(1).getType();
            if (!sortKeyType.canApplyToNumeric()) {
                throw new SemanticException(Type.NOT_SUPPORT_ORDER_ERROR_MSG);
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
                    fnName.getFunction() + " requires a numeric parameter: " + functionCallExpr.toSql(),
                    functionCallExpr.getPos());
        }

        if ((fnName.getFunction().equals(FunctionSet.MIN)
                || fnName.getFunction().equals(FunctionSet.MAX)
                || fnName.getFunction().equals(FunctionSet.NDV)
                || fnName.getFunction().equals(FunctionSet.APPROX_COUNT_DISTINCT))
                && !arg.getType().canApplyToNumeric()) {
            throw new SemanticException(Type.NOT_SUPPORT_AGG_ERROR_MSG);
        }

        if ((fnName.getFunction().equals(FunctionSet.BITMAP_UNION_INT) && !arg.getType().isIntegerType())) {
            throw new SemanticException("BITMAP_UNION_INT params only support Integer getType()",
                    functionCallExpr.getPos());
        }

        if (fnName.getFunction().equals(FunctionSet.INTERSECT_COUNT)) {
            if (functionCallExpr.getChildren().size() <= 2) {
                throw new SemanticException("intersect_count(bitmap_column, column_to_filter, filter_values) " +
                        "function requires at least three parameters", functionCallExpr.getPos());
            }

            Type inputType = functionCallExpr.getChild(0).getType();
            if (!inputType.isBitmapType()) {
                throw new SemanticException(
                        "intersect_count function first argument should be of BITMAP getType(), but was " + inputType,
                        functionCallExpr.getChild(0).getPos());
            }

            if (functionCallExpr.getChild(1).isConstant()) {
                throw new SemanticException("intersect_count function filter_values arg must be column",
                        functionCallExpr.getChild(1).getPos());
            }

            for (int i = 2; i < functionCallExpr.getChildren().size(); i++) {
                if (!functionCallExpr.getChild(i).isConstant()) {
                    throw new SemanticException("intersect_count function filter_values arg must be constant",
                            functionCallExpr.getChild(i).getPos());
                }
            }
            return;
        }

        if (fnName.getFunction().equals(FunctionSet.BITMAP_AGG)) {
            if (functionCallExpr.getChildren().size() != 1) {
                throw new SemanticException(fnName + " function could only have one child", functionCallExpr.getPos());
            }
            Type inputType = functionCallExpr.getChild(0).getType();
            if (!inputType.isIntegerType() && !inputType.isBoolean() && !inputType.isLargeIntType()
                    && !inputType.isStringType()) {
                throw new SemanticException(
                        fnName + " function's argument should be of int type or bool type or string type, but was "
                                + inputType, functionCallExpr.getChild(0).getPos());
            }
            return;
        }

        if (fnName.getFunction().equals(FunctionSet.BITMAP_COUNT)
                || fnName.getFunction().equals(FunctionSet.BITMAP_UNION)
                || fnName.getFunction().equals(FunctionSet.BITMAP_UNION_COUNT)
                || fnName.getFunction().equals(FunctionSet.BITMAP_INTERSECT)) {
            if (functionCallExpr.getChildren().size() != 1) {
                throw new SemanticException(fnName + " function could only have one child", functionCallExpr.getPos());
            }
            Type inputType = functionCallExpr.getChild(0).getType();
            if (!inputType.isBitmapType()) {
                throw new SemanticException(
                        fnName + " function's argument should be of BITMAP getType(), but was " + inputType,
                        functionCallExpr.getChild(0).getPos());
            }
            return;
        }

        if ((fnName.getFunction().equals(FunctionSet.HLL_UNION_AGG)
                || fnName.getFunction().equals(FunctionSet.HLL_UNION)
                || fnName.getFunction().equals(FunctionSet.HLL_CARDINALITY)
                || fnName.getFunction().equals(FunctionSet.HLL_RAW_AGG))
                && !arg.getType().isHllType()) {
            throw new SemanticException("HLL_UNION_AGG, HLL_RAW_AGG and HLL_CARDINALITY's params must be hll column",
                    functionCallExpr.getPos());
        }

        if (fnName.getFunction().equals(FunctionSet.MIN)
                || fnName.getFunction().equals(FunctionSet.MAX)
                || fnName.getFunction().equals(FunctionSet.NDV)
                || fnName.getFunction().equals(FunctionSet.HLL_UNION_AGG)) {
            fnParams.setIsDistinct(false);  // DISTINCT is meaningless here
        }

        if (fnName.getFunction().equals(FunctionSet.PERCENTILE_APPROX)) {
            if (functionCallExpr.getChildren().size() != 2 && functionCallExpr.getChildren().size() != 3) {
                throw new SemanticException("percentile_approx(expr, DOUBLE [, B]) requires two or three parameters",
                        functionCallExpr.getPos());
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

        if (fnName.getFunction().equals(FunctionSet.APPROX_TOP_K)) {
            Long k = null;
            Long counterNum = null;
            Expr kExpr = null;
            Expr counterNumExpr = null;
            if (functionCallExpr.hasChild(1)) {
                kExpr = functionCallExpr.getChild(1);
                if (!ExprUtil.isPositiveConstantInteger(kExpr)) {
                    throw new SemanticException(
                            "The second parameter of APPROX_TOP_K must be a constant positive integer: " +
                                    functionCallExpr.toSql(), kExpr.getPos());
                }
                k = ExprUtil.getIntegerConstant(kExpr);
            }
            if (functionCallExpr.hasChild(2)) {
                counterNumExpr = functionCallExpr.getChild(2);
                if (!ExprUtil.isPositiveConstantInteger(counterNumExpr)) {
                    throw new SemanticException(
                            "The third parameter of APPROX_TOP_K must be a constant positive integer: " +
                                    functionCallExpr.toSql(), counterNumExpr.getPos());
                }
                counterNum = ExprUtil.getIntegerConstant(counterNumExpr);
            }
            if (k != null && k > FeConstants.MAX_COUNTER_NUM_OF_TOP_K) {
                throw new SemanticException("The maximum number of the second parameter is "
                        + FeConstants.MAX_COUNTER_NUM_OF_TOP_K + ", " + functionCallExpr.toSql(), kExpr.getPos());
            }
            if (counterNum != null) {
                Preconditions.checkNotNull(k);
                if (counterNum > FeConstants.MAX_COUNTER_NUM_OF_TOP_K) {
                    throw new SemanticException("The maximum number of the third parameter is "
                            + FeConstants.MAX_COUNTER_NUM_OF_TOP_K + ", " + functionCallExpr.toSql(), counterNumExpr.getPos());
                }
                if (k > counterNum) {
                    throw new SemanticException(
                            "The second parameter must be smaller than or equal to the third parameter" +
                                    functionCallExpr.toSql(), kExpr.getPos());
                }
            }
        }

        if (fnName.getFunction().equals(FunctionSet.EXCHANGE_BYTES) ||
                fnName.getFunction().equals(FunctionSet.EXCHANGE_SPEED)) {
            if (ConnectContext.get().getSessionVariable().getNewPlannerAggStage() != 1) {
                throw new SemanticException(fnName.getFunction() + " should run in new_planner_agg_stage = 1",
                        functionCallExpr.getPos());
            }
        }

        if (fnName.getFunction().equals(FunctionSet.PERCENTILE_DISC) ||
                fnName.getFunction().equals(FunctionSet.PERCENTILE_CONT)) {
            if (functionCallExpr.getChildren().size() != 2) {
                throw new SemanticException(fnName + " requires two parameters");
            }
            if (!(functionCallExpr.getChild(1) instanceof DecimalLiteral) &&
                    !(functionCallExpr.getChild(1) instanceof IntLiteral)) {
                throw new SemanticException(fnName + " 's second parameter's data type is wrong ");
            }
            double rate = ((LiteralExpr) functionCallExpr.getChild(1)).getDoubleValue();
            if (rate < 0 || rate > 1) {
                throw new SemanticException(
                        fnName + " second parameter'value should be between 0 and 1");
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
