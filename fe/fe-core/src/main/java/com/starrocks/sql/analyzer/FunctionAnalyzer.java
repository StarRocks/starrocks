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
import com.google.common.collect.Sets;
import com.google.re2j.Pattern;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.LiteralExpr;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.UserVariableExpr;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.TableFunction;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.combinator.AggStateCombinator;
import com.starrocks.catalog.combinator.AggStateMergeCombinator;
import com.starrocks.catalog.combinator.AggStateUnionCombinator;
import com.starrocks.catalog.combinator.AggStateUtils;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariableConstants;
import com.starrocks.sql.ast.ArrayExpr;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorEvaluator;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.parser.NodePosition;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class FunctionAnalyzer {
    public static final Pattern HAS_TIME_PART = Pattern.compile("^.*[HhIiklrSsT]+.*$");
    private static final Set<String> SUPPORTED_TGT_TYPES = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);

    static {
        SUPPORTED_TGT_TYPES.addAll(Lists.newArrayList("HLL_8", "HLL_6", "HLL_4"));
    }

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

        if (FunctionSet.INDEX_ONLY_FUNCTIONS.contains(fnName.getFunction())) {
            if (!functionCallExpr.getChild(0).getType().isStringType() ||
                    !functionCallExpr.getChild(0).getType().isStringType()) {
                throw new SemanticException(
                        fnName + " function 's first parameter and second parameter must be string type",
                        functionCallExpr.getPos());
            }

            if (!functionCallExpr.getChild(1).isConstant() || !functionCallExpr.getChild(2).isConstant()) {
                throw new SemanticException(
                        fnName + " function 's second parameter and third parameter must be constant",
                        functionCallExpr.getPos());
            }
        }
        Function fn = functionCallExpr.getFn();
        if (fn instanceof AggStateCombinator) {
            // analyze `_state` combinator function by using its arg function
            FunctionName argFuncName = new FunctionName(AggStateUtils.getAggFuncNameOfCombinator(fnName.getFunction()));
            analyzeBuiltinAggFunction(argFuncName, functionCallExpr.getParams(), functionCallExpr);
        } else if (fn instanceof AggStateUnionCombinator) {
            AggStateUnionCombinator unionCombinator = (AggStateUnionCombinator) fn;
            if (Arrays.stream(fn.getArgs()).anyMatch(Type::isWildcardDecimal)) {
                throw new SemanticException(String.format("Resolved function %s has no wildcard decimal as argument type",
                        fn.functionName()), functionCallExpr.getPos());
            }
            if (unionCombinator.getReturnType().isWildcardDecimal()) {
                throw new SemanticException(String.format("Resolved function %s has no wildcard decimal as return type",
                        fn.functionName()), functionCallExpr.getPos());
            }
        } else if (fn instanceof AggStateMergeCombinator) {
            AggStateMergeCombinator mergeCombinator = (AggStateMergeCombinator) fn;
            if (Arrays.stream(fn.getArgs()).anyMatch(Type::isWildcardDecimal)) {
                throw new SemanticException(String.format("Resolved function %s has no wildcard decimal as argument type",
                        fn.functionName()), functionCallExpr.getPos());
            }
            if (mergeCombinator.getReturnType().isWildcardDecimal()) {
                throw new SemanticException(String.format("Resolved function %s has no wildcard decimal as return type",
                        fn.functionName()), functionCallExpr.getPos());
            }
        }
    }

    private static void analyzeBuiltinAggFunction(FunctionCallExpr functionCallExpr) {
        FunctionName fnName = functionCallExpr.getFnName();
        FunctionParams fnParams = functionCallExpr.getParams();
        analyzeBuiltinAggFunction(fnName, fnParams, functionCallExpr);
    }

    private static void analyzeBuiltinAggFunction(FunctionName fnName,
                                                  FunctionParams fnParams,
                                                  FunctionCallExpr functionCallExpr) {
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

            int sepPos =
                    functionCallExpr.getParams().exprs().size() - functionCallExpr.getParams().getOrderByElemNum() - 1;
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
                throw new SemanticException(fnName.getFunction() + " only used in analytic function",
                        functionCallExpr.getPos());
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
                throw new SemanticException(fnName.getFunction() + " only used in analytic function",
                        functionCallExpr.getPos());
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
                || fnName.getFunction().equals(FunctionSet.APPROX_COUNT_DISTINCT)
                || fnName.getFunction().equals(FunctionSet.DS_HLL_COUNT_DISTINCT))
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
            if (!functionCallExpr.getChild(1).getType().isNumericType()) {
                throw new SemanticException("percentile_approx requires the second parameter's type is numeric type");
            }

            if (functionCallExpr.getChildren().size() == 3) {
                if (!functionCallExpr.getChild(2).getType().isNumericType()) {
                    throw new SemanticException(
                            "percentile_approx requires the third parameter's type is numeric type");
                }
            }
        }

        if (fnName.getFunction().equals(FunctionSet.APPROX_TOP_K)) {
            Optional<Long> k = Optional.empty();
            Optional<Long> counterNum = Optional.empty();
            Expr kExpr = null;
            Expr counterNumExpr = null;
            if (functionCallExpr.hasChild(1)) {
                kExpr = functionCallExpr.getChild(1);
                k = extractIntegerValue(kExpr);
                if (!k.isPresent() || k.get() <= 0) {
                    throw new SemanticException(
                            "The second parameter of APPROX_TOP_K must be a constant positive integer: " +
                                    functionCallExpr.toSql(), kExpr.getPos());
                }
            }
            if (functionCallExpr.hasChild(2)) {
                counterNumExpr = functionCallExpr.getChild(2);
                counterNum = extractIntegerValue(counterNumExpr);
                if (!counterNum.isPresent() || counterNum.get() <= 0) {
                    throw new SemanticException(
                            "The third parameter of APPROX_TOP_K must be a constant positive integer: " +
                                    functionCallExpr.toSql(), counterNumExpr.getPos());
                }
            }
            if (k.isPresent() && k.get() > FeConstants.MAX_COUNTER_NUM_OF_TOP_K) {
                throw new SemanticException("The maximum number of the second parameter is "
                        + FeConstants.MAX_COUNTER_NUM_OF_TOP_K + ", " + functionCallExpr.toSql(), kExpr.getPos());
            }
            if (counterNum.isPresent()) {
                if (counterNum.get() > FeConstants.MAX_COUNTER_NUM_OF_TOP_K) {
                    throw new SemanticException("The maximum number of the third parameter is "
                            + FeConstants.MAX_COUNTER_NUM_OF_TOP_K + ", " + functionCallExpr.toSql(),
                            counterNumExpr.getPos());
                }
                if (k.get() > counterNum.get()) {
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
                fnName.getFunction().equals(FunctionSet.PERCENTILE_CONT) ||
                fnName.getFunction().equals(FunctionSet.LC_PERCENTILE_DISC)) {
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

        // ds_hll_count_distinct
        if (fnName.getFunction().equals(FunctionSet.DS_HLL_COUNT_DISTINCT)) {
            int argSize = functionCallExpr.getChildren().size();
            if (argSize > 3) {
                throw new SemanticException(fnName + " requires one/two/three parameters: ds_hll_count_distinct(col, <log_k>, " +
                        "<tgt_type>)");
            }

            // check the first parameter: log_k
            if (argSize >= 2) {
                if (!(functionCallExpr.getChild(1) instanceof IntLiteral)) {
                    throw new SemanticException(fnName + " 's second parameter's data type is wrong ");
                }
                long precision = ((LiteralExpr) functionCallExpr.getChild(1)).getLongValue();
                if (precision < 4 || precision > 21) {
                    throw new SemanticException(
                            fnName + " second parameter'value should be between 4 and 21");
                }
            }

            // check the second parameter: tgt_type
            if (argSize == 3) {
                if (!(functionCallExpr.getChild(2) instanceof StringLiteral)) {
                    throw new SemanticException(fnName + " 's second parameter's data type is wrong ");
                }
                String tgtType = ((LiteralExpr) functionCallExpr.getChild(2)).getStringValue();
                if (!SUPPORTED_TGT_TYPES.contains(tgtType)) {
                    throw new SemanticException(
                            fnName + " third  parameter'value should be in HLL_4/HLL_6/HLL_8");
                }
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

        if (fnName.getFunction().equals(FunctionSet.MANN_WHITNEY_U_TEST)) {
            if (functionCallExpr.getChildren().size() >= 3) {
                if (!(functionCallExpr.getChild(2) instanceof StringLiteral)) {
                    throw new SemanticException(fnName + "'s third parameter should be a string literal.");
                }
                String alternative = ((StringLiteral) functionCallExpr.getChild(2)).getStringValue();
                if (!(alternative.equals("two-sided") || alternative.equals("greater") || alternative.equals("less"))) {
                    throw new SemanticException(
                            fnName + "'s third parameter should be one of ['two-sided', 'greater', 'less'], but get '" +
                                    alternative + "'.");
                }
            }
            if (functionCallExpr.getChildren().size() >= 4) {
                long continuityCorrection;
                if (functionCallExpr.getChild(3) instanceof IntLiteral) {
                    continuityCorrection = ((IntLiteral) functionCallExpr.getChild(3)).getLongValue();
                } else if (functionCallExpr.getChild(3) instanceof LargeIntLiteral) {
                    continuityCorrection = ((LargeIntLiteral) functionCallExpr.getChild(3)).getLongValue();
                } else {
                    throw new SemanticException(fnName + "'s fourth parameter should be a non-negative int literal.");
                }
                if (continuityCorrection < 0) {
                    throw new SemanticException(fnName + "'s fourth parameter should be a non-negative int literal.");
                }
            }
        }
    }

    private static Optional<Long> extractIntegerValue(Expr expr) {
        if (expr instanceof UserVariableExpr) {
            expr = ((UserVariableExpr) expr).getValue();
        }

        if (expr instanceof LiteralExpr && expr.getType().isFixedPointType()) {
            return Optional.of(((LiteralExpr) expr).getLongValue());
        }

        return Optional.empty();
    }

    /**
     * Get function by function call expression and argument types.
     *
     * @param session       current connect context
     * @param node          function call expression
     * @param argumentTypes argument types
     * @return function if it's found, otherwise return null
     */
    public static Function getAnalyzedFunction(ConnectContext session,
                                               FunctionCallExpr node,
                                               Type[] argumentTypes) {
        // get function from function factory
        List<Type> newArgumentTypes = new ArrayList<>();
        Function fn = getAnalyzedFunctionImpl(session, node, argumentTypes, newArgumentTypes);
        if (fn == null) {
            return null;
        }
        if (fn instanceof TableFunction) {
            throw new SemanticException("Table function cannot be used in expression", node.getPos());
        }

        // argument type may be changed in getFunctionImpl, so we need to update it
        String fnName = node.getFnName().getFunction();
        if (!newArgumentTypes.isEmpty()) {
            argumentTypes = newArgumentTypes.toArray(new Type[0]);
        }

        // validate argument types
        for (int i = 0; i < fn.getNumArgs(); i++) {
            if (!argumentTypes[i].matchesType(fn.getArgs()[i]) &&
                    !Type.canCastTo(argumentTypes[i], fn.getArgs()[i])) {
                String msg = String.format("No matching function with signature: %s(%s)", fnName,
                        node.getParams().isStar() ? "*" :
                                Arrays.stream(argumentTypes).map(Type::toSql).collect(Collectors.joining(", ")));
                throw new SemanticException(msg, node.getPos());
            }
        }
        if (fn.hasVarArgs()) {
            Type varType = fn.getArgs()[fn.getNumArgs() - 1];
            for (int i = fn.getNumArgs(); i < argumentTypes.length; i++) {
                if (!argumentTypes[i].matchesType(varType) &&
                        !Type.canCastTo(argumentTypes[i], varType)) {
                    String msg = String.format("Variadic function %s(%s) can't support type: %s", fnName,
                            Arrays.stream(fn.getArgs()).map(Type::toSql).collect(Collectors.joining(", ")),
                            argumentTypes[i]);
                    throw new SemanticException(msg, node.getPos());
                }
            }
        }
        return fn;
    }

    /**
     * Get function by function call expression and argument types.
     *
     * @param session          current connect context
     * @param node             function call expression
     * @param argumentTypes    argument types
     * @param newArgumentTypes new argument types
     * @return function if it's found, otherwise return null
     */
    private static Function getAnalyzedFunctionImpl(ConnectContext session,
                                                    FunctionCallExpr node,
                                                    Type[] argumentTypes,
                                                    List<Type> newArgumentTypes) {
        // get fn from known function variants
        Function fn = getAdjustedAnalyzedFunction(session, node, argumentTypes, newArgumentTypes);
        if (fn != null) {
            return fn;
        }
        newArgumentTypes.clear();

        // get fn from normalized function
        String fnName = node.getFnName().getFunction();
        FunctionParams params = node.getParams();
        Boolean[] isArgumentConstants = node.getChildren().stream().map(Expr::isConstant).toArray(Boolean[]::new);
        fn = getAdjustedAnalyzedAggregateFunction(session, fnName, params, argumentTypes, isArgumentConstants, node.getPos());
        if (fn != null) {
            return fn;
        }

        // get fn from builtin functions
        fn = getAnalyzedBuiltInFunction(session, fnName, params, argumentTypes, node.getPos());
        if (fn != null) {
            return fn;
        }

        // get fn from udf
        fn = AnalyzerUtils.getUdfFunction(session, node.getFnName(), argumentTypes);
        if (fn != null) {
            return fn;
        }

        // get fn from meta functions
        return ScalarOperatorEvaluator.INSTANCE.getMetaFunction(node.getFnName(), argumentTypes);
    }

    /**
     * Get function's variant from known scalar functions by function name and argument types.
     * NOTE: Function's argument types may be changed in this method.
     *
     * @param session          connect context
     * @param node             function call expr
     * @param argumentTypes    original argument types
     * @param newArgumentTypes new argument types
     * @return function's variant
     */
    private static Function getAdjustedAnalyzedFunction(ConnectContext session,
                                                        FunctionCallExpr node,
                                                        Type[] argumentTypes,
                                                        List<Type> newArgumentTypes) {
        Function fn = null;
        String fnName = node.getFnName().getFunction();
        // throw exception direct
        if (fnName.equalsIgnoreCase("typeof") && argumentTypes.length == 1) {
            // For the typeof function, the parameter type of the function is the result of this function.
            // At this time, the parameter type has been obtained. You can directly replace the current
            // function with StringLiteral. However, since the parent node of the current node in ast
            // cannot be obtained, this cannot be done directly. Replacement, here the StringLiteral is
            // stored in the parameter of the function, so that the StringLiteral can be obtained in the
            // subsequent rule rewriting, and then the typeof can be replaced.
            Type originType = argumentTypes[0];
            argumentTypes[0] = Type.STRING;
            fn = new Function(new FunctionName("typeof_internal"), argumentTypes, Type.STRING, false);
            Expr newChildExpr = new StringLiteral(originType.toTypeString());
            node.getParams().exprs().set(0, newChildExpr);
            node.setChild(0, newChildExpr);
        } else if (FunctionSet.CONCAT.equals(fnName) && node.getChildren().stream().anyMatch(child ->
                child.getType().isArrayType())) {
            List<Type> arrayTypes = Arrays.stream(argumentTypes).map(argumentType -> {
                if (argumentType.isArrayType()) {
                    return argumentType;
                } else {
                    return new ArrayType(argumentType);
                }
            }).collect(Collectors.toList());
            // check if all array types are compatible
            TypeManager.getCommonSuperType(arrayTypes);
            for (int i = 0; i < argumentTypes.length; ++i) {
                if (!argumentTypes[i].isArrayType()) {
                    node.setChild(i, new ArrayExpr(new ArrayType(argumentTypes[i]),
                            Lists.newArrayList(node.getChild(i))));
                }
            }

            argumentTypes = node.getChildren().stream().map(Expr::getType).toArray(Type[]::new);
            node.resetFnName(null, FunctionSet.ARRAY_CONCAT);
            if (DecimalV3FunctionAnalyzer.argumentTypeContainDecimalV3(FunctionSet.ARRAY_CONCAT, argumentTypes)) {
                fn = DecimalV3FunctionAnalyzer.getDecimalV3Function(session, node, argumentTypes);
            } else {
                fn = Expr.getBuiltinFunction(FunctionSet.ARRAY_CONCAT, argumentTypes,
                        Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            }
        } else if (FunctionSet.NAMED_STRUCT.equals(fnName)) {
            // deriver struct type
            fn = Expr.getBuiltinFunction(FunctionSet.NAMED_STRUCT, argumentTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            fn = fn.copy();
            ArrayList<StructField> sf = Lists.newArrayList();
            for (int i = 0; i < node.getChildren().size(); i = i + 2) {
                StringLiteral literal = (StringLiteral) node.getChild(i);
                sf.add(new StructField(literal.getStringValue(), node.getChild(i + 1).getType()));
            }
            fn.setRetType(new StructType(sf));
        } else if (FunctionSet.STR_TO_DATE.equals(fnName)) {
            fn = getStrToDateFunction(node, argumentTypes);
        } else if (FunctionSet.ARRAY_GENERATE.equals(fnName)) {
            fn = getArrayGenerateFunction(node);
            argumentTypes = node.getChildren().stream().map(Expr::getType).toArray(Type[]::new);
        } else if (FunctionSet.BITMAP_UNION.equals(fnName)) {
            // bitmap_union is analyzed here rather than `getAnalyzedAggregateFunction` because
            // it's just a syntax sugar for bitmap_agg transformed from bitmap_union(to_bitmap())
            fn = Expr.getBuiltinFunction(fnName, argumentTypes, Function.CompareMode.IS_IDENTICAL);
            if (session.getSessionVariable().isEnableRewriteBitmapUnionToBitmapAgg() &&
                    node.getChild(0) instanceof FunctionCallExpr) {
                FunctionCallExpr arg0Func = (FunctionCallExpr) node.getChild(0);
                // Convert bitmap_union(to_bitmap(v1)) to bitmap_agg(v1) when v1's type
                // is a numeric type.
                if (FunctionSet.TO_BITMAP.equals(arg0Func.getFnName().getFunction())) {
                    Expr toBitmapArg0 = arg0Func.getChild(0);
                    Type toBitmapArg0Type = toBitmapArg0.getType();
                    if (toBitmapArg0Type.isIntegerType() || toBitmapArg0Type.isBoolean()
                            || toBitmapArg0Type.isLargeIntType()) {
                        argumentTypes = new Type[] { toBitmapArg0Type };
                        node.setChild(0, toBitmapArg0);
                        node.resetFnName("", FunctionSet.BITMAP_AGG);
                        node.getParams().setExprs(Lists.newArrayList(toBitmapArg0));
                        fn = Expr.getBuiltinFunction(FunctionSet.BITMAP_AGG, argumentTypes,
                                Function.CompareMode.IS_IDENTICAL);
                    }
                }
            }
        } else if (FunctionSet.COUNT.equalsIgnoreCase(fnName) && node.isDistinct() && node.getChildren().size() == 1) {
            SessionVariableConstants.CountDistinctImplMode countDistinctImplementation =
                    session.getSessionVariable().getCountDistinctImplementation();
            if (countDistinctImplementation != null) {
                switch (countDistinctImplementation) {
                    case NDV:
                        node.resetFnName("", FunctionSet.NDV);
                        node.getParams().setIsDistinct(false);
                        fn = Expr.getBuiltinFunction(FunctionSet.NDV, argumentTypes,
                                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                        break;
                    case MULTI_COUNT_DISTINCT:
                        node.resetFnName("", FunctionSet.MULTI_DISTINCT_COUNT);
                        node.getParams().setIsDistinct(false);
                        fn = Expr.getBuiltinFunction(FunctionSet.MULTI_DISTINCT_COUNT, argumentTypes,
                                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
                        break;
                }
            }
        } else if (FunctionSet.FIELD.equalsIgnoreCase(fnName)) {
            Type targetType = argumentTypes[0];
            Type returnType = Type.INT;
            if (targetType.isNull()) {
                targetType = Type.INT;
            } else {      
                for (int i = 1; i < argumentTypes.length; i++) {
                    if (argumentTypes[i].isNull()) {
                        //do nothing
                    } else if ((targetType.isNumericType() && argumentTypes[i].isNumericType()) ||
                                (targetType.isStringType() && argumentTypes[i].isStringType())) {
                        targetType = Type.getAssignmentCompatibleType(targetType, argumentTypes[i], false);
                        if (targetType.isInvalid()) {
                            throw new SemanticException("Parameter's type is invalid");
                        }
                    } else {
                        targetType = Type.DOUBLE;
                    }
                }
            }
            Type[] argsTypes = new Type[1];
            argsTypes[0] = targetType;
            fn = Expr.getBuiltinFunction(fnName, argsTypes, true, returnType, Function.CompareMode.IS_IDENTICAL);
            // correct decimal's precision and scale
            if (targetType.isDecimalV3()) {
                List<Type> argTypes = Arrays.asList(targetType);
                ScalarFunction newFn = new ScalarFunction(fn.getFunctionName(), argTypes, returnType,
                        fn.getLocation(), ((ScalarFunction) fn).getSymbolName(),
                        ((ScalarFunction) fn).getPrepareFnSymbol(),
                        ((ScalarFunction) fn).getCloseFnSymbol());
                newFn.setFunctionId(fn.getFunctionId());
                newFn.setChecksum(fn.getChecksum());
                newFn.setBinaryType(fn.getBinaryType());
                newFn.setHasVarArgs(fn.hasVarArgs());
                newFn.setId(fn.getId());
                newFn.setUserVisible(fn.isUserVisible());
                fn = newFn;
            }
        } else if (FunctionSet.ARRAY_CONTAINS.equalsIgnoreCase(fnName) || FunctionSet.ARRAY_POSITION.equalsIgnoreCase(fnName)) {
            Preconditions.checkState(argumentTypes.length == 2);
            if (argumentTypes[1].isNull() &&
                    argumentTypes[0].isArrayType() && ((ArrayType) argumentTypes[0]).getItemType().isNull()) {
                argumentTypes[0] = Type.ARRAY_BOOLEAN;
                argumentTypes[1] = Type.BOOLEAN;
                fn = Expr.getBuiltinFunction(fnName, argumentTypes, Function.CompareMode.IS_IDENTICAL);
            }
        }
        // add new argument types
        Arrays.stream(argumentTypes).forEach(newArgumentTypes::add);
        return fn;
    }

    /**
     * @TODO: Determine the return type of this function
     * If is format is constant and don't contains time part, return date type, to compatible with mysql.
     * In fact we don't want to support str_to_date return date like mysql, reason:
     * 1. The return type of FE/BE str_to_date function signature is datetime, return date
     * let type different, it's will throw unpredictable error
     * 2. Support return date and datetime at same time in one function is complicated.
     * 3. The meaning of the function is confusing. In mysql, will return date if format is a constant
     * string and it's not contains "%H/%M/%S" pattern, but it's a trick logic, if format is a variable
     * expression, like: str_to_date(col1, col2), and the col2 is '%Y%m%d', the result always be
     * datetime.
     */
    private static Function getStrToDateFunction(FunctionCallExpr node, Type[] argumentTypes) {
        Function fn = Expr.getBuiltinFunction(node.getFnName().getFunction(),
                argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        if (fn == null) {
            return null;
        }
        if (!node.getChild(1).isConstant()) {
            return fn;
        }

        ExpressionMapping expressionMapping =
                new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()),
                        Lists.newArrayList());
        ScalarOperator format = SqlToScalarOperatorTranslator.translate(node.getChild(1), expressionMapping,
                new ColumnRefFactory());
        if (format.isConstantRef() && !HAS_TIME_PART.matcher(format.toString()).matches()) {
            return Expr.getBuiltinFunction("str2date", argumentTypes,
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        }

        return fn;
    }

    private static Function getArrayGenerateFunction(FunctionCallExpr node) {
        // add the default parameters for array_generate
        if (node.getChildren().size() == 1) {
            LiteralExpr secondParam = (LiteralExpr) node.getChild(0);
            node.clearChildren();
            node.addChild(new IntLiteral(1));
            node.addChild(secondParam);
        }
        if (node.getChildren().size() == 2) {
            int idx = 0;
            BigInteger[] childValues = new BigInteger[2];
            Boolean hasNUll = false;
            for (Expr expr : node.getChildren()) {
                if (expr instanceof NullLiteral) {
                    hasNUll = true;
                } else if (expr instanceof IntLiteral) {
                    childValues[idx++] = BigInteger.valueOf(((IntLiteral) expr).getValue());
                } else {
                    childValues[idx++] = ((LargeIntLiteral) expr).getValue();
                }
            }

            if (hasNUll || childValues[0].compareTo(childValues[1]) < 0) {
                node.addChild(new IntLiteral(1));
            } else {
                node.addChild(new IntLiteral(-1));
            }
        }
        Type[] argumentTypes = node.getChildren().stream().map(Expr::getType).toArray(Type[]::new);
        return Expr.getBuiltinFunction(FunctionSet.ARRAY_GENERATE, argumentTypes,
                Function.CompareMode.IS_SUPERTYPE_OF);
    }

    public static Pair<Type[], Type> getArrayAggGroupConcatIntermediateType(String fnName,
                                                                            Type[] argumentTypes,
                                                                            List<Boolean> isAscOrder) {
        Type[] argsTypes = new Type[argumentTypes.length];
        for (int i = 0; i < argumentTypes.length; ++i) {
            argsTypes[i] = argumentTypes[i] == Type.NULL ? Type.BOOLEAN : argumentTypes[i];
            if (fnName.equals(FunctionSet.GROUP_CONCAT) && i < argumentTypes.length - isAscOrder.size()) {
                argsTypes[i] = Type.VARCHAR;
            }
        }
        ArrayList<Type> structTypes = new ArrayList<>(argsTypes.length);
        for (Type t : argsTypes) {
            structTypes.add(new ArrayType(t));
        }
        return Pair.create(argsTypes, new StructType(structTypes));
    }
    /**
     * Get and normalize function to make its argument/result type correct.
     *
     * @param fnName              function name
     * @param params              function's params(eg: is distinct or not, order by elements)
     * @param argumentTypes       function's argument types
     * @param argumentIsConstants function's argument is constant or not
     * @return normalized function
     */
    private static Function getAdjustedAnalyzedAggregateFunction(ConnectContext session,
                                                                 String fnName,
                                                                 FunctionParams params,
                                                                 Type[] argumentTypes,
                                                                 Boolean[] argumentIsConstants,
                                                                 NodePosition pos) {
        Preconditions.checkArgument(fnName != null);
        Preconditions.checkArgument(argumentTypes != null);
        Preconditions.checkArgument(argumentIsConstants != null);
        Preconditions.checkArgument(argumentIsConstants.length == argumentTypes.length);
        Function fn = null;
        int argSize = argumentTypes.length;
        boolean isDistinct = params.isDistinct();
        if (fnName.equals(FunctionSet.COUNT) && isDistinct) {
            // Compatible with the logic of the original search function "count distinct"
            // TODO: fix how we equal count distinct.
            fn = Expr.getBuiltinFunction(FunctionSet.COUNT, new Type[] { argumentTypes[0] },
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        } else if (fnName.equals(FunctionSet.EXCHANGE_BYTES) || fnName.equals(FunctionSet.EXCHANGE_SPEED)) {
            fn = Expr.getBuiltinFunction(fnName, argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            fn = fn.copy();
            fn.setArgsType(argumentTypes); // as accepting various types
            fn.setIsNullable(false);
        } else if (fnName.equals(FunctionSet.ARRAY_AGG) || fnName.equals(FunctionSet.GROUP_CONCAT)) {
            // move order by expr to node child, and extract is_asc and null_first information.
            fn = Expr.getBuiltinFunction(fnName, new Type[] {argumentTypes[0]},
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            fn = fn.copy();
            List<Boolean> isAscOrder = new ArrayList<>();
            List<Boolean> nullsFirst = new ArrayList<>();
            List<OrderByElement> orderByElements = params.getOrderByElements();
            if (orderByElements != null) {
                for (OrderByElement elem : orderByElements) {
                    isAscOrder.add(elem.getIsAsc());
                    nullsFirst.add(elem.getNullsFirstParam());
                }
            }
            Pair<Type[], Type> argsAndIntermediateTypes =
                    getArrayAggGroupConcatIntermediateType(fnName, argumentTypes, isAscOrder);
            Type[] argsTypes = argsAndIntermediateTypes.first;
            fn.setArgsType(argsTypes); // as accepting various types
            ((AggregateFunction) fn).setIntermediateType(argsAndIntermediateTypes.second);
            ((AggregateFunction) fn).setIsAscOrder(isAscOrder);
            ((AggregateFunction) fn).setNullsFirst(nullsFirst);
            boolean outputConst = true;
            if (fnName.equals(FunctionSet.ARRAY_AGG)) {
                fn.setRetType(new ArrayType(argsTypes[0]));     // return null if scalar agg with empty input
                outputConst = argumentIsConstants[0];
            } else {
                fn.setRetType(Type.VARCHAR);
                for (int i = 0; i < argSize - isAscOrder.size() - 1; i++) {
                    if (!argumentIsConstants[i]) {
                        outputConst = false;
                        break;
                    }
                }
            }
            // need to distinct output columns in finalize phase
            ((AggregateFunction) fn).setIsDistinct(isDistinct && (!isAscOrder.isEmpty() || outputConst));
        } else if (FunctionSet.PERCENTILE_DISC.equals(fnName) || FunctionSet.LC_PERCENTILE_DISC.equals(fnName)) {
            argumentTypes[1] = Type.DOUBLE;
            fn = Expr.getBuiltinFunction(fnName, argumentTypes, Function.CompareMode.IS_IDENTICAL);
            // correct decimal's precision and scale
            if (fn.getArgs()[0].isDecimalV3()) {
                List<Type> argTypes = Arrays.asList(argumentTypes[0], fn.getArgs()[1]);

                AggregateFunction newFn = new AggregateFunction(fn.getFunctionName(), argTypes, argumentTypes[0],
                        ((AggregateFunction) fn).getIntermediateType(), fn.hasVarArgs());

                newFn.setFunctionId(fn.getFunctionId());
                newFn.setChecksum(fn.getChecksum());
                newFn.setBinaryType(fn.getBinaryType());
                newFn.setHasVarArgs(fn.hasVarArgs());
                newFn.setId(fn.getId());
                newFn.setUserVisible(fn.isUserVisible());
                newFn.setisAnalyticFn(((AggregateFunction) fn).isAnalyticFn());
                fn = newFn;
            }
        } else if (fnName.endsWith(FunctionSet.AGG_STATE_SUFFIX)
                || fnName.endsWith(FunctionSet.AGG_STATE_UNION_SUFFIX)
                || fnName.endsWith(FunctionSet.AGG_STATE_MERGE_SUFFIX)) {
            Function func = Expr.getBuiltinFunction(fnName, argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            if (func == null) {
                return null;
            }
            fn = AggStateUtils.getAnalyzedCombinatorFunction(session, func, params, argumentTypes, argumentIsConstants, pos);
        }
        return fn;
    }

    /**
     * Get aggregate functions by function name and argument types.
     * NOTE: it will first normalize function, then get builtin function.
     * @param session  connect context
     * @param fnName function name
     * @param params function's params(eg: is distinct or not, order by elements)
     * @param argumentTypes function's argument types
     * @param argumentIsConstants function's argument is constant or not
     * @param pos function's syntax position
     * @return aggregate function
     */
    public static Function getAnalyzedAggregateFunction(ConnectContext session,
                                                        String fnName,
                                                        FunctionParams params,
                                                        Type[] argumentTypes,
                                                        Boolean[] argumentIsConstants,
                                                        NodePosition pos) {
        Function fn = getAdjustedAnalyzedAggregateFunction(session, fnName, params, argumentTypes, argumentIsConstants, pos);
        if (fn != null) {
            return fn;
        }
        return getAnalyzedBuiltInFunction(session, fnName, params, argumentTypes, pos);
    }

    /**
     * Get builtin function by function name and argument types without normalization.
     * @param session connect context
     * @param fnName function name
     * @param params function's params(eg: is distinct or not, order by elements)
     * @param argumentTypes function's argument types
     * @param pos function's syntax position
     * @return builtin function
     */
    public static Function getAnalyzedBuiltInFunction(ConnectContext session,
                                                      String fnName,
                                                      FunctionParams params,
                                                      Type[] argumentTypes,
                                                      NodePosition pos) {
        Function fn = null;
        if (DecimalV3FunctionAnalyzer.argumentTypeContainDecimalV3(fnName, argumentTypes)) {
            // Since the priority of decimal version is higher than double version (according functionId),
            // and in `Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF` mode, `Expr.getBuiltinFunction` always
            // return decimal version even if the input parameters are not decimal, such as (INT, INT),
            // lacking of specific decimal type process defined in `getDecimalV3Function`. So we force round functions
            // to go through `getDecimalV3Function` here
            fn = DecimalV3FunctionAnalyzer.getDecimalV3Function(session, fnName, params, argumentTypes, pos);
        } else if (DecimalV3FunctionAnalyzer.argumentTypeContainDecimalV2(fnName, argumentTypes)) {
            fn = DecimalV3FunctionAnalyzer.getDecimalV2Function(fnName, argumentTypes);
        } else if (Arrays.stream(argumentTypes).anyMatch(arg -> arg.matchesType(Type.TIME))) {
            fn = Expr.getBuiltinFunction(fnName, argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            if (fn instanceof AggregateFunction) {
                throw new SemanticException("Time Type can not used in " + fnName + " function", pos);
            }
        } else {
            fn = Expr.getBuiltinFunction(fnName, argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        }
        return fn;
    }
}
