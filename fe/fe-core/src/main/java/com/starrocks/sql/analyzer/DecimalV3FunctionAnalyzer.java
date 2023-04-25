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
import com.google.common.collect.ImmutableSortedSet;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF;

public class DecimalV3FunctionAnalyzer {
    public static final Set<String> DECIMAL_UNARY_FUNCTION_SET =
            new ImmutableSortedSet.Builder<>(String::compareTo)
                    .add(FunctionSet.ABS).add(FunctionSet.POSITIVE).add(FunctionSet.NEGATIVE)
                    .add(FunctionSet.MONEY_FORMAT).build();

    public static final Set<String> DECIMAL_IDENTICAL_TYPE_FUNCTION_SET =
            new ImmutableSortedSet.Builder<>(String::compareTo)
                    .add(FunctionSet.LEAST).add(FunctionSet.GREATEST).add(FunctionSet.NULLIF)
                    .add(FunctionSet.IFNULL).add(FunctionSet.COALESCE).add(FunctionSet.MOD).build();

    public static final Set<String> DECIMAL_AGG_FUNCTION_SAME_TYPE =
            new ImmutableSortedSet.Builder<>(String::compareTo)
                    .add(FunctionSet.MAX).add(FunctionSet.MIN)
                    .add(FunctionSet.LEAD).add(FunctionSet.LAG)
                    .add(FunctionSet.FIRST_VALUE).add(FunctionSet.LAST_VALUE)
                    .add(FunctionSet.ANY_VALUE).add(FunctionSet.ARRAY_AGG)
                    .add(FunctionSet.HISTOGRAM).build();

    public static final Set<String> DECIMAL_AGG_FUNCTION_WIDER_TYPE =
            new ImmutableSortedSet.Builder<>(String::compareTo)
                    .add(FunctionSet.COUNT).add(FunctionSet.SUM)
                    .add(FunctionSet.MULTI_DISTINCT_SUM).add(FunctionSet.AVG).add(FunctionSet.VARIANCE)
                    .add(FunctionSet.VARIANCE_POP).add(FunctionSet.VAR_POP).add(FunctionSet.VARIANCE_SAMP)
                    .add(FunctionSet.VAR_SAMP).add(FunctionSet.STD).add(FunctionSet.STDDEV).add(FunctionSet.STDDEV_POP)
                    .add(FunctionSet.STDDEV_SAMP).build();

    public static final Set<String> DECIMAL_AGG_VARIANCE_STDDEV_TYPE =
            new ImmutableSortedSet.Builder<>(String::compareTo)
                    .add(FunctionSet.VARIANCE).add(FunctionSet.VARIANCE_POP).add(FunctionSet.VAR_POP)
                    .add(FunctionSet.VARIANCE_SAMP).add(FunctionSet.VAR_SAMP).add(FunctionSet.STD)
                    .add(FunctionSet.STDDEV).add(FunctionSet.STDDEV_POP).add(FunctionSet.STDDEV_SAMP).build();

    public static final Set<String> DECIMAL_SUM_FUNCTION_TYPE =
            new ImmutableSortedSet.Builder<>(String::compareTo).add(FunctionSet.SUM)
                    .add(FunctionSet.MULTI_DISTINCT_SUM).build();

    public static final Set<String> DECIMAL_AGG_FUNCTION =
            new ImmutableSortedSet.Builder<>(String::compareTo)
                    .addAll(DECIMAL_AGG_FUNCTION_SAME_TYPE)
                    .addAll(DECIMAL_AGG_FUNCTION_WIDER_TYPE).build();

    // For decimal32/64/128 types, scale and precision of returnType depends on argTypes'
    public static Type normalizeDecimalArgTypes(Type[] argTypes, String fnName) {
        if (argTypes == null || argTypes.length == 0) {
            return Type.INVALID;
        }

        if (FunctionSet.HISTOGRAM.equals(fnName)) {
            return Type.VARCHAR;
        }

        if (FunctionSet.MAX_BY.equals(fnName)) {
            if (argTypes[0].isDecimalV3()) {
                return ScalarType.createDecimalV3Type(argTypes[0].getPrimitiveType(),
                        argTypes[0].getPrecision(),
                        ((ScalarType) argTypes[0]).getScalarScale());
            } else {
                return argTypes[0];
            }
        }

        if (DECIMAL_UNARY_FUNCTION_SET.contains(fnName)) {
            return FunctionSet.MONEY_FORMAT.equals(fnName) ? Type.VARCHAR : argTypes[0];
        }

        if (DECIMAL_AGG_FUNCTION_SAME_TYPE.contains(fnName)) {
            return argTypes[0];
        }

        if (DECIMAL_AGG_FUNCTION_WIDER_TYPE.contains(fnName)) {
            Type argType = argTypes[0];
            if (!argType.isDecimalV3()) {
                return ScalarType.INVALID;
            }
            ScalarType argScalarType = (ScalarType) argType;
            int precision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128);
            int scale = argScalarType.getScalarScale();
            // TODO(by satanson): Maybe accumulating narrower decimal types to wider decimal types directly w/o
            //  casting the narrower type to the wider type is sound and efficient.
            return ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, precision, scale);
        }

        if (FunctionSet.decimalRoundFunctions.contains(fnName)) {
            return argTypes[0].isDecimalV3() ?
                    ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, argTypes[0].getPrecision(),
                            ((ScalarType) argTypes[0]).getScalarScale()) : Type.DEFAULT_DECIMAL128;
        }

        boolean hasDecimalImpl = false;
        int commonTypeStartIdx = -1;
        if (DECIMAL_IDENTICAL_TYPE_FUNCTION_SET.contains(fnName)) {
            commonTypeStartIdx = 0;
            hasDecimalImpl = true;
        } else if (fnName.equalsIgnoreCase("if")) {
            Type commonType = Type.getCommonType(argTypes, 1, argTypes.length);
            argTypes[0] = Type.BOOLEAN;
            Arrays.fill(argTypes, 1, argTypes.length, commonType);
            return commonType;
        }

        if (hasDecimalImpl) {
            if (Arrays.stream(argTypes, commonTypeStartIdx, argTypes.length).noneMatch(
                    Type::isDecimalV3)) {
                return Type.INVALID;
            }
            Type commonType = Type.getCommonType(argTypes, commonTypeStartIdx, argTypes.length);
            if (commonType.isDecimalV3()) {
                Arrays.fill(argTypes, commonTypeStartIdx, argTypes.length, commonType);
            }
            return commonType;
        }
        return Type.INVALID;
    }

    public static Function getFunctionOfRound(FunctionCallExpr node, Function fn, List<Type> argumentTypes) {
        final Type firstArgType = argumentTypes.get(0);
        final Expr secondArg;
        // For unary round, round(x) <==> round(x, 0)
        if (argumentTypes.size() == 1) {
            secondArg = new IntLiteral(0);
        } else {
            secondArg = node.getParams().exprs().get(1);
        }

        // Double version of truncate
        if (!firstArgType.isDecimalV3()) {
            return fn;
        }

        // For simplicity, we use decimal128(38, ?) as return type, so we only need to
        // figure out the scale
        final int originalScale = ((ScalarType) firstArgType).getScalarScale();
        final PrimitiveType returnPrimitiveType = PrimitiveType.DECIMAL128;
        final int returnPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128);
        final int returnScale;
        final Type returnType;

        if (secondArg instanceof IntLiteral) {
            final int expectedScale = (int) ((IntLiteral) secondArg).getValue();

            // If scale expand, we use the maximum precision as the result's precision
            if (expectedScale > originalScale) {
                // truncate(0.1, 10000) is treated as truncate(0.1, 38), type of result is decimal128(38, 38)
                if (expectedScale > PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128)) {
                    returnScale = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128);
                } else {
                    returnScale = expectedScale;
                }
            } else if (expectedScale < 0) {
                // Invalid expectedScale, modify it to lower bounder
                returnScale = 0;
            } else {
                // Scale reduce
                returnScale = expectedScale;
            }
            returnType = ScalarType.createType(returnPrimitiveType, -1, returnPrecision, returnScale);
        } else if (Expr.containsSlotRef(secondArg)) {
            returnScale = originalScale;
            returnType = ScalarType.createType(returnPrimitiveType, -1, returnPrecision, returnScale);
        } else {
            return Expr.getBuiltinFunction(fn.getFunctionName().getFunction(), new Type[] {Type.DOUBLE, Type.INT},
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        }

        ScalarFunction newFn = new ScalarFunction(fn.getFunctionName(), argumentTypes, returnType,
                fn.getLocation(), ((ScalarFunction) fn).getSymbolName(),
                ((ScalarFunction) fn).getPrepareFnSymbol(),
                ((ScalarFunction) fn).getCloseFnSymbol());
        newFn.setFunctionId(fn.getFunctionId());
        newFn.setChecksum(fn.getChecksum());
        newFn.setBinaryType(fn.getBinaryType());
        newFn.setHasVarArgs(fn.hasVarArgs());
        newFn.setId(fn.getId());
        newFn.setUserVisible(fn.isUserVisible());

        return newFn;
    }

    public static AggregateFunction rectifyAggregationFunction(AggregateFunction fn, Type argType, Type returnType) {
        if (argType.isDecimalV2() || argType.isDecimalV3()) {
            if (fn.functionName().equals(FunctionSet.COUNT)) {
                // count function return type always bigint
                returnType = fn.getReturnType();
            } else if (fn.functionName().equals(FunctionSet.AVG)) {
                // avg on decimal complies with Snowflake-style
                ScalarType decimal128p38s0 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 0);
                final ArithmeticExpr.TypeTriple triple =
                        ArithmeticExpr.getReturnTypeOfDecimal(ArithmeticExpr.Operator.DIVIDE, (ScalarType) argType,
                                decimal128p38s0);
                returnType = triple.returnType;
            } else if (DECIMAL_AGG_VARIANCE_STDDEV_TYPE.contains(fn.functionName())) {
                returnType = argType;
            } else if (argType.isDecimalV3() && DECIMAL_SUM_FUNCTION_TYPE.contains(fn.functionName())) {
                // For decimal aggregation sum, there is a risk of overflow if the scale is too large,
                // so we limit the maximum scale for this case
                if (((ScalarType) argType).getScalarScale() > 18) {
                    argType = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 18);
                    returnType = argType;
                }
            }
        }
        AggregateFunction newFn = new AggregateFunction(fn.getFunctionName(), Arrays.asList(argType), returnType,
                fn.getIntermediateType(), fn.hasVarArgs());

        newFn.setFunctionId(fn.getFunctionId());
        newFn.setChecksum(fn.getChecksum());
        newFn.setBinaryType(fn.getBinaryType());
        newFn.setHasVarArgs(fn.hasVarArgs());
        newFn.setId(fn.getId());
        newFn.setUserVisible(fn.isUserVisible());
        newFn.setisAnalyticFn(fn.isAnalyticFn());
        return newFn;
    }

    // This function is used to convert the sum(distinct) function to the multi_distinct_sum function in
    // optimizing phase and PlanFragment building phase.
    // Decimal types of multi_distinct_sum must be rectified because the function signature registered in
    // FunctionSet contains wildcard decimal types which is invalid in BE, so it is forbidden to be used
    // without decimal type rectification.
    public static Function convertSumToMultiDistinctSum(Function sumFn, Type argType) {
        AggregateFunction fn = (AggregateFunction) Expr.getBuiltinFunction(FunctionSet.MULTI_DISTINCT_SUM,
                new Type[] {argType},
                IS_NONSTRICT_SUPERTYPE_OF);
        Preconditions.checkArgument(fn != null);
        // Only rectify decimal typed functions.
        if (!argType.isDecimalV3()) {
            return fn;
        }
        ScalarType decimal128Type = ScalarType.createDecimalV3NarrowestType(38, ((ScalarType) argType).getScalarScale());
        AggregateFunction newFn = new AggregateFunction(
                fn.getFunctionName(), Arrays.asList(sumFn.getArgs()), decimal128Type,
                fn.getIntermediateType(), fn.hasVarArgs());

        newFn.setFunctionId(fn.getFunctionId());
        newFn.setChecksum(fn.getChecksum());
        newFn.setBinaryType(fn.getBinaryType());
        newFn.setHasVarArgs(fn.hasVarArgs());
        newFn.setId(fn.getId());
        newFn.setUserVisible(fn.isUserVisible());
        newFn.setisAnalyticFn(fn.isAnalyticFn());
        return newFn;
    }

    // When converting avg(distinct) into sum(distinct)/count(distinct), invoke this function to
    // rectify the sum function(is_distinct flag is on) that contains wildcard decimal types.
    public static Function rectifySumDistinct(Function sumFn, Type argType) {
        if (!argType.isDecimalV3()) {
            return sumFn;
        }
        ScalarType decimalType = (ScalarType) argType;
        AggregateFunction fn = (AggregateFunction) sumFn;
        ScalarType decimal128Type = ScalarType.createDecimalV3Type(
                PrimitiveType.DECIMAL128, 38, decimalType.getScalarScale());
        AggregateFunction newFn = new AggregateFunction(
                fn.getFunctionName(), Arrays.asList(decimalType), decimal128Type,
                fn.getIntermediateType(), fn.hasVarArgs());
        newFn.setFunctionId(fn.getFunctionId());
        newFn.setChecksum(fn.getChecksum());
        newFn.setBinaryType(fn.getBinaryType());
        newFn.setHasVarArgs(fn.hasVarArgs());
        newFn.setId(fn.getId());
        newFn.setUserVisible(fn.isUserVisible());
        newFn.setisAnalyticFn(fn.isAnalyticFn());
        return newFn;
    }
}
