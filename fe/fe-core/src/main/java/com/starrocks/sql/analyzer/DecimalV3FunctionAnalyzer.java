// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.collect.ImmutableSortedSet;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.common.TypeManager;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class DecimalV3FunctionAnalyzer {
    public static final Set<String> DECIMAL_UNARY_FUNCTION_SET =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .add("abs").add("positive").add("negative").add("money_format").build();

    public static final Set<String> DECIMAL_IDENTICAL_TYPE_FUNCTION_SET =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .add("least").add("greatest").add("nullif").add("ifnull").add("coalesce").add("mod").build();

    public static final Set<String> DECIMAL_AGG_FUNCTION_SAME_TYPE =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .add(FunctionSet.MAX).add(FunctionSet.MIN)
                    .add(FunctionSet.LEAD).add(FunctionSet.LAG)
                    .add(FunctionSet.FIRST_VALUE).add(FunctionSet.LAST_VALUE)
                    .add(FunctionSet.ANY_VALUE).add(FunctionSet.ARRAY_AGG).build();

    public static final Set<String> DECIMAL_AGG_FUNCTION_WIDER_TYPE =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .add(FunctionSet.COUNT)
                    .add("sum").add("sum_distinct").add(FunctionSet.MULTI_DISTINCT_SUM).add("avg").add("variance")
                    .add("variance_pop").add("var_pop").add("variance_samp").add("var_samp")
                    .add("stddev").add("stddev_pop").add("stddev_samp").build();

    public static final Set<String> DECIMAL_AGG_VARIANCE_STDDEV_TYPE =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .add("variance").add("variance_pop").add("var_pop").add("variance_samp").add("var_samp")
                    .add("stddev").add("stddev_pop").add("stddev_samp").build();

    public static final Set<String> DECIMAL_AGG_FUNCTION =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .addAll(DECIMAL_AGG_FUNCTION_SAME_TYPE)
                    .addAll(DECIMAL_AGG_FUNCTION_WIDER_TYPE).build();

    // For decimal32/64/128 types, scale and precision of returnType depends on argTypes'
    public static Type normalizeDecimalArgTypes(Type[] argTypes, String fnName) {
        if (argTypes == null || argTypes.length == 0) {
            return Type.INVALID;
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

        if (FunctionSet.TRUNCATE_DECIMAL.equalsIgnoreCase(fnName)) {
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
            commonTypeStartIdx = 1;
            hasDecimalImpl = true;
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

    public static Type getReturnTypeOfTruncate(FunctionCallExpr node, List<Type> argTypes) {
        Type firstArgType = argTypes.get(0);
        Expr secondArg = node.getParams().exprs().get(1);

        // TODO(hcf) How to deduce the type of return
        if (!firstArgType.isDecimalV3()) {
            return Type.DEFAULT_DECIMAL128;
        }

        // For simplicity, we use decimal128(38, ?) as return type, so we only need to
        // figure out the scale
        final int originalScale = ((ScalarType) firstArgType).getScalarScale();
        final PrimitiveType returnPrimitiveType = PrimitiveType.DECIMAL128;
        final int returnPrecision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128);
        final int returnScale;

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
        } else if (Expr.containsSlotRef(secondArg)) {
            returnScale = originalScale;
        } else {
            throw new StarRocksPlannerException(
                    "Second arg of function truncate_decimal must be int literal or slotRef expression.",
                    ErrorType.USER_ERROR);
        }

        return ScalarType.createType(returnPrimitiveType, -1, returnPrecision,
                returnScale);
    }

    public static AggregateFunction rectifyAggregationFunction(AggregateFunction fn, Type argType, Type returnType) {
        if (argType.isDecimalV3()) {
            if (fn.functionName().equals(FunctionSet.COUNT)) {
                // count function return type always bigint
                returnType = fn.getReturnType();
            } else if (fn.functionName().equals(FunctionSet.AVG)) {
                // avg on decimal complies with Snowflake-style
                ScalarType decimal128p38s0 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 0);
                TypeManager.TypeTriple triple = TypeManager.getReturnTypeOfDecimal(
                        ArithmeticExpr.Operator.DIVIDE, (ScalarType) argType, decimal128p38s0);
                returnType = triple.returnType;
            } else if (DECIMAL_AGG_VARIANCE_STDDEV_TYPE.contains(fn.functionName())) {
                returnType = argType;
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
}
