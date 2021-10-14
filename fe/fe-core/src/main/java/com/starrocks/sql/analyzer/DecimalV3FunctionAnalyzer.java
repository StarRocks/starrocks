// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer;

import com.google.common.collect.ImmutableSortedSet;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.common.TypeManager;

import java.util.Arrays;
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
                    .add(FunctionSet.FIRST_VALUE).add(FunctionSet.LAST_VALUE).build();

    public static final Set<String> DECIMAL_AGG_FUNCTION_WIDER_TYPE =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .add("sum").add("sum_distinct").add("multi_distinct_sum").add("avg").add("variance")
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
    public static Type normalizeDecimalArgTypes(Type[] argTypes, FunctionName fnName) {
        if (argTypes == null || argTypes.length == 0) {
            return Type.INVALID;
        }

        String name = fnName.getFunction();
        if (DECIMAL_UNARY_FUNCTION_SET.contains(name)) {
            return name.equalsIgnoreCase("money_format") ? Type.VARCHAR : argTypes[0];
        }

        if (DECIMAL_AGG_FUNCTION_SAME_TYPE.contains(name)) {
            return argTypes[0];
        }

        if (DECIMAL_AGG_FUNCTION_WIDER_TYPE.contains(name)) {
            Type argType = argTypes[0];
            if (!argType.isDecimalV3()) {
                return ScalarType.INVALID;
            }
            ScalarType argScalarType = (ScalarType) argType;
            // use a wider type to prevent accumulation from overflowing
            PrimitiveType widerPrimitiveType = PrimitiveType.getWiderDecimalV3Type(
                    PrimitiveType.DECIMAL64, argScalarType.getPrimitiveType());
            int precision = PrimitiveType.getMaxPrecisionOfDecimal(widerPrimitiveType);
            int scale = argScalarType.getScalarScale();
            // TODO(by satanson): Maybe accumulating narrower decimal types to wider decimal types directly w/o
            //  casting the narrower type to the wider type is sound and efficient.
            return ScalarType.createDecimalV3Type(widerPrimitiveType, precision, scale);
        }

        boolean hasDecimalImpl = false;
        int commonTypeStartIdx = -1;
        if (DECIMAL_IDENTICAL_TYPE_FUNCTION_SET.contains(name)) {
            commonTypeStartIdx = 0;
            hasDecimalImpl = true;
        } else if (name.equalsIgnoreCase("if")) {
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

    public static AggregateFunction rectifyAggregationFunction(AggregateFunction fn, Type argType, Type returnType) {
        if (argType.isDecimalV3()) {
            // avg on decimal complies with Snowflake-style
            if (fn.functionName().equalsIgnoreCase("avg")) {
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
