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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
                    .add(FunctionSet.ANY_VALUE)
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

    private static final ScalarType DECIMAL128P38S0 = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 0);

    // For decimal32/64/128 types, normalize argType's scale and precision
    private static Type[] normalizeDecimalArgTypes(final Type[] argTypes, String fnName) {
        if (argTypes == null || argTypes.length == 0) {
            return argTypes;
        }

        if (DECIMAL_IDENTICAL_TYPE_FUNCTION_SET.contains(fnName) || fnName.equalsIgnoreCase(FunctionSet.IF)) {
            boolean isIfFunc = fnName.equals(FunctionSet.IF);
            int commonTypeStartIdx = isIfFunc ? 1 : 0;
            if (Arrays.stream(argTypes, commonTypeStartIdx, argTypes.length).noneMatch(Type::isDecimalV3)) {
                return argTypes;
            }
            Type commonType = Type.getCommonType(argTypes, commonTypeStartIdx, argTypes.length);
            Type[] newArgType = new Type[argTypes.length];
            newArgType[0] = isIfFunc ? Type.BOOLEAN : argTypes[0];
            Arrays.fill(newArgType, commonTypeStartIdx, argTypes.length, commonType);
            return newArgType;
        }

        if (FunctionSet.ARRAY_INTERSECT.equalsIgnoreCase(fnName) || FunctionSet.ARRAY_CONCAT.equalsIgnoreCase(fnName)) {
            Type[] childTypes = Arrays.stream(argTypes).map(a -> {
                if (a.isArrayType()) {
                    return ((ArrayType) a).getItemType();
                } else {
                    return a;
                }
            }).toArray(Type[]::new);
            Preconditions.checkState(Arrays.stream(childTypes).anyMatch(Type::isDecimalV3));
            Type commonType = new ArrayType(Type.getCommonType(childTypes, 0, childTypes.length));
            return Arrays.stream(argTypes).map(t -> commonType).toArray(Type[]::new);
        }

        if (FunctionSet.ARRAYS_OVERLAP.equalsIgnoreCase(fnName)) {
            Preconditions.checkState(argTypes.length == 2);
            Type[] childTypes = Arrays.stream(argTypes).map(a -> {
                if (a.isArrayType()) {
                    return ((ArrayType) a).getItemType();
                } else {
                    return a;
                }
            }).toArray(Type[]::new);
            ArrayType commonType = new ArrayType(Type.getAssignmentCompatibleType(childTypes[0], childTypes[1], false));
            return new Type[] {commonType, commonType};
        }

        return argTypes;
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
            // Invalid expectedScale, modify it to lower bounder
            // Scale reduce
            if (expectedScale > originalScale) {
                // truncate(0.1, 10000) is treated as truncate(0.1, 38), type of result is decimal128(38, 38)
                returnScale =
                        Math.min(expectedScale, PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128));
            } else {
                returnScale = Math.max(expectedScale, 0);
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
                final ArithmeticExpr.TypeTriple triple =
                        ArithmeticExpr.getReturnTypeOfDecimal(ArithmeticExpr.Operator.DIVIDE, (ScalarType) argType,
                                DECIMAL128P38S0);
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

        Type[] decimalTypes = new Type[fn.getNumArgs()];
        Arrays.fill(decimalTypes, argType);
        Type[] args = replaceArgDecimalType(fn.getArgs(), decimalTypes);

        AggregateFunction newFn = (AggregateFunction) fn.copy();
        newFn.setArgsType(args);
        newFn.setRetType(returnType);
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
        ScalarType decimal128Type =
                ScalarType.createDecimalV3NarrowestType(38, ((ScalarType) argType).getScalarScale());
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
                fn.getFunctionName(), Collections.singletonList(decimalType), decimal128Type,
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

    public static boolean argumentTypeContainDecimalV3(String fnName, Type[] argumentTypes) {
        if (FunctionSet.DECIMAL_ROUND_FUNCTIONS.contains(fnName)) {
            return true;
        }

        if (Arrays.stream(argumentTypes).anyMatch(Type::isDecimalV3)) {
            return true;
        }

        // check array child type
        return Arrays.stream(argumentTypes).filter(Type::isArrayType).map(t -> (ArrayType) t)
                .anyMatch(t -> t.getItemType().isDecimalV3());
    }

    // Multi parameters decimalV2 function will match decimalV3 function, and doesn't set decimalV3 precision&scale.
    // like that, user input may: array_slice(decimalV2, tinyint), sr always match array_slice(decimalV3, bigint),
    // because array_slice(decimalV2, tinyint) can't match array_slice(decimalV2, bigint) in IS_IDENTICAL mode, but
    // only match array_slice(decimalV3, bigint) in IS_NONSTRICT_SUPERTYPE_OF mode (decimalV3 function is higher
    // than decimalV2 function).
    public static boolean argumentTypeContainDecimalV2(String fnName, Type[] argumentTypes) {
        if (!FunctionSet.ARRAY_SLICE.equals(fnName)) {
            return false;
        }

        if (Arrays.stream(argumentTypes).anyMatch(Type::isDecimalV2)) {
            return true;
        }

        // check array child type
        return Arrays.stream(argumentTypes).filter(Type::isArrayType).map(t -> (ArrayType) t)
                .anyMatch(t -> t.getItemType().isDecimalV2());
    }

    public static Function getDecimalV2Function(FunctionCallExpr node, Type[] argumentTypes) {
        String fnName = node.getFnName().getFunction();
        argumentTypes = normalizeDecimalArgTypes(argumentTypes, fnName);

        if (FunctionSet.ARRAY_SLICE.equals(fnName)) {
            Type[] clone = Arrays.copyOf(argumentTypes, argumentTypes.length);
            for (int i = 1; i < clone.length; i++) {
                clone[i] = Type.BIGINT;
            }

            argumentTypes = clone;
        }

        return Expr.getBuiltinFunction(fnName, argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
    }

    public static Function getDecimalV3Function(ConnectContext session, FunctionCallExpr node, Type[] argumentTypes) {
        String fnName = node.getFnName().getFunction();
        if (FunctionSet.VARIANCE_FUNCTIONS.contains(fnName)) {
            // When decimal values are too small, the stddev and variance alogrithm of decimal-version do not
            // work incorrectly. because we use decimal128(38,9) multiplication in this algorithm,
            // decimal128(38,9) * decimal128(38,9) produces a result of decimal128(38,9). if two numbers are
            // too small, for an example, 0.000000001 * 0.000000001 produces 0.000000000, so the algorithm
            // can not work. Because of this reason, stddev and variance on very small decimal numbers always
            // yields a zero, so we use double instead of decimal128(38,9) to compute stddev and variance of
            // decimal types.
            Type[] doubleArgTypes = Stream.of(argumentTypes).map(t -> Type.DOUBLE).toArray(Type[]::new);
            return Expr.getBuiltinFunction(fnName, doubleArgTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        }

        // modify search argument types
        argumentTypes = normalizeDecimalArgTypes(argumentTypes, fnName);
        Function fn = Expr.getBuiltinFunction(fnName, argumentTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);

        if (fn == null) {
            fn = AnalyzerUtils.getUdfFunction(session, node.getFnName(), argumentTypes);
        }

        if (fn == null) {
            String msg = String.format("No matching function with signature: %s(%s).", fnName,
                    node.getParams().isStar() ? "*" : Joiner.on(", ")
                            .join(Arrays.stream(argumentTypes).map(Type::toSql).collect(Collectors.toList())));
            throw new SemanticException(msg, node.getPos());
        }

        Function newFn = fn;
        if (DECIMAL_AGG_FUNCTION_SAME_TYPE.contains(fnName) || DECIMAL_AGG_FUNCTION_WIDER_TYPE.contains(fnName)) {
            Type commonType = Type.INVALID;
            if (FunctionSet.HISTOGRAM.equals(fnName)) {
                commonType = Type.VARCHAR;
            } else if (DECIMAL_AGG_FUNCTION_SAME_TYPE.contains(fnName)) {
                commonType = argumentTypes[0];
            } else if (DECIMAL_AGG_FUNCTION_WIDER_TYPE.contains(fnName) && argumentTypes[0].isDecimalV3()) {
                ScalarType argScalarType = (ScalarType) argumentTypes[0];
                int precision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128);
                int scale = argScalarType.getScalarScale();
                // TODO(by satanson): Maybe accumulating narrower decimal types to wider decimal types directly w/o
                //  casting the narrower type to the wider type is sound and efficient.
                commonType = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, precision, scale);
            }

            Type argType = node.getChild(0).getType();
            // stddev/variance always use decimal128(38,9) to computing result.
            if (DECIMAL_AGG_VARIANCE_STDDEV_TYPE.contains(fnName) && argType.isDecimalV3()) {
                argType = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 9);
            }
            newFn = DecimalV3FunctionAnalyzer
                    .rectifyAggregationFunction((AggregateFunction) fn, argType, commonType);
        } else if (FunctionSet.MAX_BY.equals(fnName) || FunctionSet.MIN_BY.equals(fnName)) {
            Type returnType = fn.getReturnType();
            // Decimal v3 function return type maybe need change
            ScalarType firstType = (ScalarType) argumentTypes[0];
            Type commonType = firstType;
            if (firstType.isDecimalV3()) {
                commonType = ScalarType.createDecimalV3Type(firstType.getPrimitiveType(),
                        firstType.getPrecision(), firstType.getScalarScale());
            }
            if (returnType.isDecimalV3() && commonType.isValid()) {
                returnType = commonType;
            }
            Preconditions.checkState(fn instanceof AggregateFunction);

            Type[] argTypes = replaceArgDecimalType(fn.getArgs(), argumentTypes);
            newFn = fn.copy();
            newFn.setArgsType(argTypes);
            newFn.setRetType(returnType);
            ((AggregateFunction) newFn).setIntermediateType(Type.VARCHAR);
        } else if (DECIMAL_UNARY_FUNCTION_SET.contains(fnName)) {
            Type commonType = argumentTypes[0];
            Type returnType = fn.getReturnType();
            // Decimal v3 function return type maybe need change
            if (returnType.isDecimalV3() && commonType.isValid()) {
                returnType = commonType;
            }

            Type[] argTypes = replaceArgDecimalType(fn.getArgs(), argumentTypes);
            newFn = fn.copy();
            newFn.setArgsType(argTypes);
            newFn.setRetType(returnType);
        } else if (DECIMAL_IDENTICAL_TYPE_FUNCTION_SET.contains(fnName) || FunctionSet.IF.equals(fnName)) {
            int commonTypeStartIdx = fnName.equalsIgnoreCase("if") ? 1 : 0;
            Type commonType = argumentTypes[commonTypeStartIdx];
            Type returnType = fn.getReturnType();
            // Decimal v3 function return type maybe need change
            if (returnType.isDecimalV3() && commonType.isValid()) {
                returnType = commonType;
            }

            Type[] argTypes = replaceArgDecimalType(fn.getArgs(), argumentTypes);
            newFn = fn.copy();
            newFn.setRetType(returnType);
            newFn.setArgsType(argTypes);
        } else if (FunctionSet.DECIMAL_ROUND_FUNCTIONS.contains(fnName)) {
            // Decimal version of truncate/round/round_up_to may change the scale, we need to calculate the scale of the return type
            // And we need to downgrade to double version if second param is neither int literal nor SlotRef expression
            Type commonType = argumentTypes[0].isDecimalV3() ?
                    ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, argumentTypes[0].getPrecision(),
                            ((ScalarType) argumentTypes[0]).getScalarScale()) : Type.DEFAULT_DECIMAL128;
            List<Type> argTypes = Arrays.stream(fn.getArgs()).map(t -> t.isDecimalV3() ? commonType : t)
                    .collect(Collectors.toList());
            newFn = getFunctionOfRound(node, fn, argTypes);
        } else if (FunctionSet.ARRAY_DECIMAL_FUNCTIONS.contains(fnName)) {
            newFn = getArrayDecimalFunction(fn, argumentTypes);
        }

        if (newFn != null) {
            Preconditions.checkState(newFn.getNumArgs() == fn.getNumArgs());
        }
        return newFn;
    }

    private static Type[] replaceArgDecimalType(Type[] originTypes, Type[] normalizedTypes) {
        Preconditions.checkState(originTypes.length <= normalizedTypes.length);
        Type[] result = new Type[originTypes.length];
        for (int i = 0; i < originTypes.length; i++) {
            if (originTypes[i].isDecimalV3()) {
                result[i] = normalizedTypes[i];
            } else {
                result[i] = originTypes[i];
            }
        }
        return result;
    }

    private static Function getArrayDecimalFunction(Function fn, Type[] argumentTypes) {
        Function newFn = fn.copy();
        Preconditions.checkState(argumentTypes.length > 0);
        switch (fn.functionName()) {
            case FunctionSet.ARRAY_DISTINCT:
            case FunctionSet.ARRAY_SORT:
            case FunctionSet.REVERSE:
            case FunctionSet.ARRAY_INTERSECT:
            case FunctionSet.ARRAY_CONCAT: {
                newFn.setArgsType(new Type[] {argumentTypes[0]});
                newFn.setRetType(argumentTypes[0]);
                return newFn;
            }
            case FunctionSet.ARRAY_MAX:
            case FunctionSet.ARRAY_MIN: {
                Type decimalType = ((ArrayType) argumentTypes[0]).getItemType();
                newFn.setArgsType(argumentTypes);
                newFn.setRetType(decimalType);
                return newFn;
            }
            case FunctionSet.ARRAY_SUM:
            case FunctionSet.ARRAY_AVG: {
                ScalarType decimalType = (ScalarType) ((ArrayType) argumentTypes[0]).getItemType();
                int precision = PrimitiveType.getMaxPrecisionOfDecimal(PrimitiveType.DECIMAL128);
                int scale = decimalType.getScalarScale();
                ScalarType retType = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, precision, scale);
                newFn.setArgsType(new Type[] {new ArrayType(decimalType)});
                if (FunctionSet.ARRAY_AVG.equals(fn.functionName())) {
                    // avg on decimal complies with Snowflake-style
                    ArithmeticExpr.TypeTriple triple =
                            ArithmeticExpr.getReturnTypeOfDecimal(ArithmeticExpr.Operator.DIVIDE, decimalType,
                                    DECIMAL128P38S0);
                    retType = triple.returnType;
                    Preconditions.checkState(PrimitiveType.DECIMAL128.equals(retType.getPrimitiveType()));
                    newFn.setArgsType(argumentTypes);
                } else if (FunctionSet.ARRAY_SUM.equals(fn.functionName()) && retType.getScalarScale() > 18) {
                    retType = ScalarType.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 18);
                    newFn.setArgsType(new Type[] {new ArrayType(retType)});
                }
                newFn.setRetType(retType);
                return newFn;
            }
            case FunctionSet.ARRAY_DIFFERENCE: {
                ScalarType decimalType = (ScalarType) ((ArrayType) argumentTypes[0]).getItemType();
                ArithmeticExpr.TypeTriple triple =
                        ArithmeticExpr.getReturnTypeOfDecimal(ArithmeticExpr.Operator.SUBTRACT, decimalType,
                                decimalType);
                newFn.setArgsType(argumentTypes);
                newFn.setRetType(new ArrayType(triple.returnType));
                return newFn;
            }
            case FunctionSet.ARRAYS_OVERLAP: {
                newFn.setArgsType(argumentTypes);
                return newFn;
            }
            case FunctionSet.ARRAY_SLICE: {
                newFn.setRetType(argumentTypes[0]);
                return newFn;
            }
            default:
                return fn;
        }
    }
}
