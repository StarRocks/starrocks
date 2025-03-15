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

package com.starrocks.catalog.combinator;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Lists;
import com.starrocks.analysis.FunctionParams;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.FunctionAnalyzer;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class AggStateUtils {
    // Functions that only support numeric-like types(numeric/date/bool) or string types
    public static final Set<String> ONLY_NUMERIC_ARGUMENT_FUNCTIONS_L1 =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .add(FunctionSet.MIN)
                    .add(FunctionSet.MAX)
                    .add(FunctionSet.NDV)
                    .add(FunctionSet.DS_HLL_COUNT_DISTINCT)
                    .add(FunctionSet.DS_THETA_COUNT_DISTINCT)
                    .add(FunctionSet.APPROX_COUNT_DISTINCT)
                    .add(FunctionSet.INTERSECT_COUNT)
                    .build();
    // Functions that only support numeric-like types(numeric/date/bool)
    public static final Set<String> ONLY_NUMERIC_ARGUMENT_FUNCTIONS_L2 =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .add(FunctionSet.LC_PERCENTILE_DISC)
                    .build();
    // Functions that only support numeric types
    public static final Set<String> ONLY_NUMERIC_ARGUMENT_FUNCTIONS_L3 =
            new ImmutableSortedSet.Builder<>(String.CASE_INSENSITIVE_ORDER)
                    .add(FunctionSet.SUM)
                    .add(FunctionSet.AVG)
                    .build();
    /**
     * TODO: Refactor this function to unify the same check policy with FunctionAnalyzer#analyze
     * Check if the aggregate function is supported in the combinator.
     * @param aggFunc the aggregate function to check
     * @return true if the aggregate function is supported in the combinator
     */
    public static boolean isSupportedAggStateFunction(AggregateFunction aggFunc) {
        if (aggFunc == null) {
            return false;
        }
        // unsupported functions or only used in analytic functions
        if (FunctionSet.UNSUPPORTED_AGG_STATE_FUNCTIONS.contains(aggFunc.functionName()) ||
                FunctionSet.onlyAnalyticUsedFunctions.contains(aggFunc.functionName())) {
            return false;
        }
        // unsupported argument types
        if (Stream.of(aggFunc.getArgs()).anyMatch(t -> t.isUnknown() || t.isTime() ||
                t.isBitmapType() || t.isHllType() || t.isPercentile() || t.isNull() || t.isDecimalV2())) {
            return false;
        }
        String fnName = aggFunc.functionName();
        // count only support count(col)
        if (FunctionSet.COUNT.equalsIgnoreCase(fnName) && aggFunc.getArgs().length == 0) {
            return false;
        }
        if (ONLY_NUMERIC_ARGUMENT_FUNCTIONS_L1.contains(fnName) &&
                Stream.of(aggFunc.getArgs()).anyMatch(t -> !t.canApplyToNumeric())) {
            return false;
        }
        if (ONLY_NUMERIC_ARGUMENT_FUNCTIONS_L2.contains(fnName) &&
                Stream.of(aggFunc.getArgs()).anyMatch(t -> (!t.isNumericType() || t.isStringType()))) {
            return false;
        }
        if (ONLY_NUMERIC_ARGUMENT_FUNCTIONS_L3.contains(fnName) &&
                Stream.of(aggFunc.getArgs()).anyMatch(t -> !t.isNumericType())) {
            return false;
        }
        // max_by
        if (FunctionSet.MAX_BY.equalsIgnoreCase(fnName) || FunctionSet.MIN_BY.equalsIgnoreCase(fnName)) {
            if (aggFunc.getArgs().length != 2) {
                return false;
            }
            if (aggFunc.getArgs()[0].isBinaryType()) {
                return false;
            }
            if (!aggFunc.getArgs()[1].canApplyToNumeric()) {
                return false;
            }
        }
        // bitmap_union_int
        if (FunctionSet.BITMAP_UNION_INT.equalsIgnoreCase(fnName) && !aggFunc.getArgs()[0].isIntegerType()) {
            return false;
        }
        return true;
    }

    /**
     * Get the aggregate function name of the combinator function.
     * @param fnName combinator function name
     */
    public static String getAggFuncNameOfCombinator(String fnName) {
        if (fnName.endsWith(FunctionSet.AGG_STATE_SUFFIX)) {
            return fnName.substring(0, fnName.length() - FunctionSet.AGG_STATE_SUFFIX.length());
        } else if (fnName.endsWith(FunctionSet.AGG_STATE_UNION_SUFFIX)) {
            return fnName.substring(0, fnName.length() - FunctionSet.AGG_STATE_UNION_SUFFIX.length());
        } else if (fnName.endsWith(FunctionSet.AGG_STATE_MERGE_SUFFIX)) {
            return fnName.substring(0, fnName.length() - FunctionSet.AGG_STATE_MERGE_SUFFIX.length());
        } else {
            return fnName;
        }
    }

    /**
     * Analyze the combinator function and return the correct aggregate function for type correction.
     * @param session connect context
     * @param func combinator function
     * @param params function parameters
     * @param argumentTypes argument types
     * @param argumentIsConstants argument is constant or not
     * @param pos node position
     * @return the correct aggregate function for type correction
     */
    public static Function getAnalyzedCombinatorFunction(ConnectContext session,
                                                          Function func,
                                                          FunctionParams params,
                                                          Type[] argumentTypes,
                                                          Boolean[] argumentIsConstants,
                                                          NodePosition pos) {
        Optional<? extends Function> result = Optional.empty();
        if (func instanceof AggStateCombinator) {
            // correct aggregate function for type correction
            // `_state`'s input types are the same with inputs' types.
            String aggFuncName = AggStateUtils.getAggFuncNameOfCombinator(func.functionName());
            Function argFn = FunctionAnalyzer.getAnalyzedAggregateFunction(session, aggFuncName,
                    params, argumentTypes, argumentIsConstants, pos);
            if (argFn == null) {
                return null;
            }
            if (!(argFn instanceof AggregateFunction)) {
                return null;
            }
            AggregateFunction aggFunc = (AggregateFunction) argFn;
            if (aggFunc.getNumArgs() == 1) {
                // only copy argument if it's a decimal type
                AggregateFunction argFnCopy = (AggregateFunction) aggFunc.copy();
                argFnCopy.setArgsType(argumentTypes);
                result = AggStateCombinator.of(argFnCopy);
            } else {
                result = AggStateCombinator.of(aggFunc);
            }
        } else if (func instanceof AggStateUnionCombinator) {
            AggregateFunction argFn = getAggStateFunction(session, func, argumentTypes, pos);
            if (argFn == null) {
                return null;
            }
            result = AggStateUnionCombinator.of(argFn);
        } else if (func instanceof AggStateMergeCombinator) {
            AggregateFunction argFn = getAggStateFunction(session, func, argumentTypes, pos);
            if (argFn == null) {
                return null;
            }
            result = AggStateMergeCombinator.of(argFn);
        }

        if (result.isEmpty()) {
            return null;
        }
        return result.get();
    }

    private static AggregateFunction getAggStateFunction(ConnectContext session,
                                                         Function inputFunc,
                                                         Type[] argumentTypes,
                                                         NodePosition pos) {
        Preconditions.checkArgument(argumentTypes.length == 1,
                "AggState's AggFunc should have only one argument");
        Type arg0Type = argumentTypes[0];
        if (arg0Type.getAggStateDesc() == null) {
            throw new SemanticException(String.format("AggState's AggFunc should have AggStateDesc: %s",
                    inputFunc), pos);
        }
        AggStateDesc aggStateDesc = arg0Type.getAggStateDesc();
        List<Type> argTypes = aggStateDesc.getArgTypes();
        String argFnName = aggStateDesc.getFunctionName();
        Type[] newArgumentTypes = getNewArgumentTypes(argTypes.toArray(new Type[0]), argFnName, arg0Type);
        FunctionParams argParams = new FunctionParams(false, Lists.newArrayList());
        Boolean[] argArgumentConstants = IntStream.range(0, argTypes.size())
                .mapToObj(x -> false).toArray(Boolean[]::new);
        Function fn = FunctionAnalyzer.getAnalyzedAggregateFunction(session,
                argFnName, argParams, newArgumentTypes, argArgumentConstants, pos);
        if (fn == null) {
            return null;
        }
        if (!(fn instanceof AggregateFunction)) {
            return null;
        }
        AggregateFunction result = (AggregateFunction) fn.copy();
        result.setAggStateDesc(aggStateDesc);
        return result;
    }

    private static Type[] getNewArgumentTypes(Type[] origArgTypes, String argFnName, Type arg0Type) {
        Type[] newArgumentTypes;
        if (FunctionSet.ARRAY_AGG_DISTINCT.equalsIgnoreCase(argFnName)) {
            // array_agg_distinct use array's item as its input
            ArrayType arrayType = (ArrayType) arg0Type;
            newArgumentTypes = new Type[] {arrayType.getItemType()};
        } else {
            // multi_distinct_count/any_value/array_unique_agg use array as its input, which is the same as its output
            newArgumentTypes = origArgTypes;
        }
        return newArgumentTypes;
    }
}
