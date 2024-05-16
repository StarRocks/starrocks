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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.ImmutableMap;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.starrocks.catalog.Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF;

/**
 * `AggregateFunctionRewriter` will try to rewrite some agg functions to some transformations so can be
 * better to be rewritten.
 * eg: AVG -> SUM / COUNT
 */
public class AggregateFunctionRollupUtils {
    private static final Logger LOG = LogManager.getLogger(AggregateFunctionRollupUtils.class);

    // Unsafe means not exactly equal for each other, cannot be used directly and need
    // extra routines to handle the difference.
    // eg: count(col) = coalesce(sum(col), 0)
    public static final Map<String, String> UNSAFE_REWRITE_ROLLUP_FUNCTION_MAP = ImmutableMap.<String, String>builder()
            .put(FunctionSet.COUNT, FunctionSet.SUM)
            .build();

    // Functions that rollup function name is different from original function name.
    public static final Map<String, String> SAFE_REWRITE_ROLLUP_FUNCTION_MAP = ImmutableMap.<String, String>builder()
            // Functions and rollup functions are the same.
            .put(FunctionSet.SUM, FunctionSet.SUM)
            .put(FunctionSet.MAX, FunctionSet.MAX)
            .put(FunctionSet.MIN, FunctionSet.MIN)
            .put(FunctionSet.BITMAP_UNION, FunctionSet.BITMAP_UNION)
            .put(FunctionSet.HLL_UNION, FunctionSet.HLL_UNION)
            .put(FunctionSet.PERCENTILE_UNION, FunctionSet.PERCENTILE_UNION)
            .put(FunctionSet.ANY_VALUE, FunctionSet.ANY_VALUE)
            // Functions and rollup functions are not the same.
            .put(FunctionSet.BITMAP_AGG, FunctionSet.BITMAP_UNION)
            .put(FunctionSet.ARRAY_AGG_DISTINCT, FunctionSet.ARRAY_UNIQUE_AGG)
            .build();

    public static final Map<String, String> REWRITE_ROLLUP_FUNCTION_MAP = ImmutableMap.<String, String>builder()
            .putAll(UNSAFE_REWRITE_ROLLUP_FUNCTION_MAP)
            .putAll(SAFE_REWRITE_ROLLUP_FUNCTION_MAP)
            .build();

    public static final Map<String, String> SUPPORTED_DISTINCT_ROLLUP_FUNCTIONS = ImmutableMap.<String, String>builder()
            .put(FunctionSet.ARRAY_AGG, FunctionSet.ARRAY_UNIQUE_AGG)
            .build(); // array_agg is not supported to rollup yet.

    // Functions that rollup function name is different from original function name.
    public static final Map<String, String> TO_REWRITE_ROLLUP_FUNCTION_MAP = ImmutableMap.<String, String>builder()
            .put(FunctionSet.COUNT, FunctionSet.SUM)
            .put(FunctionSet.BITMAP_AGG, FunctionSet.BITMAP_UNION)
            .put(FunctionSet.ARRAY_AGG_DISTINCT, FunctionSet.ARRAY_UNIQUE_AGG)
            .build();

    /**
     * There is some difference between whether it's a mv union rewrite or not.
     * eg: count(distinct) is not supported to rollup in mv union rewrite, but it's safe for mv union rewrite.
     * @param aggFunc: original aggregate function to get the associated rollup function
     * @param isUnionRewrite: whether it's a mv union rewrite
     * @return: the associated rollup function name
     */
    public static String getRollupFunctionName(CallOperator aggFunc, boolean isUnionRewrite) {
        String fn = aggFunc.getFnName();
        if ((isUnionRewrite || !aggFunc.isDistinct()) && REWRITE_ROLLUP_FUNCTION_MAP.containsKey(fn)) {
            return REWRITE_ROLLUP_FUNCTION_MAP.get(fn);
        }

        if (aggFunc.isDistinct() && SUPPORTED_DISTINCT_ROLLUP_FUNCTIONS.containsKey(fn)) {
            return SUPPORTED_DISTINCT_ROLLUP_FUNCTIONS.get(fn);
        }
        return null;
    }

    public static ScalarOperator genRollupProject(CallOperator aggCall, ColumnRefOperator oldColRef,
                                                  boolean hasGroupByKeys) {
        if (!hasGroupByKeys && aggCall.getFnName().equals(FunctionSet.COUNT)) {
            // NOTE: This can only happen when query has no group-by keys.
            // The behavior is different between count(NULL) and sum(NULL),  count(NULL) = 0, sum(NULL) = NULL.
            // Add `coalesce(count_col, 0)` to avoid return NULL instead of 0 for count rollup.
            List<ScalarOperator> args = Arrays.asList(oldColRef, ConstantOperator.createBigint(0L));
            Type[] argTypes = args.stream().map(a -> a.getType()).toArray(Type[]::new);
            return new CallOperator(FunctionSet.COALESCE, aggCall.getType(), args,
                    Expr.getBuiltinFunction(FunctionSet.COALESCE, argTypes, IS_NONSTRICT_SUPERTYPE_OF));
        } else {
            return oldColRef;
        }
    }
}
