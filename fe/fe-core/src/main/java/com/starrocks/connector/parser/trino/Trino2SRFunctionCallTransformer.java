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

package com.starrocks.connector.parser.trino;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CaseWhenClause;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.sql.ast.ArrayExpr;

import java.util.List;
import java.util.Map;

/**
 * This Class is the entry to to convert a Trino function to StarRocks built-in function, we use
 * {@link FunctionCallTransformer} to record the transformation of a function，Because a  function
 * name can take different arguments, a function with the same name can have multiple
 * FunctionCallTransformer, all stored in TRANSFORMER_MAP.
 */

public class Trino2SRFunctionCallTransformer {
    // function name -> list of function transformer
    public static Map<String, List<FunctionCallTransformer>> TRANSFORMER_MAP = Maps.newHashMap();

    static {
        registerAllFunctionTransformer();
    }

    public static Expr convert(String fnName, List<Expr> children) {
        List<FunctionCallTransformer> transformers = TRANSFORMER_MAP.get(fnName);
        if (transformers == null) {
            return null;
        }

        FunctionCallTransformer matcher = null;
        for (FunctionCallTransformer transformer : transformers) {
            if (transformer.match(children)) {
                matcher = transformer;
            }
        }
        if (matcher == null) {
            return null;
        }
        return matcher.transform(children);
    }

    private static void registerAllFunctionTransformer() {
        registerAggregateFunctionTransformer();
        registerArrayFunctionTransformer();
        // todo： support more function transform
    }

    private static void registerAggregateFunctionTransformer() {
        // 1.approx_distinct
        registerFunctionTransformer("approx_distinct", 1,
                "approx_count_distinct", ImmutableList.of(Expr.class));
        
        // 2. arbitrary
        registerFunctionTransformer("arbitrary", 1,
                "any_value", ImmutableList.of(Expr.class));

        // 3. approx_percentile
        registerFunctionTransformer("approx_percentile", 2,
                "percentile_approx", ImmutableList.of(Expr.class, Expr.class));

        // 4. stddev
        registerFunctionTransformer("stddev", 1,
                "stddev_samp", ImmutableList.of(Expr.class));

        // 5. stddev_pop
        registerFunctionTransformer("stddev_pop", 1,
                "stddev", ImmutableList.of(Expr.class));

        // 6. variance
        registerFunctionTransformer("variance", 1,
                "var_samp", ImmutableList.of(Expr.class));

        // 7. var_pop
        registerFunctionTransformer("var_pop", 1,
                "variance", ImmutableList.of(Expr.class));

        // 8. count_if(x) -> count(case when x then 1 end)
        registerFunctionTransformer("count_if", 1, new FunctionCallExpr("count",
                ImmutableList.of(new CaseExpr(null, ImmutableList.of(new CaseWhenClause(
                        new PlaceholderExpr(1, Expr.class), new IntLiteral(1L))), null))));
    }

    private static void registerArrayFunctionTransformer() {
        // 1. array_union -> array_distinct(array_concat(x, y))
        registerFunctionTransformer("array_union", 2, new FunctionCallExpr("array_distinct",
                ImmutableList.of(new FunctionCallExpr("array_concat", ImmutableList.of(
                        new PlaceholderExpr(1, Expr.class), new PlaceholderExpr(2, Expr.class)
                )))));

        // 2. concat -> array_concat
        registerFunctionTransformerWithVarArgs("concat", "array_concat",
                ImmutableList.of(ArrayExpr.class));

        // 3. contains -> array_contains
        registerFunctionTransformer("contains", 2, "array_contains",
                ImmutableList.of(Expr.class, Expr.class));

        // 4. contains_sequence -> array_contains_all
        registerFunctionTransformer("contains_sequence", 2, "array_contains_all",
                ImmutableList.of(Expr.class, Expr.class));

        // 5. slice -> array_slice
        registerFunctionTransformer("slice", 3, "array_slice",
                ImmutableList.of(Expr.class, Expr.class, Expr.class));
    }

    private static void registerFunctionTransformer(String trinoFnName, int trinoFnArgNums, String starRocksFnName,
                                                    List<Class<? extends Expr>> starRocksArgumentsClass) {
        FunctionCallExpr starRocksFunctionCall = buildStarRocksFunctionCall(starRocksFnName, starRocksArgumentsClass);
        registerFunctionTransformer(trinoFnName, trinoFnArgNums, starRocksFunctionCall);
    }

    private static void registerFunctionTransformerWithVarArgs(String trinoFnName, String starRocksFnName,
                                                    List<Class<? extends Expr>> starRocksArgumentsClass) {
        Preconditions.checkState(starRocksArgumentsClass.size() == 1);
        FunctionCallExpr starRocksFunctionCall = buildStarRocksFunctionCall(starRocksFnName, starRocksArgumentsClass);
        registerFunctionTransformerWithVarArgs(trinoFnName, starRocksFunctionCall);
    }

    private static void registerFunctionTransformer(String trinoFnName, int trinoFnArgNums,
                                                    FunctionCallExpr starRocksFunctionCall) {
        FunctionCallTransformer transformer = new FunctionCallTransformer(starRocksFunctionCall, trinoFnArgNums);

        List<FunctionCallTransformer> transformerList = TRANSFORMER_MAP.computeIfAbsent(trinoFnName,
                k -> Lists.newArrayList());
        transformerList.add(transformer);
    }

    private static void registerFunctionTransformerWithVarArgs(String trinoFnName, FunctionCallExpr starRocksFunctionCall) {
        FunctionCallTransformer transformer = new FunctionCallTransformer(starRocksFunctionCall, true);

        List<FunctionCallTransformer> transformerList = TRANSFORMER_MAP.computeIfAbsent(trinoFnName,
                k -> Lists.newArrayList());
        transformerList.add(transformer);
    }

    private static FunctionCallExpr buildStarRocksFunctionCall(String starRocksFnName,
                                                               List<Class<? extends Expr>> starRocksArgumentsClass) {
        List<Expr> arguments = Lists.newArrayList();
        for (int index = 0; index < starRocksArgumentsClass.size(); ++index) {
            // For a FunctionCallExpr, do not known the actual arguments here, so we use a PlaceholderExpr to replace it.
            arguments.add(new PlaceholderExpr(index + 1, starRocksArgumentsClass.get(index)));
        }
        return new FunctionCallExpr(starRocksFnName, arguments);
    }
}
