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
import com.starrocks.analysis.StringLiteral;

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
        Expr result = convertRegisterFn(fnName, children);
        if (result == null) {
            result = ComplexFunctionCallTransformer.transform(fnName, children.toArray(new Expr[0]));
        }
        return result;
    }

    public static Expr convertRegisterFn(String fnName, List<Expr> children) {
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
        registerDateFunctionTransformer();
        registerStringFunctionTransformer();
        registerRegexpFunctionTransformer();
        registerJsonFunctionTransformer();
        registerBitwiseFunctionTransformer();
        registerUnicodeFunctionTransformer();
        registerMapFunctionTransformer();
        registerBinaryFunctionTransformer();
        // todo: support more function transform
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
        // array_union -> array_distinct(array_concat(x, y))
        registerFunctionTransformer("array_union", 2, new FunctionCallExpr("array_distinct",
                ImmutableList.of(new FunctionCallExpr("array_concat", ImmutableList.of(
                        new PlaceholderExpr(1, Expr.class), new PlaceholderExpr(2, Expr.class)
                )))));
        // contains -> array_contains
        registerFunctionTransformer("contains", 2, "array_contains",
                ImmutableList.of(Expr.class, Expr.class));
        // slice -> array_slice
        registerFunctionTransformer("slice", 3, "array_slice",
                ImmutableList.of(Expr.class, Expr.class, Expr.class));
        // filter(array, lambda) -> array_filter(array, lambda)
        registerFunctionTransformer("filter", 2, "array_filter",
                ImmutableList.of(Expr.class, Expr.class));
    }

    private static void registerDateFunctionTransformer() {
        // to_unixtime -> unix_timestamp
        registerFunctionTransformer("to_unixtime", 1, "unix_timestamp",
                ImmutableList.of(Expr.class));

        // date_parse -> str_to_date
        registerFunctionTransformer("date_parse", 2, "str_to_date",
                ImmutableList.of(Expr.class, Expr.class));

        // day_of_week -> dayofweek
        registerFunctionTransformer("day_of_week", 1, "dayofweek_iso",
                ImmutableList.of(Expr.class));

        // dow -> dayofweek
        registerFunctionTransformer("dow", 1, "dayofweek_iso",
                ImmutableList.of(Expr.class));

        // day_of_month -> dayofmonth
        registerFunctionTransformer("day_of_month", 1, "dayofmonth",
                ImmutableList.of(Expr.class));

        // day_of_year -> dayofyear
        registerFunctionTransformer("day_of_year", 1, "dayofyear",
                ImmutableList.of(Expr.class));

        // doy -> dayofyear
        registerFunctionTransformer("doy", 1, "dayofyear",
                ImmutableList.of(Expr.class));

        // week_of_year -> week_iso
        registerFunctionTransformer("week_of_year", 1, "week_iso",
                ImmutableList.of(Expr.class));

        // week -> week_iso
        registerFunctionTransformer("week", 1, "week_iso",
                ImmutableList.of(Expr.class));

        // format_datetime -> jodatime_format
        registerFunctionTransformer("format_datetime", 2, "jodatime_format",
                ImmutableList.of(Expr.class, Expr.class));
    }

    private static void registerStringFunctionTransformer() {
        // chr -> char
        registerFunctionTransformer("chr", 1, "char", ImmutableList.of(Expr.class));

        // codepoint -> ascii
        registerFunctionTransformer("codepoint", 1, "ascii", ImmutableList.of(Expr.class));

        // strpos -> locate
        registerFunctionTransformer("strpos", 2, new FunctionCallExpr("locate",
                ImmutableList.of(new PlaceholderExpr(2, Expr.class), new PlaceholderExpr(1, Expr.class))));

        // length -> char_length
        registerFunctionTransformer("length", 1, "char_length", ImmutableList.of(Expr.class));
    }

    private static void registerRegexpFunctionTransformer() {
        // regexp_like -> regexp
        registerFunctionTransformer("regexp_like", 2, "regexp",
                ImmutableList.of(Expr.class, Expr.class));

        // regexp_extract(string, pattern) -> regexp_extract(str, pattern, 0)
        registerFunctionTransformer("regexp_extract", 2, new FunctionCallExpr("regexp_extract",
                ImmutableList.of(new PlaceholderExpr(1, Expr.class), new PlaceholderExpr(2, Expr.class), new IntLiteral(0L))));
    }

    private static void registerJsonFunctionTransformer() {
        // json_array_length -> json_length
        registerFunctionTransformer("json_array_length", 1, "json_length",
                ImmutableList.of(Expr.class));

        // json_parse -> parse_json
        registerFunctionTransformer("json_parse", 1, "parse_json",
                ImmutableList.of(Expr.class));

        // json_extract -> json_query
        registerFunctionTransformer("json_extract", 2, "json_query",
                ImmutableList.of(Expr.class, Expr.class));

        // json_size -> json_length
        registerFunctionTransformer("json_size", 2, "json_length",
                ImmutableList.of(Expr.class, Expr.class));
    }

    private static void registerBitwiseFunctionTransformer() {
        // bitwise_and -> bitand
        registerFunctionTransformer("bitwise_and", 2, "bitand", ImmutableList.of(Expr.class, Expr.class));

        // bitwise_not -> bitnot
        registerFunctionTransformer("bitwise_not", 1, "bitnot", ImmutableList.of(Expr.class));

        // bitwise_or -> bitor
        registerFunctionTransformer("bitwise_or", 2, "bitor", ImmutableList.of(Expr.class, Expr.class));

        // bitwise_xor -> bitxor
        registerFunctionTransformer("bitwise_xor", 2, "bitxor", ImmutableList.of(Expr.class, Expr.class));

        // bitwise_left_shift -> bit_shift_left
        registerFunctionTransformer("bitwise_left_shift", 2, "bit_shift_left", ImmutableList.of(Expr.class, Expr.class));

        // bitwise_right_shift -> bit_shift_right
        registerFunctionTransformer("bitwise_right_shift", 2, "bit_shift_right", ImmutableList.of(Expr.class, Expr.class));
    }

    private static void registerUnicodeFunctionTransformer() {
        // to_utf8 -> to_binary
        registerFunctionTransformer("to_utf8", 1, new FunctionCallExpr("to_binary",
                ImmutableList.of(new PlaceholderExpr(1, Expr.class), new StringLiteral("utf8"))));

        // from_utf8 -> from_binary
        registerFunctionTransformer("from_utf8", 1, new FunctionCallExpr("from_binary",
                ImmutableList.of(new PlaceholderExpr(1, Expr.class), new StringLiteral("utf8"))));
    }

    private static void registerMapFunctionTransformer() {
        // map(array, array) -> map_from_arrays
        registerFunctionTransformer("map", 2, "map_from_arrays",
                ImmutableList.of(Expr.class, Expr.class));
    }

    private static void registerBinaryFunctionTransformer() {
        // to_hex -> hex
        registerFunctionTransformer("to_hex", 1, "hex", ImmutableList.of(Expr.class));
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
            // For a FunctionCallExpr, do not know the actual arguments here, so we use a PlaceholderExpr to replace it.
            arguments.add(new PlaceholderExpr(index + 1, starRocksArgumentsClass.get(index)));
        }
        return new FunctionCallExpr(starRocksFnName, arguments);
    }
}
