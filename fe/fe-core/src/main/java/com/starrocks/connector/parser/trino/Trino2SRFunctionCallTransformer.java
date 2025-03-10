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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CaseWhenClause;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.VariableExpr;

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
        registerURLFunctionTransformer();
        registerBitwiseFunctionTransformer();
        registerUnicodeFunctionTransformer();
        registerMapFunctionTransformer();
        registerBinaryFunctionTransformer();
        registerHLLFunctionTransformer();
        registerMathFunctionTransformer();
        // todo: support more function transform
    }

    private static void registerAggregateFunctionTransformer() {
        // 1.approx_distinct
        registerFunctionTransformer("approx_distinct", 1,
                "approx_count_distinct", List.of(Expr.class));

        // 2. arbitrary
        registerFunctionTransformer("arbitrary", 1,
                "any_value", List.of(Expr.class));

        // 3. approx_percentile
        registerFunctionTransformer("approx_percentile", 2,
                "percentile_approx", List.of(Expr.class, Expr.class));

        // 4. stddev
        registerFunctionTransformer("stddev", 1,
                "stddev_samp", List.of(Expr.class));

        // 5. stddev_pop
        registerFunctionTransformer("stddev_pop", 1,
                "stddev", List.of(Expr.class));

        // 6. variance
        registerFunctionTransformer("variance", 1,
                "var_samp", List.of(Expr.class));

        // 7. var_pop
        registerFunctionTransformer("var_pop", 1,
                "variance", List.of(Expr.class));

        // 8. count_if(x) -> count(case when x then 1 end)
        registerFunctionTransformer("count_if", 1, new FunctionCallExpr("count",
                List.of(new CaseExpr(null, List.of(new CaseWhenClause(
                        new PlaceholderExpr(1, Expr.class), new IntLiteral(1L))), null))));
    }

    private static void registerArrayFunctionTransformer() {
        // array_union -> array_distinct(array_concat(x, y))
        registerFunctionTransformer("array_union", 2, new FunctionCallExpr("array_distinct",
                List.of(new FunctionCallExpr("array_concat", List.of(
                        new PlaceholderExpr(1, Expr.class), new PlaceholderExpr(2, Expr.class)
                )))));
        // contains -> array_contains
        registerFunctionTransformer("contains", 2, "array_contains",
                List.of(Expr.class, Expr.class));
        // slice -> array_slice
        registerFunctionTransformer("slice", 3, "array_slice",
                List.of(Expr.class, Expr.class, Expr.class));
        // filter(array, lambda) -> array_filter(array, lambda)
        registerFunctionTransformer("filter", 2, "array_filter",
                List.of(Expr.class, Expr.class));
        // contains_sequence -> array_contains_seq
        registerFunctionTransformer("contains_sequence", 2, "array_contains_seq",
                List.of(Expr.class, Expr.class));
    }

    private static void registerDateFunctionTransformer() {
        // to_unixtime -> unix_timestamp
        registerFunctionTransformer("to_unixtime", 1, "unix_timestamp",
                List.of(Expr.class));

        // from_unixtime(unixtime) -> from_unixtime
        registerFunctionTransformer("from_unixtime", 1, "from_unixtime",
                List.of(Expr.class));

        //from_unixtime(unixtime, zone) -> convert_tz, from_unixtime
        registerFunctionTransformer("from_unixtime", 2,
                new FunctionCallExpr("convert_tz", List.of(
                        new FunctionCallExpr("from_unixtime", List.of(
                                new PlaceholderExpr(1, Expr.class))),
                        new VariableExpr("time_zone"),
                        new PlaceholderExpr(2, Expr.class)
                )));

        //from_unixtime(unixtime, hours, minutes) -> hours_add, minutes_add, from_unixtime
        registerFunctionTransformer("from_unixtime", 3,
                new FunctionCallExpr("hours_add", List.of(
                        new FunctionCallExpr("minutes_add", List.of(
                                new FunctionCallExpr("from_unixtime", List.of(
                                        new PlaceholderExpr(1, Expr.class))),
                                new PlaceholderExpr(3, Expr.class))),
                        new PlaceholderExpr(2, Expr.class)

                )));

        //at_timezone -> convert_tz
        registerFunctionTransformer("at_timezone", 2,
                new FunctionCallExpr("convert_tz", List.of(
                        new PlaceholderExpr(1, Expr.class),
                        new VariableExpr("time_zone"),
                        new PlaceholderExpr(2, Expr.class)
                )));

        // date_parse -> str_to_date
        registerFunctionTransformer("date_parse", 2, "str_to_date",
                List.of(Expr.class, Expr.class));

        // day_of_week -> dayofweek
        registerFunctionTransformer("day_of_week", 1, "dayofweek_iso",
                List.of(Expr.class));

        // dow -> dayofweek
        registerFunctionTransformer("dow", 1, "dayofweek_iso",
                List.of(Expr.class));

        // day_of_month -> dayofmonth
        registerFunctionTransformer("day_of_month", 1, "dayofmonth",
                List.of(Expr.class));

        // day_of_year -> dayofyear
        registerFunctionTransformer("day_of_year", 1, "dayofyear",
                List.of(Expr.class));

        // doy -> dayofyear
        registerFunctionTransformer("doy", 1, "dayofyear",
                List.of(Expr.class));

        // week_of_year -> week_iso
        registerFunctionTransformer("week_of_year", 1, "week_iso",
                List.of(Expr.class));

        // week -> week_iso
        registerFunctionTransformer("week", 1, "week_iso",
                List.of(Expr.class));

        // format_datetime -> jodatime_format
        registerFunctionTransformer("format_datetime", 2, "jodatime_format",
                List.of(Expr.class, Expr.class));

        // to_char -> jodatime_format
        registerFunctionTransformer("to_char", 2, "jodatime_format",
                List.of(Expr.class, Expr.class));

        // to_date -> to_tera_date
        registerFunctionTransformer("to_date", 2, "to_tera_date",
                List.of(Expr.class, Expr.class));

        // to_timestamp -> to_tera_timestamp
        registerFunctionTransformer("to_timestamp", 2, "to_tera_timestamp",
                List.of(Expr.class, Expr.class));

        // last_day_of_month(x)  -> last_day(x,'month')
        registerFunctionTransformer("last_day_of_month", 1, new FunctionCallExpr("last_day",
                List.of(new PlaceholderExpr(1, Expr.class), new StringLiteral("month"))));

        // year_of_week(x) -> floor(divide(yearweek('x', 1),100))
        registerFunctionTransformer("year_of_week", 1,
                new FunctionCallExpr("floor", List.of(
                        new FunctionCallExpr("divide", List.of(
                                new FunctionCallExpr("yearweek", List.of(
                                        new PlaceholderExpr(1, Expr.class),
                                        new IntLiteral(1))
                                ),
                                new IntLiteral(100))
                        )
                )));

        // yow(x) -> floor(divide(yearweek('x', 1),100))
        registerFunctionTransformer("yow", 1,
                new FunctionCallExpr("floor", List.of(
                        new FunctionCallExpr("divide", List.of(
                                new FunctionCallExpr("yearweek", List.of(
                                        new PlaceholderExpr(1, Expr.class),
                                        new IntLiteral(1))
                                ),
                                new IntLiteral(100))
                        )
                )));

        // from_iso8601_timestamp -> timestamp
        registerFunctionTransformer("from_iso8601_timestamp", 1, "timestamp",
                List.of(Expr.class));
    }

    private static void registerStringFunctionTransformer() {
        // chr -> char
        registerFunctionTransformer("chr", 1, "char", List.of(Expr.class));

        // codepoint -> ascii
        registerFunctionTransformer("codepoint", 1, "ascii", List.of(Expr.class));

        // strpos -> locate
        registerFunctionTransformer("strpos", 2, new FunctionCallExpr("locate",
                List.of(new PlaceholderExpr(2, Expr.class), new PlaceholderExpr(1, Expr.class))));

        // length -> char_length
        registerFunctionTransformer("length", 1, "char_length", List.of(Expr.class));

        // str_to_map(str, del1, del2) -> str_to_map(split(str, del1), del2)
        registerFunctionTransformer("str_to_map", 3, new FunctionCallExpr("str_to_map",
                List.of(new FunctionCallExpr("split", List.of(
                        new PlaceholderExpr(1, Expr.class), new PlaceholderExpr(2, Expr.class)
                )), new PlaceholderExpr(3, Expr.class))));

        // str_to_map(str) -> str_to_map(split(str, del1), del2)
        registerFunctionTransformer("str_to_map", 2, new FunctionCallExpr("str_to_map",
                List.of(new FunctionCallExpr("split", List.of(
                        new PlaceholderExpr(1, Expr.class), new PlaceholderExpr(2, Expr.class)
                )), new StringLiteral(":"))));

        // str_to_map(str) -> str_to_map(split(str, del1), del2)
        registerFunctionTransformer("str_to_map", 1, new FunctionCallExpr("str_to_map",
                List.of(new FunctionCallExpr("split", List.of(
                        new PlaceholderExpr(1, Expr.class), new StringLiteral(","))), new StringLiteral(":"))));

        // replace(string, search) -> replace(string, search, '')
        registerFunctionTransformer("replace", 2, new FunctionCallExpr("replace",
                List.of(new PlaceholderExpr(1, Expr.class), new PlaceholderExpr(2, Expr.class),
                        new StringLiteral(""))));

        registerFunctionTransformer("index", 2, "instr",
                List.of(Expr.class, Expr.class));
    }

    private static void registerRegexpFunctionTransformer() {
        // regexp_like -> regexp
        registerFunctionTransformer("regexp_like", 2, "regexp",
                List.of(Expr.class, Expr.class));
    }

    private static void registerURLFunctionTransformer() {
        // url_extract_path('https://www.starrocks.io/showcase') -> parse_url('https://www.starrocks.io/showcase', 'PATH')
        registerFunctionTransformer("url_extract_path", 1, new FunctionCallExpr("parse_url",
                List.of(new PlaceholderExpr(1, Expr.class), new StringLiteral("PATH"))));
    }

    private static void registerJsonFunctionTransformer() {
        // json_array_length -> json_length
        registerFunctionTransformer("json_array_length", 1, "json_length",
                List.of(Expr.class));

        // json_parse -> parse_json
        registerFunctionTransformer("json_parse", 1, "parse_json",
                List.of(Expr.class));

        // json_extract -> get_json_string
        registerFunctionTransformer("json_extract", 2, "get_json_string",
                List.of(Expr.class, Expr.class));

        // json_size -> json_length
        registerFunctionTransformer("json_size", 2, "json_length",
                List.of(Expr.class, Expr.class));
    }

    private static void registerBitwiseFunctionTransformer() {
        // bitwise_and -> bitand
        registerFunctionTransformer("bitwise_and", 2, "bitand", List.of(Expr.class, Expr.class));

        // bitwise_not -> bitnot
        registerFunctionTransformer("bitwise_not", 1, "bitnot", List.of(Expr.class));

        // bitwise_or -> bitor
        registerFunctionTransformer("bitwise_or", 2, "bitor", List.of(Expr.class, Expr.class));

        // bitwise_xor -> bitxor
        registerFunctionTransformer("bitwise_xor", 2, "bitxor", List.of(Expr.class, Expr.class));

        // bitwise_left_shift -> bit_shift_left
        registerFunctionTransformer("bitwise_left_shift", 2, "bit_shift_left", List.of(Expr.class, Expr.class));

        // bitwise_right_shift -> bit_shift_right
        registerFunctionTransformer("bitwise_right_shift", 2, "bit_shift_right", List.of(Expr.class, Expr.class));
    }

    private static void registerUnicodeFunctionTransformer() {
        // to_utf8 -> to_binary
        registerFunctionTransformer("to_utf8", 1, new FunctionCallExpr("to_binary",
                List.of(new PlaceholderExpr(1, Expr.class), new StringLiteral("utf8"))));

        // from_utf8 -> from_binary
        registerFunctionTransformer("from_utf8", 1, new FunctionCallExpr("from_binary",
                List.of(new PlaceholderExpr(1, Expr.class), new StringLiteral("utf8"))));
    }

    private static void registerMapFunctionTransformer() {
        // map(array, array) -> map_from_arrays
        registerFunctionTransformer("map", 2, "map_from_arrays",
                List.of(Expr.class, Expr.class));
    }

    private static void registerBinaryFunctionTransformer() {
        // to_hex -> hex
        registerFunctionTransformer("to_hex", 1, "hex", List.of(Expr.class));

        // from_hex -> hex_decode_binary
        registerFunctionTransformer("from_hex", 1, "hex_decode_binary", List.of(Expr.class));
    }

    private static void registerHLLFunctionTransformer() {
        // approx_set -> HLL_HASH
        registerFunctionTransformer("approx_set", 1, "hll_hash", List.of(Expr.class));

        // empty_approx_set -> HLL_EMPTY
        registerFunctionTransformer("empty_approx_set", "hll_empty");

        // merge -> HLL_RAW_AGG
        registerFunctionTransformer("merge", 1, "hll_raw_agg", List.of(Expr.class));
    }

    private static void registerMathFunctionTransformer() {
        // truncate(x) -> truncate(x, 0)
        registerFunctionTransformer("truncate", 1, new FunctionCallExpr("truncate",
                List.of(new PlaceholderExpr(1, Expr.class), new IntLiteral(0))));

    }

    private static void registerFunctionTransformer(String trinoFnName, int trinoFnArgNums, String starRocksFnName,
                                                    List<Class<? extends Expr>> starRocksArgumentsClass) {
        FunctionCallExpr starRocksFunctionCall = buildStarRocksFunctionCall(starRocksFnName, starRocksArgumentsClass);
        registerFunctionTransformer(trinoFnName, trinoFnArgNums, starRocksFunctionCall);
    }

    private static void registerFunctionTransformer(String trinoFnName, String starRocksFnName) {
        FunctionCallExpr starRocksFunctionCall = buildStarRocksFunctionCall(starRocksFnName, Lists.newArrayList());
        registerFunctionTransformer(trinoFnName, 0, starRocksFunctionCall);
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
