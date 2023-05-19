// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/FunctionSet.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.ArithmeticExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.builtins.VectorizedBuiltinFunctions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FunctionSet {
    // For most build-in functions, it will return NullLiteral when params contain NullLiteral.
    // But a few functions need to handle NullLiteral differently, such as "if". It need to add
    // an attribute to LiteralExpr to mark null and check the attribute to decide whether to
    // replace the result with NullLiteral when function finished. It leaves to be realized.
    // Functions in this set is defined in `gensrc/script/starrocks_builtins_functions.py`,
    // and will be built automatically.

    // Aggregate Functions:
    public static final String COUNT = "count";
    public static final String MAX = "max";
    public static final String MIN = "min";
    public static final String SUM = "sum";
    public static final String SUM_DISTINCT = "sum_distinct";
    public static final String AVG = "avg";
    public static final String MONEY_FORMAT = "money_format";
    public static final String STR_TO_DATE = "str_to_date";
    public static final String HLL_UNION = "hll_union";
    public static final String HLL_UNION_AGG = "hll_union_agg";
    public static final String HLL_RAW_AGG = "hll_raw_agg";
    public static final String NDV = "ndv";
    public static final String APPROX_COUNT_DISTINCT = "approx_count_distinct";
    public static final String PERCENTILE_APPROX = "percentile_approx";
    public static final String PERCENTILE_APPROX_RAW = "percentile_approx_raw";
    public static final String PERCENTILE_UNION = "percentile_union";
    public static final String PERCENTILE_CONT = "percentile_cont";
    public static final String BITMAP_UNION = "bitmap_union";
    public static final String BITMAP_AGG = "bitmap_agg";
    public static final String BITMAP_UNION_COUNT = "bitmap_union_count";
    public static final String BITMAP_UNION_INT = "bitmap_union_int";
    public static final String INTERSECT_COUNT = "intersect_count";
    public static final String BITMAP_INTERSECT = "bitmap_intersect";
    public static final String MULTI_DISTINCT_COUNT = "multi_distinct_count";
    public static final String MULTI_DISTINCT_SUM = "multi_distinct_sum";
    public static final String DICT_MERGE = "dict_merge";
    public static final String ANY_VALUE = "any_value";
    public static final String RETENTION = "retention";
    public static final String GROUP_CONCAT = "group_concat";
    public static final String ARRAY_AGG = "array_agg";
    public static final String ARRAYS_OVERLAP = "arrays_overlap";

    // Window functions:
    public static final String LEAD = "lead";
    public static final String LAG = "lag";
    public static final String FIRST_VALUE = "first_value";
    public static final String FIRST_VALUE_REWRITE = "first_value_rewrite";
    public static final String LAST_VALUE = "last_value";
    public static final String DENSE_RANK = "dense_rank";
    public static final String RANK = "rank";
    public static final String ROW_NUMBER = "row_number";

    // Scalar functions:
    public static final String BITMAP_COUNT = "bitmap_count";
    public static final String HLL_HASH = "hll_hash";
    public static final String HLL_CARDINALITY = "hll_cardinality";
    public static final String PERCENTILE_HASH = "percentile_hash";
    public static final String TO_BITMAP = "to_bitmap";
    public static final String NULL_OR_EMPTY = "null_or_empty";
    public static final String IF = "if";
    public static final String IF_NULL = "ifnull";
    public static final String MD5_SUM = "md5sum";

    // Arithmetic functions:
    public static final String ADD = "add";
    public static final String SUBTRACT = "subtract";
    public static final String MULTIPLY = "multiply";
    public static final String DIVIDE = "divide";

    // date functions
    public static final String YEAR = "year";
    public static final String YEARS_ADD = "years_add";
    public static final String YEARS_SUB = "years_sub";
    public static final String MONTH = "month";
    public static final String MONTHS_ADD = "months_add";
    public static final String MONTHS_SUB = "months_sub";
    public static final String DAY = "day";
    public static final String DAYS_ADD = "days_add";
    public static final String DAYS_SUB = "days_sub";
    public static final String ADDDATE = "adddate";
    public static final String SUBDATE = "subdate";
    public static final String DATE_ADD = "date_add";
    public static final String DATE_SUB = "date_sub";
    public static final String HOUR = "hour";
    public static final String MINUTE = "minute";
    public static final String SECOND = "second";
    public static final String CURDATE = "curdate";
    public static final String CURRENT_TIMESTAMP = "current_timestamp";
    public static final String CURRENT_TIME = "current_time";
    public static final String NOW = "now";
    public static final String UNIX_TIMESTAMP = "unix_timestamp";
    public static final String UTC_TIMESTAMP = "utc_timestamp";
    public static final String DATE_TRUNC = "date_trunc";

    // stddev and variance
    public static final String STDDEV = "stddev";
    public static final String STDDEV_POP = "stddev_pop";
    public static final String STDDEV_SAMP = "stddev_samp";
    public static final String VARIANCE = "variance";
    public static final String VAR_POP = "var_pop";
    public static final String VARIANCE_POP = "variance_pop";
    public static final String VAR_SAMP = "var_samp";
    public static final String VARIANCE_SAMP = "variance_samp";
    public static final String STD = "std";
    public static final String STDDEV_VAL = "stddev_val";

    // string functions
    public static final String SUBSTRING = "substring";
    public static final String STARTS_WITH = "starts_with";
    public static final String SUBSTITUTE = "substitute";

    // geo functions
    public static final String ST_ASTEXT = "st_astext";
    public static final String GEO_FUNCTION_PREFIX = "st_";

    // JSON functions
    public static final String JSON_QUERY = "json_query";
    public static final String PARSE_JSON = "parse_json";
    public static final Function JSON_QUERY_FUNC = new Function(
            new FunctionName(JSON_QUERY), new Type[] {Type.JSON, Type.VARCHAR}, Type.JSON, false);
    public static final String GET_JSON_INT = "get_json_int";
    public static final String GET_JSON_DOUBLE = "get_json_double";
    public static final String GET_JSON_STRING = "get_json_string";

    // Array functions
    public static final String ARRAY_DIFFERENCE = "array_difference";

    private static final Logger LOG = LogManager.getLogger(FunctionSet.class);

    private static final Map<Type, Type> MULTI_DISTINCT_SUM_RETURN_TYPE =
            ImmutableMap.<Type, Type>builder()
                    .put(Type.BOOLEAN, Type.BIGINT)
                    .put(Type.TINYINT, Type.BIGINT)
                    .put(Type.SMALLINT, Type.BIGINT)
                    .put(Type.INT, Type.BIGINT)
                    .put(Type.BIGINT, Type.BIGINT)
                    .put(Type.FLOAT, Type.DOUBLE)
                    .put(Type.DOUBLE, Type.DOUBLE)
                    .put(Type.LARGEINT, Type.LARGEINT)
                    .put(Type.DECIMALV2, Type.DECIMALV2)
                    .put(Type.DECIMAL32, Type.DECIMAL128)
                    .put(Type.DECIMAL64, Type.DECIMAL128)
                    .put(Type.DECIMAL128, Type.DECIMAL128)
                    .build();

    private static final Set<Type> STDDEV_ARG_TYPE =
            ImmutableSet.<Type>builder()
                    .add(Type.TINYINT)
                    .add(Type.SMALLINT)
                    .add(Type.INT)
                    .add(Type.DECIMAL32)
                    .add(Type.DECIMAL64)
                    .add(Type.DECIMAL128)
                    .add(Type.BIGINT)
                    .add(Type.FLOAT)
                    .add(Type.DOUBLE)
                    .build();
    /**
     * Use for vectorized engine, but we can't use vectorized function directly, because we
     * need to check whether the expression tree can use vectorized function from bottom to
     * top, we must to re-analyze function_call_expr when vectorized function is can't used
     * if we choose to use the vectorized function here. So... we need bind vectorized function
     * to row function when init.
     */
    private final Map<String, List<Function>> vectorizedFunctions;

    // This contains the nullable functions, which cannot return NULL result directly for the NULL parameter.
    // This does not contain any user defined functions. All UDFs handle null values by themselves.
    private final ImmutableSet<String> notAlwaysNullResultWithNullParamFunctions = ImmutableSet.of("if",
            "concat_ws", "ifnull", "nullif", "null_or_empty", "coalesce", "bitmap_hash", "percentile_hash", "hll_hash",
            "json_array", "json_object");

    // If low cardinality string column with global dict, for some string functions,
    // we could evaluate the function only with the dict content, not all string column data.
    public final ImmutableSet<String> couldApplyDictOptimizationFunctions = ImmutableSet.of(
            "append_trailing_char_if_absent",
            "concat",
            "concat_ws",
            "hex",
            "left",
            "like",
            "lower",
            "lpad",
            "ltrim",
            "regexp_extract",
            "regexp_replace",
            "repeat",
            "reverse",
            "right",
            "rpad",
            "rtrim",
            "split_part",
            "substr",
            "substring",
            "trim",
            "upper",
            "if");

    public static final Set<String> alwaysReturnNonNullableFunctions =
            ImmutableSet.<String>builder()
                    .add(FunctionSet.COUNT)
                    .add(FunctionSet.MULTI_DISTINCT_COUNT)
                    .add(FunctionSet.NULL_OR_EMPTY)
                    .add(FunctionSet.HLL_HASH)
                    .add(FunctionSet.HLL_UNION_AGG)
                    .add(FunctionSet.NDV)
                    .add(FunctionSet.APPROX_COUNT_DISTINCT)
                    .add(FunctionSet.BITMAP_UNION_INT)
                    .add(FunctionSet.BITMAP_UNION_COUNT)
                    .add(FunctionSet.BITMAP_COUNT)
                    .add(FunctionSet.CURDATE)
                    .add(FunctionSet.CURRENT_TIMESTAMP)
                    .add(FunctionSet.CURRENT_TIME)
                    .add(FunctionSet.NOW)
                    .add(FunctionSet.UTC_TIMESTAMP)
                    .add(FunctionSet.MD5_SUM)
                    .build();

    public static final Set<String> decimalRoundFunctions =
            ImmutableSet.<String>builder()
                    .add("truncate")
                    .add("round")
                    .add("round_up_to")
                    .build();

    public static final Set<String> nonDeterministicFunctions =
            ImmutableSet.<String>builder()
                    .add("rand")
                    .add("random")
                    .add("uuid")
                    .add("sleep")
                    .build();

    public static final Set<String> varianceFunctions = ImmutableSet.<String>builder()
            .add(FunctionSet.VAR_POP)
            .add(FunctionSet.VAR_SAMP)
            .add(FunctionSet.VARIANCE)
            .add(FunctionSet.VARIANCE_POP)
            .add(FunctionSet.VARIANCE_SAMP)
            .add(FunctionSet.STD)
            .add(FunctionSet.STDDEV)
            .add(FunctionSet.STDDEV_POP)
            .add(FunctionSet.STDDEV_SAMP)
            .add(FunctionSet.STDDEV_VAL).build();

    public FunctionSet() {
        vectorizedFunctions = Maps.newHashMap();
    }

    /**
     * There are essential differences in the implementation of some functions for different
     * types params, which should be prohibited.
     *
     * @param desc
     * @param candicate
     * @return
     */
    public static boolean isCastMatchAllowed(Function desc, Function candicate) {
        final String functionName = desc.getFunctionName().getFunction();
        final Type[] descArgTypes = desc.getArgs();
        final Type[] candicateArgTypes = candicate.getArgs();
        if (functionName.equalsIgnoreCase("hex")
                || functionName.equalsIgnoreCase("lead")
                || functionName.equalsIgnoreCase("lag")) {
            final ScalarType descArgType = (ScalarType) descArgTypes[0];
            final ScalarType candicateArgType = (ScalarType) candicateArgTypes[0];
            // Bitmap, HLL, PERCENTILE type don't allow cast
            if (descArgType.isOnlyMetricType()) {
                return false;
            }
            if (functionName.equalsIgnoreCase("lead") ||
                    functionName.equalsIgnoreCase("lag")) {
                // lead and lag function respect first arg type
                return descArgType.isNull() || descArgType.matchesType(candicateArgType);
            } else {
                // The implementations of hex for string and int are different.
                return descArgType.isStringType() || !candicateArgType.isStringType();
            }
        }

        // ifnull, nullif(DATE, DATETIME) should return datetime, not bigint
        // if(boolean, DATE, DATETIME) should return datetime
        int arg_index = 0;
        if (functionName.equalsIgnoreCase("ifnull") ||
                functionName.equalsIgnoreCase("nullif") ||
                functionName.equalsIgnoreCase("if")) {
            if (functionName.equalsIgnoreCase("if")) {
                arg_index = 1;
            }
            boolean descIsAllDateType = true;
            for (int i = arg_index; i < descArgTypes.length; ++i) {
                if (!descArgTypes[i].isDateType()) {
                    descIsAllDateType = false;
                    break;
                }
            }
            Type candicateArgType = candicateArgTypes[arg_index];
            if (descIsAllDateType && !candicateArgType.isDateType()) {
                return false;
            }
        }
        return true;
    }

    public void init() {
        ArithmeticExpr.initBuiltins(this);
        TableFunction.initBuiltins(this);
        VectorizedBuiltinFunctions.initBuiltins(this);
        initAggregateBuiltins();
    }

    public boolean isNotAlwaysNullResultWithNullParamFunctions(String funcName) {
        return notAlwaysNullResultWithNullParamFunctions.contains(funcName)
                || alwaysReturnNonNullableFunctions.contains(funcName);
    }

    public Function getFunction(Function desc, Function.CompareMode mode) {
        List<Function> fns = vectorizedFunctions.get(desc.functionName());
        if (fns == null) {
            return null;
        }

        // First check for identical
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_IDENTICAL)) {
                return checkPolymorphicFunction(f, desc.getArgs());
            }
        }
        if (mode == Function.CompareMode.IS_IDENTICAL) {
            return null;
        }

        // Next check for indistinguishable
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_INDISTINGUISHABLE)) {
                return checkPolymorphicFunction(f, desc.getArgs());
            }
        }
        if (mode == Function.CompareMode.IS_INDISTINGUISHABLE) {
            return null;
        }

        // Next check for strict supertypes
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_SUPERTYPE_OF) && isCastMatchAllowed(desc, f)) {
                return checkPolymorphicFunction(f, desc.getArgs());
            }
        }
        if (mode == Function.CompareMode.IS_SUPERTYPE_OF) {
            return null;
        }

        // Finally check for non-strict supertypes
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF) && isCastMatchAllowed(desc, f)) {
                return checkPolymorphicFunction(f, desc.getArgs());
            }
        }
        return null;
    }

    private void addBuiltInFunction(Function fn) {
        Preconditions.checkArgument(!fn.getReturnType().isPseudoType() || fn.isPolymorphic(), fn.toString());
        if (getFunction(fn, Function.CompareMode.IS_INDISTINGUISHABLE) != null) {
            return;
        }
        fn.setIsNullable(!alwaysReturnNonNullableFunctions.contains(fn.functionName()));
        List<Function> fns = vectorizedFunctions.computeIfAbsent(fn.functionName(), k -> Lists.newArrayList());
        fns.add(fn);
    }

    // for vectorized engine
    public void addVectorizedScalarBuiltin(long fid, String fnName, boolean varArgs,
                                           Type retType, Type... args) {

        List<Type> argsType = Arrays.stream(args).collect(Collectors.toList());
        addVectorizedBuiltin(ScalarFunction.createVectorizedBuiltin(fid, fnName, argsType, varArgs, retType));
    }

    private void addVectorizedBuiltin(Function fn) {
        if (findVectorizedFunction(fn) != null) {
            return;
        }
        fn.setCouldApplyDictOptimize(couldApplyDictOptimizationFunctions.contains(fn.functionName()));
        fn.setIsNullable(!alwaysReturnNonNullableFunctions.contains(fn.functionName()));
        List<Function> fns = vectorizedFunctions.computeIfAbsent(fn.functionName(), k -> Lists.newArrayList());
        fns.add(fn);
    }

    private Function findVectorizedFunction(Function desc) {
        List<Function> fns = vectorizedFunctions.get(desc.functionName());

        if (fns == null) {
            return null;
        }

        // First check for identical
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_IDENTICAL)) {
                return f;
            }
        }

        // Next check for indistinguishable
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_INDISTINGUISHABLE)) {
                return f;
            }
        }
        return null;

    }

    /**
     * Adds a builtin to this database. The function must not already exist.
     */
    public void addBuiltin(Function fn) {
        addBuiltInFunction(fn);
    }

    // Populate all the aggregate builtins in the catalog.
    // null symbols indicate the function does not need that step of the evaluation.
    // An empty symbol indicates a TODO for the BE to implement the function.
    private void initAggregateBuiltins() {
        // count(*)
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.COUNT,
                new ArrayList<>(), Type.BIGINT, Type.BIGINT, false, true, true));

        for (Type t : Type.getSupportedTypes()) {
            if (t.isNull()) {
                continue; // NULL is handled through type promotion.
            }
            if (t.isChar()) {
                continue; // promoted to STRING
            }
            // Count
            addBuiltin(AggregateFunction.createBuiltin(FunctionSet.COUNT,
                    Lists.newArrayList(t), Type.BIGINT, Type.BIGINT, false, true, true));

            if (t.isPseudoType()) {
                continue; // Only function `Count` support pseudo types now.
            }

            // count in multi distinct
            if (t.isChar() || t.isVarchar()) {
                addBuiltin(AggregateFunction.createBuiltin(FunctionSet.MULTI_DISTINCT_COUNT, Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.VARCHAR,
                        false, true, true));
            } else if (t.isBoolean() || t.isTinyint() || t.isSmallint() || t.isInt() || t.isBigint() ||
                    t.isLargeint() || t.isFloat() || t.isDouble()) {
                addBuiltin(AggregateFunction.createBuiltin(FunctionSet.MULTI_DISTINCT_COUNT, Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.VARCHAR,
                        false, true, true));
            } else if (t.isDate() || t.isDatetime()) {
                addBuiltin(AggregateFunction.createBuiltin(FunctionSet.MULTI_DISTINCT_COUNT, Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.VARCHAR,
                        false, true, true));
            } else if (t.isDecimalV2() || t.isDecimalV3()) {
                addBuiltin(AggregateFunction.createBuiltin(FunctionSet.MULTI_DISTINCT_COUNT, Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.VARCHAR,
                        false, true, true));
            }

            // sum in multi distinct
            if (t.isBoolean() || t.isTinyint() || t.isSmallint() || t.isInt() || t.isFloat()) {
                addBuiltin(AggregateFunction.createBuiltin(FunctionSet.MULTI_DISTINCT_SUM, Lists.newArrayList(t),
                        MULTI_DISTINCT_SUM_RETURN_TYPE.get(t),
                        Type.VARCHAR,
                        false, true, true));
            } else if (t.isBigint() || t.isLargeint() || t.isDouble()) {
                addBuiltin(AggregateFunction.createBuiltin(FunctionSet.MULTI_DISTINCT_SUM, Lists.newArrayList(t),
                        t,
                        Type.VARCHAR,
                        false, true, true));
            } else if (t.isDecimalV2() || t.isDecimalV3()) {
                addBuiltin(AggregateFunction.createBuiltin(FunctionSet.MULTI_DISTINCT_SUM, Lists.newArrayList(t),
                        MULTI_DISTINCT_SUM_RETURN_TYPE.get(t),
                        Type.VARCHAR,
                        false, true, true));
            }
            // Min
            addBuiltin(AggregateFunction.createBuiltin("min",
                    Lists.newArrayList(t), t, t, true, true, false));

            // Max
            addBuiltin(AggregateFunction.createBuiltin("max",
                    Lists.newArrayList(t), t, t, true, true, false));

            // NDV
            // ndv return string
            addBuiltin(AggregateFunction.createBuiltin("ndv",
                    Lists.newArrayList(t), Type.BIGINT, Type.VARCHAR,
                    true, false, true));

            // ANY_VALUE
            addBuiltin(AggregateFunction.createBuiltin("any_value",
                    Lists.newArrayList(t), t, t, true, false, false));

            //APPROX_COUNT_DISTINCT
            //alias of ndv, compute approx count distinct use HyperLogLog
            addBuiltin(AggregateFunction.createBuiltin("approx_count_distinct",
                    Lists.newArrayList(t), Type.BIGINT, Type.VARCHAR,
                    true, false, true));

            // BITMAP_UNION_INT
            addBuiltin(AggregateFunction.createBuiltin(BITMAP_UNION_INT,
                    Lists.newArrayList(t), Type.BIGINT, Type.BITMAP,
                    true, false, true));

            // INTERSECT_COUNT
            addBuiltin(AggregateFunction.createBuiltin(INTERSECT_COUNT,
                    Lists.newArrayList(Type.BITMAP, t, t), Type.BIGINT, Type.VARCHAR, true,
                    true, false, true));

            if (STDDEV_ARG_TYPE.contains(t)) {
                addBuiltin(AggregateFunction.createBuiltin("stddev",
                        Lists.newArrayList(t), Type.DOUBLE, Type.VARCHAR,
                        false, true, false));
                addBuiltin(AggregateFunction.createBuiltin("stddev_samp",
                        Lists.newArrayList(t), Type.DOUBLE, Type.VARCHAR,
                        false, true, false));
                addBuiltin(AggregateFunction.createBuiltin("stddev_pop",
                        Lists.newArrayList(t), Type.DOUBLE, Type.VARCHAR,
                        false, true, false));
                addBuiltin(AggregateFunction.createBuiltin("std",
                        Lists.newArrayList(t), Type.DOUBLE, Type.VARCHAR,
                        false, true, false));
                addBuiltin(AggregateFunction.createBuiltin("variance",
                        Lists.newArrayList(t), Type.DOUBLE, Type.VARCHAR,
                        false, true, false));
                addBuiltin(AggregateFunction.createBuiltin("variance_samp",
                        Lists.newArrayList(t), Type.DOUBLE, Type.VARCHAR,
                        false, true, false));
                addBuiltin(AggregateFunction.createBuiltin("var_samp",
                        Lists.newArrayList(t), Type.DOUBLE, Type.VARCHAR,
                        false, true, false));
                addBuiltin(AggregateFunction.createBuiltin("variance_pop",
                        Lists.newArrayList(t), Type.DOUBLE, Type.VARCHAR,
                        false, true, false));
                addBuiltin(AggregateFunction.createBuiltin("var_pop",
                        Lists.newArrayList(t), Type.DOUBLE, Type.VARCHAR,
                        false, true, false));
            }
        }

        // Sum
        String[] sumNames = {"sum", "sum_distinct"};
        for (String name : sumNames) {
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.BOOLEAN), Type.BIGINT, Type.BIGINT, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.TINYINT), Type.BIGINT, Type.BIGINT, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.SMALLINT), Type.BIGINT, Type.BIGINT, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.INT), Type.BIGINT, Type.BIGINT, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.DECIMAL32), Type.DECIMAL128, Type.DECIMAL128, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.BIGINT), Type.BIGINT, Type.BIGINT, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.DECIMAL64), Type.DECIMAL128, Type.DECIMAL128, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.FLOAT), Type.DOUBLE, Type.DOUBLE, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.DOUBLE), Type.DOUBLE, Type.DOUBLE, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.DECIMALV2), Type.DECIMALV2, Type.DECIMALV2, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.LARGEINT), Type.LARGEINT, Type.LARGEINT, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.DECIMAL128), Type.DECIMAL128, Type.DECIMAL128, false, true, false));
        }

        // HLL_UNION_AGG
        addBuiltin(AggregateFunction.createBuiltin("hll_union_agg",
                Lists.newArrayList(Type.HLL), Type.BIGINT, Type.HLL,
                true, true, true));

        // HLL_UNION
        addBuiltin(AggregateFunction.createBuiltin("hll_union",
                Lists.newArrayList(Type.HLL), Type.HLL, Type.HLL,
                true, false, true));

        // HLL_RAW_AGG is alias of HLL_UNION
        addBuiltin(AggregateFunction.createBuiltin("hll_raw_agg",
                Lists.newArrayList(Type.HLL), Type.HLL, Type.HLL,
                true, false, true));

        // bitmap
        addBuiltin(AggregateFunction.createBuiltin(BITMAP_UNION, Lists.newArrayList(Type.BITMAP),
                Type.BITMAP,
                Type.BITMAP,
                true, false, true));

        for (Type t : Type.getIntegerTypes()) {
            addBuiltin(AggregateFunction.createBuiltin(BITMAP_AGG, Lists.newArrayList(t),
                    Type.BITMAP, Type.BITMAP, true, false, true));
        }

        addBuiltin(AggregateFunction.createBuiltin(BITMAP_UNION_COUNT, Lists.newArrayList(Type.BITMAP),
                Type.BIGINT,
                Type.BITMAP,
                true, true, true));
        // TODO(ml): supply function symbol
        addBuiltin(AggregateFunction.createBuiltin(BITMAP_INTERSECT, Lists.newArrayList(Type.BITMAP),
                Type.BITMAP, Type.BITMAP,
                true, false, true));

        //PercentileApprox
        addBuiltin(AggregateFunction.createBuiltin("percentile_approx",
                Lists.newArrayList(Type.DOUBLE, Type.DOUBLE), Type.DOUBLE, Type.VARCHAR,
                false, false, false));
        addBuiltin(AggregateFunction.createBuiltin("percentile_approx",
                Lists.newArrayList(Type.DOUBLE, Type.DOUBLE, Type.DOUBLE), Type.DOUBLE, Type.VARCHAR,
                false, false, false));

        addBuiltin(AggregateFunction.createBuiltin("percentile_union",
                Lists.newArrayList(Type.PERCENTILE), Type.PERCENTILE, Type.PERCENTILE,
                false, false, false));

        addBuiltin(AggregateFunction.createBuiltin(RETENTION, Lists.newArrayList(Type.ARRAY_BOOLEAN),
                Type.ARRAY_BOOLEAN, Type.BIGINT, false, false, false));

        // PercentileCont
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.PERCENTILE_CONT,
                Lists.newArrayList(Type.DATE, Type.DOUBLE), Type.DATE, Type.VARCHAR,
                false, false, false));
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.PERCENTILE_CONT,
                Lists.newArrayList(Type.DATETIME, Type.DOUBLE), Type.DATETIME, Type.VARCHAR,
                false, false, false));
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.PERCENTILE_CONT,
                Lists.newArrayList(Type.DOUBLE, Type.DOUBLE), Type.DOUBLE, Type.VARCHAR,
                false, false, false));

        // Avg
        // TODO: switch to CHAR(sizeof(AvgIntermediateType) when that becomes available
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.BOOLEAN), Type.DOUBLE, Type.VARCHAR,
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.TINYINT), Type.DOUBLE, Type.VARCHAR,
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.SMALLINT), Type.DOUBLE, Type.VARCHAR,
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.INT), Type.DOUBLE, Type.VARCHAR,
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.DECIMAL32), Type.DECIMAL128, Type.VARCHAR,
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.BIGINT), Type.DOUBLE, Type.VARCHAR,
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.DECIMAL64), Type.DECIMAL128, Type.VARCHAR,
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.FLOAT), Type.DOUBLE, Type.VARCHAR,
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.DOUBLE), Type.DOUBLE, Type.VARCHAR,
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.DECIMALV2), Type.DECIMALV2, Type.VARCHAR,
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.DECIMAL128), Type.DECIMAL128, Type.VARCHAR,
                false, true, false));
        // Avg(Timestamp)
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.DATE), Type.DATE, Type.VARCHAR,
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.DATETIME), Type.DATETIME, Type.DATETIME,
                false, true, false));

        // array_agg
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.ARRAY_AGG,
                Lists.newArrayList(Type.BOOLEAN), Type.ARRAY_BOOLEAN, Type.ARRAY_BOOLEAN,
                false, false, false));
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.ARRAY_AGG,
                Lists.newArrayList(Type.TINYINT), Type.ARRAY_TINYINT, Type.ARRAY_TINYINT,
                false, false, false));
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.ARRAY_AGG,
                Lists.newArrayList(Type.SMALLINT), Type.ARRAY_SMALLINT, Type.ARRAY_SMALLINT,
                false, false, false));
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.ARRAY_AGG,
                Lists.newArrayList(Type.INT), Type.ARRAY_INT, Type.ARRAY_INT,
                false, false, false));
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.ARRAY_AGG,
                Lists.newArrayList(Type.BIGINT), Type.ARRAY_BIGINT, Type.ARRAY_BIGINT,
                false, false, false));
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.ARRAY_AGG,
                Lists.newArrayList(Type.LARGEINT), Type.ARRAY_LARGEINT, Type.ARRAY_LARGEINT,
                false, false, false));
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.ARRAY_AGG,
                Lists.newArrayList(Type.FLOAT), Type.ARRAY_FLOAT, Type.ARRAY_FLOAT,
                false, false, false));
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.ARRAY_AGG,
                Lists.newArrayList(Type.DOUBLE), Type.ARRAY_DOUBLE, Type.ARRAY_DOUBLE,
                false, false, false));
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.ARRAY_AGG,
                Lists.newArrayList(Type.VARCHAR), Type.ARRAY_VARCHAR, Type.ARRAY_VARCHAR,
                false, false, false));
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.ARRAY_AGG,
                Lists.newArrayList(Type.CHAR), Type.ARRAY_VARCHAR, Type.ARRAY_VARCHAR,
                false, false, false));
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.ARRAY_AGG,
                Lists.newArrayList(Type.DATE), Type.ARRAY_DATE, Type.ARRAY_DATE,
                false, false, false));
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.ARRAY_AGG,
                Lists.newArrayList(Type.DATETIME), Type.ARRAY_DATETIME, Type.ARRAY_DATETIME,
                false, false, false));
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.ARRAY_AGG,
                Lists.newArrayList(Type.DECIMAL32), Type.ARRAY_DECIMALV2, Type.ARRAY_DECIMALV2,
                false, false, false));
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.ARRAY_AGG,
                Lists.newArrayList(Type.TIME), Type.ARRAY_DATETIME, Type.ARRAY_DATETIME,
                false, false, false));

        // Group_concat(string)
        addBuiltin(AggregateFunction.createBuiltin("group_concat",
                Lists.newArrayList(Type.VARCHAR), Type.VARCHAR, Type.VARCHAR,
                false, false, false));
        // Group_concat(string, string)
        addBuiltin(AggregateFunction.createBuiltin("group_concat",
                Lists.newArrayList(Type.VARCHAR, Type.VARCHAR), Type.VARCHAR, Type.VARCHAR,
                false, false, false));

        // analytic functions
        // Rank
        addBuiltin(AggregateFunction.createAnalyticBuiltin("rank",
                Collections.emptyList(), Type.BIGINT, Type.VARCHAR));
        // Dense rank
        addBuiltin(AggregateFunction.createAnalyticBuiltin("dense_rank",
                Collections.emptyList(), Type.BIGINT, Type.VARCHAR));
        addBuiltin(AggregateFunction.createAnalyticBuiltin("row_number",
                Collections.emptyList(), Type.BIGINT, Type.BIGINT));

        addBuiltin(AggregateFunction.createBuiltin(DICT_MERGE, Lists.newArrayList(Type.VARCHAR),
                Type.VARCHAR, Type.VARCHAR, true, false, false));

        addBuiltin(AggregateFunction.createBuiltin(DICT_MERGE, Lists.newArrayList(Type.ARRAY_VARCHAR),
                Type.VARCHAR, Type.VARCHAR, true, false, false));

        for (Type t : Type.getSupportedTypes()) {
            if (t.isNull()) {
                continue; // NULL is handled through type promotion.
            }
            if (t.isChar()) {
                continue; // promoted to STRING
            }
            if (t.isTime()) {
                continue;
            }
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "first_value", Lists.newArrayList(t), t, t));
            // Implements FIRST_VALUE for some windows that require rewrites during planning.
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "first_value_rewrite", Lists.newArrayList(t, Type.BIGINT), t, t));

            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "last_value", Lists.newArrayList(t), t, t));

            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lag", Lists.newArrayList(t, Type.BIGINT, t), t, t));
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lead", Lists.newArrayList(t, Type.BIGINT, t), t, t));

            // lead() and lag() the default offset and the default value should be
            // rewritten to call the overrides that take all parameters.
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lag", Lists.newArrayList(t), t, t));
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lag", Lists.newArrayList(t, Type.BIGINT), t, t));
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lead", Lists.newArrayList(t), t, t));
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lead", Lists.newArrayList(t, Type.BIGINT), t, t));
        }

    }

    public List<Function> getBuiltinFunctions() {
        List<Function> builtinFunctions = Lists.newArrayList();
        for (Map.Entry<String, List<Function>> entry : vectorizedFunctions.entrySet()) {
            builtinFunctions.addAll(entry.getValue());
        }
        return builtinFunctions;
    }

    /**
     * Inspired by https://github.com/postgres/postgres/blob/master/src/backend/parser/parse_coerce.c#L1934
     * <p>
     * Make sure a polymorphic function is legally callable, and deduce actual argument and result types.
     * <p>
     * If any polymorphic pseudotype is used in a function's arguments or return type, we make sure the
     * actual data types are consistent with each other.
     * 1) If return type is ANYELEMENT, and any argument is ANYELEMENT, use the
     * argument's actual type as the function's return type.
     * 2) If return type is ANYARRAY, and any argument is ANYARRAY, use the
     * argument's actual type as the function's return type.
     * 3) Otherwise, if return type is ANYELEMENT or ANYARRAY, and there is
     * at least one ANYELEMENT, ANYARRAY input, deduce the return type from those inputs, or return null
     * if we can't.
     * </p>
     * <p>
     * Like PostgreSQL, two pseudo-types of special interest are ANY_ARRAY and ANY_ELEMENT, which are collectively
     * called polymorphic types. Any function declared using these types is said to be a polymorphic function.
     * A polymorphic function can operate on many different data types, with the specific data type(s) being
     * determined by the data types actually passed to it in a particular call.
     * <p>
     * Polymorphic arguments and results are tied to each other and are resolved to a specific data type when a
     * query calling a polymorphic function is parsed. Each position (either argument or return value) declared
     * as ANY_ELEMENT is allowed to have any specific actual data type, but in any given call they must all be
     * the same actual type. Each position declared as ANY_ARRAY can have any array data type, but similarly they
     * must all be the same type. Furthermore, if there are positions declared ANY_ARRAY and others declared
     * ANY_ELEMENT, the actual array type in the ANY_ARRAY positions must be an array whose elements are the same
     * type appearing in the ANY_ELEMENT positions.
     * <p>
     * Thus, when more than one argument position is declared with a polymorphic type, the net effect is that only
     * certain combinations of actual argument types are allowed. For example, a function declared as
     * equal(ANY_ELEMENT, ANY_ELEMENT) will take any two input values, so long as they are of the same data type.
     * <p>
     * When the return value of a function is declared as a polymorphic type, there must be at least one argument
     * position that is also polymorphic, and the actual data type supplied as the argument determines the actual
     * result type for that call. For example, if there were not already an array subscripting mechanism, one
     * could define a function that implements subscripting as subscript(ANY_ARRAY, INT) returns ANY_ELEMENT. This
     * declaration constrains the actual first argument to be an array type, and allows the parser to infer the
     * correct result type from the actual first argument's type.
     * </p>
     * TODO(zhuming): throws an exception on error, instead of return null.
     */
    private Function checkPolymorphicFunction(Function fn, Type[] paramTypes) {
        if (!fn.isPolymorphic()) {
            return fn;
        }
        Type[] declTypes = fn.getArgs();
        Type[] realTypes = Arrays.copyOf(declTypes, declTypes.length);
        ArrayType typeArray = null;
        Type typeElement = null;
        Type retType = fn.getReturnType();
        for (int i = 0; i < declTypes.length; i++) {
            Type declType = declTypes[i];
            Type realType = paramTypes[i];
            if (declType instanceof AnyArrayType) {
                if (realType.isNull()) {
                    continue;
                }
                if (typeArray == null) {
                    typeArray = (ArrayType) realType;
                } else if ((typeArray = (ArrayType) getSuperType(typeArray, realType)) == null) {
                    LOG.warn("could not determine polymorphic type because input has non-match types");
                    return null;
                }
            } else if (declType instanceof AnyElementType) {
                if (realType.isNull()) {
                    continue;
                }
                if (typeElement == null) {
                    typeElement = realType;
                } else if ((typeElement = getSuperType(typeElement, realType)) == null) {
                    LOG.warn("could not determine polymorphic type because input has non-match types");
                    return null;
                }
            } else {
                LOG.warn("has unhandled pseudo type '{}'", declType);
                return null;
            }
        }

        if (typeArray != null && typeElement != null) {
            typeArray = (ArrayType) getSuperType(typeArray, new ArrayType(typeElement));
            if (typeArray == null) {
                LOG.warn("could not determine polymorphic type because has non-match types");
                return null;
            }
            typeElement = typeArray.getItemType();
        } else if (typeArray != null) {
            typeElement = typeArray.getItemType();
        } else if (typeElement != null) {
            typeArray = new ArrayType(typeElement);
        } else {
            typeElement = Type.NULL;
            typeArray = new ArrayType(Type.NULL);
        }

        if (!typeArray.getItemType().matchesType(typeElement)) {
            LOG.warn("could not determine polymorphic type because has non-match types");
            return null;
        }

        if (retType instanceof AnyArrayType) {
            retType = typeArray;
        } else if (retType instanceof AnyElementType) {
            retType = typeElement;
        } else if (!(fn instanceof TableFunction)) { //TableFunction don't use retType
            assert !retType.isPseudoType();
        }

        for (int i = 0; i < declTypes.length; i++) {
            if (declTypes[i] instanceof AnyArrayType) {
                realTypes[i] = typeArray;
            } else if (declTypes[i] instanceof AnyElementType) {
                realTypes[i] = typeElement;
            } else {
                realTypes[i] = declTypes[i];
            }
        }

        if (fn instanceof ScalarFunction) {
            ScalarFunction newFn = new ScalarFunction(fn.getFunctionName(), Arrays.asList(realTypes), retType,
                    fn.getLocation(), ((ScalarFunction) fn).getSymbolName(), ((ScalarFunction) fn).getPrepareFnSymbol(),
                    ((ScalarFunction) fn).getCloseFnSymbol());
            newFn.setFunctionId(fn.getFunctionId());
            newFn.setChecksum(fn.getChecksum());
            newFn.setBinaryType(fn.getBinaryType());
            newFn.setHasVarArgs(fn.hasVarArgs());
            newFn.setId(fn.getId());
            newFn.setUserVisible(fn.isUserVisible());
            return newFn;
        }
        if (fn instanceof AggregateFunction) {
            AggregateFunction newFn = new AggregateFunction(fn.getFunctionName(), Arrays.asList(realTypes), retType,
                    ((AggregateFunction) fn).getIntermediateType(), fn.hasVarArgs());
            newFn.setFunctionId(fn.getFunctionId());
            newFn.setChecksum(fn.getChecksum());
            newFn.setBinaryType(fn.getBinaryType());
            newFn.setHasVarArgs(fn.hasVarArgs());
            newFn.setId(fn.getId());
            newFn.setUserVisible(fn.isUserVisible());
            return newFn;
        }
        if (fn instanceof TableFunction) {
            TableFunction tableFunction = (TableFunction) fn;
            List<Type> tableFnRetTypes = tableFunction.getTableFnReturnTypes();
            List<Type> realTableFnRetTypes = new ArrayList<>();
            for (Type t : tableFnRetTypes) {
                if (t instanceof AnyArrayType) {
                    realTableFnRetTypes.add(typeArray);
                } else if (t instanceof AnyElementType) {
                    realTableFnRetTypes.add(typeElement);
                } else {
                    assert !retType.isPseudoType();
                }
            }

            return new TableFunction(fn.getFunctionName(), ((TableFunction) fn).getDefaultColumnNames(),
                    Arrays.asList(realTypes), realTableFnRetTypes);
        }
        LOG.error("polymorphic function has unknown type: {}", fn);
        return null;
    }

    Type getSuperType(Type t1, Type t2) {
        if (t1.matchesType(t2)) {
            return t1;
        }
        if (t1.isNull()) {
            return t2;
        }
        if (t2.isNull()) {
            return t1;
        }
        if (t1.isFixedPointType() && t2.isFixedPointType()) {
            Type commonType = Type.getCommonType(t1, t2);
            return commonType.isValid() ? commonType : null;
        }
        if (t1.isArrayType() && t2.isArrayType()) {
            Type superElementType = getSuperType(((ArrayType) t1).getItemType(), ((ArrayType) t2).getItemType());
            return superElementType != null ? new ArrayType(superElementType) : null;
        }
        return null;
    }
}
