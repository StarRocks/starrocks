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
import com.starrocks.analysis.BinaryPredicate;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.InPredicate;
import com.starrocks.analysis.IsNullPredicate;
import com.starrocks.analysis.LikePredicate;
import com.starrocks.builtins.ScalarBuiltins;
import com.starrocks.builtins.VectorizedBuiltinFunctions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
    public static final String AVG = "avg";
    public static final String HLL_UNION = "hll_union";
    public static final String HLL_UNION_AGG = "hll_union_agg";
    public static final String HLL_RAW_AGG = "hll_raw_agg";
    public static final String NDV = "ndv";
    public static final String APPROX_COUNT_DISTINCT = "approx_count_distinct";
    public static final String PERCENTILE_APPROX = "percentile_approx";
    public static final String PERCENTILE_APPROX_RAW = "percentile_approx_raw";
    public static final String PERCENTILE_UNION = "percentile_union";
    public static final String BITMAP_UNION = "bitmap_union";
    public static final String BITMAP_UNION_COUNT = "bitmap_union_count";
    public static final String BITMAP_UNION_INT = "bitmap_union_int";
    public static final String INTERSECT_COUNT = "intersect_count";
    public static final String BITMAP_INTERSECT = "bitmap_intersect";
    public static final String MULTI_DISTINCT_COUNT = "multi_distinct_count";
    public static final String MULTI_DISTINCT_SUM = "multi_distinct_sum";
    public static final String DICT_MERGE = "dict_merge";

    // Window functions:
    public static final String LEAD = "lead";
    public static final String LAG = "lag";
    public static final String FIRST_VALUE = "first_value";
    public static final String LAST_VALUE = "last_value";

    // Scalar functions:
    public static final String BITMAP_COUNT = "bitmap_count";
    public static final String HLL_HASH = "hll_hash";
    public static final String PERCENTILE_HASH = "percentile_hash";
    public static final String TO_BITMAP = "to_bitmap";
    public static final String NULL_OR_EMPTY = "null_or_empty";
    public static final String IF = "if";

    // Arithmetic functions:
    public static final String ADD = "add";
    public static final String SUBTRACT = "subtract";
    public static final String MULTIPLY = "multiply";
    public static final String DIVIDE = "divide";

    // date functions
    public static final String YEARS_ADD = "years_add";
    public static final String YEARS_SUB = "years_sub";
    public static final String MONTHS_ADD = "months_add";
    public static final String MONTHS_SUB = "months_sub";
    public static final String DAYS_ADD = "days_add";
    public static final String DAYS_SUB = "days_sub";
    public static final String ADDDATE = "adddate";
    public static final String SUBDATE = "subdate";
    public static final String DATE_ADD = "date_add";
    public static final String DATE_SUB = "date_sub";


    private static final Logger LOG = LogManager.getLogger(FunctionSet.class);
    private static final Map<Type, String> MIN_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "3minIN13starrocks_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.TINYINT,
                            "3minIN13starrocks_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.SMALLINT,
                            "3minIN13starrocks_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.INT,
                            "3minIN13starrocks_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.BIGINT,
                            "3minIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.FLOAT,
                            "3minIN13starrocks_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DOUBLE,
                            "3minIN13starrocks_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
                    // .put(Type.CHAR,
                    //     "3minIN13starrocks_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.VARCHAR,
                            "3minIN13starrocks_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DATE,
                            "3minIN13starrocks_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DATETIME,
                            "3minIN13starrocks_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DECIMALV2,
                            "3minIN13starrocks_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.LARGEINT,
                            "3minIN13starrocks_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .build();
    private static final Map<Type, String> MAX_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "3maxIN13starrocks_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.TINYINT,
                            "3maxIN13starrocks_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.SMALLINT,
                            "3maxIN13starrocks_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.INT,
                            "3maxIN13starrocks_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.BIGINT,
                            "3maxIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.FLOAT,
                            "3maxIN13starrocks_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DOUBLE,
                            "3maxIN13starrocks_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
                    // .put(Type.CHAR,
                    //    "3maxIN13starrocks_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.VARCHAR,
                            "3maxIN13starrocks_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DATE,
                            "3maxIN13starrocks_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DATETIME,
                            "3maxIN13starrocks_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DECIMALV2,
                            "3maxIN13starrocks_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.LARGEINT,
                            "3maxIN13starrocks_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .build();
    private static final Map<Type, Type> MULTI_DISTINCT_SUM_RETURN_TYPE =
            ImmutableMap.<Type, Type>builder()
                    .put(Type.TINYINT, Type.BIGINT)
                    .put(Type.SMALLINT, Type.BIGINT)
                    .put(Type.INT, Type.BIGINT)
                    .put(Type.BIGINT, Type.BIGINT)
                    .put(Type.FLOAT, Type.DOUBLE)
                    .put(Type.DOUBLE, Type.DOUBLE)
                    .put(Type.LARGEINT, Type.LARGEINT)
                    .put(Type.DECIMALV2, Type.DECIMALV2)
                    .put(Type.DECIMAL32, Type.DECIMAL64)
                    .put(Type.DECIMAL64, Type.DECIMAL64)
                    .put(Type.DECIMAL128, Type.DECIMAL128)
                    .build();
    private static final Map<Type, String> MULTI_DISTINCT_INIT_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "34count_or_sum_distinct_numeric_initIN13starrocks_udf10TinyIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "34count_or_sum_distinct_numeric_initIN13starrocks_udf11SmallIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.INT,
                            "34count_or_sum_distinct_numeric_initIN13starrocks_udf6IntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.BIGINT,
                            "34count_or_sum_distinct_numeric_initIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.FLOAT,
                            "34count_or_sum_distinct_numeric_initIN13starrocks_udf8FloatValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.DOUBLE,
                            "34count_or_sum_distinct_numeric_initIN13starrocks_udf9DoubleValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.LARGEINT,
                            "34count_or_sum_distinct_numeric_initIN13starrocks_udf11LargeIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .build();
    private static final Map<Type, String> MULTI_DISTINCT_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "36count_or_sum_distinct_numeric_updateIN13starrocks_udf10TinyIntValEEEvPNS2_15FunctionContextERT_PNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "36count_or_sum_distinct_numeric_updateIN13starrocks_udf11SmallIntValEEEvPNS2_15FunctionContextERT_PNS2_9StringValE")
                    .put(Type.INT,
                            "36count_or_sum_distinct_numeric_updateIN13starrocks_udf6IntValEEEvPNS2_15FunctionContextERT_PNS2_9StringValE")
                    .put(Type.BIGINT,
                            "36count_or_sum_distinct_numeric_updateIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERT_PNS2_9StringValE")
                    .put(Type.FLOAT,
                            "36count_or_sum_distinct_numeric_updateIN13starrocks_udf8FloatValEEEvPNS2_15FunctionContextERT_PNS2_9StringValE")
                    .put(Type.DOUBLE,
                            "36count_or_sum_distinct_numeric_updateIN13starrocks_udf9DoubleValEEEvPNS2_15FunctionContextERT_PNS2_9StringValE")
                    .put(Type.LARGEINT,
                            "36count_or_sum_distinct_numeric_updateIN13starrocks_udf11LargeIntValEEEvPNS2_15FunctionContextERT_PNS2_9StringValE")
                    .build();
    private static final Map<Type, String> MULTI_DISTINCT_MERGE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "35count_or_sum_distinct_numeric_mergeIN13starrocks_udf10TinyIntValEEEvPNS2_15FunctionContextERNS2_9StringValEPS6_")
                    .put(Type.SMALLINT,
                            "35count_or_sum_distinct_numeric_mergeIN13starrocks_udf11SmallIntValEEEvPNS2_15FunctionContextERNS2_9StringValEPS6_")
                    .put(Type.INT,
                            "35count_or_sum_distinct_numeric_mergeIN13starrocks_udf6IntValEEEvPNS2_15FunctionContextERNS2_9StringValEPS6_")
                    .put(Type.BIGINT,
                            "35count_or_sum_distinct_numeric_mergeIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERNS2_9StringValEPS6_")
                    .put(Type.FLOAT,
                            "35count_or_sum_distinct_numeric_mergeIN13starrocks_udf8FloatValEEEvPNS2_15FunctionContextERNS2_9StringValEPS6_")
                    .put(Type.DOUBLE,
                            "35count_or_sum_distinct_numeric_mergeIN13starrocks_udf9DoubleValEEEvPNS2_15FunctionContextERNS2_9StringValEPS6_")
                    .put(Type.LARGEINT,
                            "35count_or_sum_distinct_numeric_mergeIN13starrocks_udf11LargeIntValEEEvPNS2_15FunctionContextERNS2_9StringValEPS6_")
                    .build();
    private static final Map<Type, String> MULTI_DISTINCT_SERIALIZE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "39count_or_sum_distinct_numeric_serializeIN13starrocks_udf10TinyIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
                    .put(Type.SMALLINT,
                            "39count_or_sum_distinct_numeric_serializeIN13starrocks_udf11SmallIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
                    .put(Type.INT,
                            "39count_or_sum_distinct_numeric_serializeIN13starrocks_udf6IntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
                    .put(Type.BIGINT,
                            "39count_or_sum_distinct_numeric_serializeIN13starrocks_udf9BigIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
                    .put(Type.FLOAT,
                            "39count_or_sum_distinct_numeric_serializeIN13starrocks_udf8FloatValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
                    .put(Type.DOUBLE,
                            "39count_or_sum_distinct_numeric_serializeIN13starrocks_udf9DoubleValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
                    .put(Type.LARGEINT,
                            "39count_or_sum_distinct_numeric_serializeIN13starrocks_udf11LargeIntValEEENS2_9StringValEPNS2_15FunctionContextERKS4_")
                    .build();
    private static final Map<Type, String> MULTI_DISTINCT_COUNT_FINALIZE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "38count_or_sum_distinct_numeric_finalizeIN13starrocks_udf10TinyIntValEEENS2_9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "38count_or_sum_distinct_numeric_finalizeIN13starrocks_udf11SmallIntValEEENS2_9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.INT,
                            "38count_or_sum_distinct_numeric_finalizeIN13starrocks_udf8FloatValEEENS2_9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.BIGINT,
                            "38count_or_sum_distinct_numeric_finalizeIN13starrocks_udf9BigIntValEEES3_PNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.FLOAT,
                            "38count_or_sum_distinct_numeric_finalizeIN13starrocks_udf8FloatValEEENS2_9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.DOUBLE,
                            "38count_or_sum_distinct_numeric_finalizeIN13starrocks_udf9DoubleValEEENS2_9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.LARGEINT,
                            "38count_or_sum_distinct_numeric_finalizeIN13starrocks_udf11LargeIntValEEENS2_9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .build();
    private static final Map<Type, String> MULTI_DISTINCT_SUM_FINALIZE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "28sum_distinct_bigint_finalizeIN13starrocks_udf10TinyIntValEEENS2_9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "28sum_distinct_bigint_finalizeIN13starrocks_udf11SmallIntValEEENS2_9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.INT,
                            "28sum_distinct_bigint_finalizeIN13starrocks_udf6IntValEEENS2_9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.BIGINT,
                            "28sum_distinct_bigint_finalizeIN13starrocks_udf9BigIntValEEES3_PNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.FLOAT,
                            "28sum_distinct_double_finalizeIN13starrocks_udf8FloatValEEENS2_9DoubleValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.DOUBLE,
                            "28sum_distinct_double_finalizeIN13starrocks_udf9DoubleValEEES3_PNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.LARGEINT,
                            "30sum_distinct_largeint_finalizeIN13starrocks_udf11LargeIntValEEES3_PNS2_15FunctionContextERKNS2_9StringValE")
                    .build();
    private static final Map<Type, String> STDDEV_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "16knuth_var_updateIN13starrocks_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "16knuth_var_updateIN13starrocks_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.INT,
                            "16knuth_var_updateIN13starrocks_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.DECIMAL32,
                            "16knuth_var_updateIN13starrocks_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.DECIMAL64,
                            "16knuth_var_updateIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.DECIMAL128,
                            "16knuth_var_updateIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.BIGINT,
                            "16knuth_var_updateIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.FLOAT,
                            "16knuth_var_updateIN13starrocks_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.DOUBLE,
                            "16knuth_var_updateIN13starrocks_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .build();
    private static final Map<Type, String> HLL_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "10hll_updateIN13starrocks_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.TINYINT,
                            "10hll_updateIN13starrocks_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "10hll_updateIN13starrocks_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.INT,
                            "10hll_updateIN13starrocks_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.BIGINT,
                            "10hll_updateIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.FLOAT,
                            "10hll_updateIN13starrocks_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.DOUBLE,
                            "10hll_updateIN13starrocks_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    // .put(Type.CHAR,
                    //    "10hll_updateIN13starrocks_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
                    .put(Type.VARCHAR,
                            "10hll_updateIN13starrocks_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
                    .put(Type.DATE,
                            "10hll_updateIN13starrocks_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.DATETIME,
                            "10hll_updateIN13starrocks_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.DECIMALV2,
                            "10hll_updateIN13starrocks_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.LARGEINT,
                            "10hll_updateIN13starrocks_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .build();
    private static final Map<Type, String> OFFSET_FN_INIT_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "14offset_fn_initIN13starrocks_udf10BooleanValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.DECIMALV2,
                            "14offset_fn_initIN13starrocks_udf12DecimalV2ValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.TINYINT,
                            "14offset_fn_initIN13starrocks_udf10TinyIntValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.SMALLINT,
                            "14offset_fn_initIN13starrocks_udf11SmallIntValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.DATE,
                            "14offset_fn_initIN13starrocks_udf11DateTimeValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.DATETIME,
                            "14offset_fn_initIN13starrocks_udf11DateTimeValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.INT,
                            "14offset_fn_initIN13starrocks_udf6IntValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.FLOAT,
                            "14offset_fn_initIN13starrocks_udf8FloatValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.BIGINT,
                            "14offset_fn_initIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.DOUBLE,
                            "14offset_fn_initIN13starrocks_udf9DoubleValEEEvPNS2_15FunctionContextEPT_")
                    // .put(Type.CHAR,
                    //     "14offset_fn_initIN13starrocks_udf9StringValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.VARCHAR,
                            "14offset_fn_initIN13starrocks_udf9StringValEEEvPNS2_15FunctionContextEPT_")
                    .put(Type.LARGEINT,
                            "14offset_fn_initIN13starrocks_udf11LargeIntValEEEvPNS2_15FunctionContextEPT_")

                    .build();
    private static final Map<Type, String> OFFSET_FN_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "16offset_fn_updateIN13starrocks_udf10BooleanValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                    .put(Type.DECIMALV2,
                            "16offset_fn_updateIN13starrocks_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                    .put(Type.TINYINT,
                            "16offset_fn_updateIN13starrocks_udf10TinyIntValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                    .put(Type.SMALLINT,
                            "16offset_fn_updateIN13starrocks_udf11SmallIntValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                    .put(Type.DATE,
                            "16offset_fn_updateIN13starrocks_udf11DateTimeValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                    .put(Type.DATETIME,
                            "16offset_fn_updateIN13starrocks_udf11DateTimeValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                    .put(Type.INT,
                            "16offset_fn_updateIN13starrocks_udf6IntValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                    .put(Type.FLOAT,
                            "16offset_fn_updateIN13starrocks_udf8FloatValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                    .put(Type.BIGINT,
                            "16offset_fn_updateIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_RKS3_S8_PS6_")
                    .put(Type.DOUBLE,
                            "16offset_fn_updateIN13starrocks_udf9DoubleValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                    // .put(Type.CHAR,
                    //     "16offset_fn_updateIN13starrocks_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                    .put(Type.VARCHAR,
                            "16offset_fn_updateIN13starrocks_udf9StringValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                    .put(Type.LARGEINT,
                            "16offset_fn_updateIN13starrocks_udf11LargeIntValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValES8_PS6_")
                    .build();
    private static final Map<Type, String> LAST_VALUE_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "15last_val_updateIN13starrocks_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DECIMALV2,
                            "15last_val_updateIN13starrocks_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.TINYINT,
                            "15last_val_updateIN13starrocks_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.SMALLINT,
                            "15last_val_updateIN13starrocks_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DATE,
                            "15last_val_updateIN13starrocks_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DATETIME,
                            "15last_val_updateIN13starrocks_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.INT,
                            "15last_val_updateIN13starrocks_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.FLOAT,
                            "15last_val_updateIN13starrocks_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.BIGINT,
                            "15last_val_updateIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DOUBLE,
                            "15last_val_updateIN13starrocks_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
                    // .put(Type.CHAR,
                    //     "15last_val_updateIN13starrocks_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.VARCHAR,
                            "15last_val_updateIN13starrocks_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.LARGEINT,
                            "15last_val_updateIN13starrocks_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .build();
    private static final Map<Type, String> FIRST_VALUE_REWRITE_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "24first_val_rewrite_updateIN13starrocks_udf10BooleanValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                    .put(Type.DECIMALV2,
                            "24first_val_rewrite_updateIN13starrocks_udf12DecimalV2ValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                    .put(Type.TINYINT,
                            "24first_val_rewrite_updateIN13starrocks_udf10TinyIntValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                    .put(Type.SMALLINT,
                            "24first_val_rewrite_updateIN13starrocks_udf11SmallIntValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                    .put(Type.DATE,
                            "24first_val_rewrite_updateIN13starrocks_udf11DateTimeValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                    .put(Type.DATETIME,
                            "24first_val_rewrite_updateIN13starrocks_udf11DateTimeValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                    .put(Type.INT,
                            "24first_val_rewrite_updateIN13starrocks_udf6IntValEEEvPNS2_15FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                    .put(Type.FLOAT,
                            "24first_val_rewrite_updateIN13starrocks_udf8FloatValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                    .put(Type.BIGINT,
                            "24first_val_rewrite_updateIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_RKS3_PS6_")
                    .put(Type.DOUBLE,
                            "24first_val_rewrite_updateIN13starrocks_udf9DoubleValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                    .put(Type.VARCHAR,
                            "24first_val_rewrite_updateIN13starrocks_udf9StringValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                    .put(Type.LARGEINT,
                            "24first_val_rewrite_updateIN13starrocks_udf11LargeIntValEEEvPNS2_15"
                                    + "FunctionContextERKT_RKNS2_9BigIntValEPS6_")
                    // .put(Type.VARCHAR,
                    //     "15last_val_updateIN13starrocks_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .build();
    private static final Map<Type, String> LAST_VALUE_REMOVE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "15last_val_removeIN13starrocks_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DECIMALV2,
                            "15last_val_removeIN13starrocks_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.TINYINT,
                            "15last_val_removeIN13starrocks_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.SMALLINT,
                            "15last_val_removeIN13starrocks_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DATE,
                            "15last_val_removeIN13starrocks_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DATETIME,
                            "15last_val_removeIN13starrocks_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.INT,
                            "15last_val_removeIN13starrocks_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.FLOAT,
                            "15last_val_removeIN13starrocks_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.BIGINT,
                            "15last_val_removeIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DOUBLE,
                            "15last_val_removeIN13starrocks_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
                    // .put(Type.CHAR,
                    //     "15last_val_removeIN13starrocks_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.VARCHAR,
                            "15last_val_removeIN13starrocks_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.LARGEINT,
                            "15last_val_removeIN13starrocks_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .build();
    private static final Map<Type, String> FIRST_VALUE_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.BOOLEAN,
                            "16first_val_updateIN13starrocks_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DECIMALV2,
                            "16first_val_updateIN13starrocks_udf12DecimalV2ValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.TINYINT,
                            "16first_val_updateIN13starrocks_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.SMALLINT,
                            "16first_val_updateIN13starrocks_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DATE,
                            "16first_val_updateIN13starrocks_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DATETIME,
                            "16first_val_updateIN13starrocks_udf11DateTimeValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.INT,
                            "16first_val_updateIN13starrocks_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.FLOAT,
                            "16first_val_updateIN13starrocks_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.BIGINT,
                            "16first_val_updateIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.DOUBLE,
                            "16first_val_updateIN13starrocks_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
                    // .put(Type.CHAR,
                    //     "16first_val_updateIN13starrocks_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.VARCHAR,
                            "16first_val_updateIN13starrocks_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
                    .put(Type.LARGEINT,
                            "16first_val_updateIN13starrocks_udf11LargeIntValEEEvPNS2_15FunctionContextERKT_PS6_")

                    .build();
    private static final Map<Type, String> BITMAP_UNION_INT_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "_ZN9starrocks15BitmapFunctions17bitmap_update_intIN13starrocks_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "_ZN9starrocks15BitmapFunctions17bitmap_update_intIN13starrocks_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .put(Type.INT,
                            "_ZN9starrocks15BitmapFunctions17bitmap_update_intIN13starrocks_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
                    .build();
    private static final Map<Type, String> BITMAP_INTERSECT_INIT_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "_ZN9starrocks15BitmapFunctions21bitmap_intersect_initIaN13starrocks_udf10TinyIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "_ZN9starrocks15BitmapFunctions21bitmap_intersect_initIsN13starrocks_udf11SmallIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.INT,
                            "_ZN9starrocks15BitmapFunctions21bitmap_intersect_initIiN13starrocks_udf6IntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.BIGINT,
                            "_ZN9starrocks15BitmapFunctions21bitmap_intersect_initIlN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.LARGEINT,
                            "_ZN9starrocks15BitmapFunctions21bitmap_intersect_initInN13starrocks_udf11LargeIntValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.FLOAT,
                            "_ZN9starrocks15BitmapFunctions21bitmap_intersect_initIfN13starrocks_udf8FloatValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.DOUBLE,
                            "_ZN9starrocks15BitmapFunctions21bitmap_intersect_initIdN13starrocks_udf9DoubleValEEEvPNS2_15FunctionContextEPNS2_9StringValE")
                    .put(Type.DATE,
                            "_ZN9starrocks15BitmapFunctions21bitmap_intersect_initINS_13DateTimeValueEN13starrocks_udf11DateTimeValEEEvPNS3_15FunctionContextEPNS3_9StringValE")
                    .put(Type.DATETIME,
                            "_ZN9starrocks15BitmapFunctions21bitmap_intersect_initINS_13DateTimeValueEN13starrocks_udf11DateTimeValEEEvPNS3_15FunctionContextEPNS3_9StringValE")
                    .put(Type.DECIMALV2,
                            "_ZN9starrocks15BitmapFunctions21bitmap_intersect_initINS_14DecimalV2ValueEN13starrocks_udf12DecimalV2ValEEEvPNS3_15FunctionContextEPNS3_9StringValE")
                    .put(Type.CHAR,
                            "_ZN9starrocks15BitmapFunctions21bitmap_intersect_initINS_11StringValueEN13starrocks_udf9StringValEEEvPNS3_15FunctionContextEPS4_")
                    .put(Type.VARCHAR,
                            "_ZN9starrocks15BitmapFunctions21bitmap_intersect_initINS_11StringValueEN13starrocks_udf9StringValEEEvPNS3_15FunctionContextEPS4_")
                    .build();
    private static final Map<Type, String> BITMAP_INTERSECT_UPDATE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "_ZN9starrocks15BitmapFunctions23bitmap_intersect_updateIaN13starrocks_udf10TinyIntValEEEvPNS2_15FunctionContextERKNS2_9StringValERKT0_iPSA_PS7_")
                    .put(Type.SMALLINT,
                            "_ZN9starrocks15BitmapFunctions23bitmap_intersect_updateIsN13starrocks_udf11SmallIntValEEEvPNS2_15FunctionContextERKNS2_9StringValERKT0_iPSA_PS7_")
                    .put(Type.INT,
                            "_ZN9starrocks15BitmapFunctions23bitmap_intersect_updateIiN13starrocks_udf6IntValEEEvPNS2_15FunctionContextERKNS2_9StringValERKT0_iPSA_PS7_")
                    .put(Type.BIGINT,
                            "_ZN9starrocks15BitmapFunctions23bitmap_intersect_updateIlN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKNS2_9StringValERKT0_iPSA_PS7_")
                    .put(Type.LARGEINT,
                            "_ZN9starrocks15BitmapFunctions23bitmap_intersect_updateInN13starrocks_udf11LargeIntValEEEvPNS2_15FunctionContextERKNS2_9StringValERKT0_iPSA_PS7_")
                    .put(Type.FLOAT,
                            "_ZN9starrocks15BitmapFunctions23bitmap_intersect_updateIfN13starrocks_udf8FloatValEEEvPNS2_15FunctionContextERKNS2_9StringValERKT0_iPSA_PS7_")
                    .put(Type.DOUBLE,
                            "_ZN9starrocks15BitmapFunctions23bitmap_intersect_updateIdN13starrocks_udf9DoubleValEEEvPNS2_15FunctionContextERKNS2_9StringValERKT0_iPSA_PS7_")
                    .put(Type.DATE,
                            "_ZN9starrocks15BitmapFunctions23bitmap_intersect_updateINS_13DateTimeValueEN13starrocks_udf11DateTimeValEEEvPNS3_15FunctionContextERKNS3_9StringValERKT0_iPSB_PS8_")
                    .put(Type.DATETIME,
                            "_ZN9starrocks15BitmapFunctions23bitmap_intersect_updateINS_13DateTimeValueEN13starrocks_udf11DateTimeValEEEvPNS3_15FunctionContextERKNS3_9StringValERKT0_iPSB_PS8_")
                    .put(Type.DECIMALV2,
                            "_ZN9starrocks15BitmapFunctions23bitmap_intersect_updateINS_14DecimalV2ValueEN13starrocks_udf12DecimalV2ValEEEvPNS3_15FunctionContextERKNS3_9StringValERKT0_iPSB_PS8_")
                    .put(Type.CHAR,
                            "_ZN9starrocks15BitmapFunctions23bitmap_intersect_updateINS_11StringValueEN13starrocks_udf9StringValEEEvPNS3_15FunctionContextERKS4_RKT0_iPSA_PS7_")
                    .put(Type.VARCHAR,
                            "_ZN9starrocks15BitmapFunctions23bitmap_intersect_updateINS_11StringValueEN13starrocks_udf9StringValEEEvPNS3_15FunctionContextERKS4_RKT0_iPSA_PS7_")
                    .build();
    private static final Map<Type, String> BITMAP_INTERSECT_MERGE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "_ZN9starrocks15BitmapFunctions22bitmap_intersect_mergeIaEEvPN13starrocks_udf15FunctionContextERKNS2_9StringValEPS6_")
                    .put(Type.SMALLINT,
                            "_ZN9starrocks15BitmapFunctions22bitmap_intersect_mergeIsEEvPN13starrocks_udf15FunctionContextERKNS2_9StringValEPS6_")
                    .put(Type.INT,
                            "_ZN9starrocks15BitmapFunctions22bitmap_intersect_mergeIiEEvPN13starrocks_udf15FunctionContextERKNS2_9StringValEPS6_")
                    .put(Type.BIGINT,
                            "_ZN9starrocks15BitmapFunctions22bitmap_intersect_mergeIlEEvPN13starrocks_udf15FunctionContextERKNS2_9StringValEPS6_")
                    .put(Type.LARGEINT,
                            "_ZN9starrocks15BitmapFunctions22bitmap_intersect_mergeInEEvPN13starrocks_udf15FunctionContextERKNS2_9StringValEPS6_")
                    .put(Type.FLOAT,
                            "_ZN9starrocks15BitmapFunctions22bitmap_intersect_mergeIfEEvPN13starrocks_udf15FunctionContextERKNS2_9StringValEPS6_")
                    .put(Type.DOUBLE,
                            "_ZN9starrocks15BitmapFunctions22bitmap_intersect_mergeIdEEvPN13starrocks_udf15FunctionContextERKNS2_9StringValEPS6_")
                    .put(Type.DATE,
                            "_ZN9starrocks15BitmapFunctions22bitmap_intersect_mergeINS_13DateTimeValueEEEvPN13starrocks_udf15FunctionContextERKNS3_9StringValEPS7_")
                    .put(Type.DATETIME,
                            "_ZN9starrocks15BitmapFunctions22bitmap_intersect_mergeINS_13DateTimeValueEEEvPN13starrocks_udf15FunctionContextERKNS3_9StringValEPS7_")
                    .put(Type.DECIMALV2,
                            "_ZN9starrocks15BitmapFunctions22bitmap_intersect_mergeINS_14DecimalV2ValueEEEvPN13starrocks_udf15FunctionContextERKNS3_9StringValEPS7_")
                    .put(Type.CHAR,
                            "_ZN9starrocks15BitmapFunctions22bitmap_intersect_mergeINS_11StringValueEEEvPN13starrocks_udf15FunctionContextERKNS3_9StringValEPS7_")
                    .put(Type.VARCHAR,
                            "_ZN9starrocks15BitmapFunctions22bitmap_intersect_mergeINS_11StringValueEEEvPN13starrocks_udf15FunctionContextERKNS3_9StringValEPS7_")
                    .build();
    private static final Map<Type, String> BITMAP_INTERSECT_SERIALIZE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "_ZN9starrocks15BitmapFunctions26bitmap_intersect_serializeIaEEN13starrocks_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.SMALLINT,
                            "_ZN9starrocks15BitmapFunctions26bitmap_intersect_serializeIsEEN13starrocks_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.INT,
                            "_ZN9starrocks15BitmapFunctions26bitmap_intersect_serializeIiEEN13starrocks_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.BIGINT,
                            "_ZN9starrocks15BitmapFunctions26bitmap_intersect_serializeIlEEN13starrocks_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.LARGEINT,
                            "_ZN9starrocks15BitmapFunctions26bitmap_intersect_serializeInEEN13starrocks_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.FLOAT,
                            "_ZN9starrocks15BitmapFunctions26bitmap_intersect_serializeIfEEN13starrocks_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.DOUBLE,
                            "_ZN9starrocks15BitmapFunctions26bitmap_intersect_serializeIdEEN13starrocks_udf9StringValEPNS2_15FunctionContextERKS3_")
                    .put(Type.DATE,
                            "_ZN9starrocks15BitmapFunctions26bitmap_intersect_serializeINS_13DateTimeValueEEEN13starrocks_udf9StringValEPNS3_15FunctionContextERKS4_")
                    .put(Type.DATETIME,
                            "_ZN9starrocks15BitmapFunctions26bitmap_intersect_serializeINS_13DateTimeValueEEEN13starrocks_udf9StringValEPNS3_15FunctionContextERKS4_")
                    .put(Type.DECIMALV2,
                            "_ZN9starrocks15BitmapFunctions26bitmap_intersect_serializeINS_14DecimalV2ValueEEEN13starrocks_udf9StringValEPNS3_15FunctionContextERKS4_")
                    .put(Type.CHAR,
                            "_ZN9starrocks15BitmapFunctions26bitmap_intersect_serializeINS_11StringValueEEEN13starrocks_udf9StringValEPNS3_15FunctionContextERKS4_")
                    .put(Type.VARCHAR,
                            "_ZN9starrocks15BitmapFunctions26bitmap_intersect_serializeINS_11StringValueEEEN13starrocks_udf9StringValEPNS3_15FunctionContextERKS4_")
                    .build();
    private static final Map<Type, String> BITMAP_INTERSECT_FINALIZE_SYMBOL =
            ImmutableMap.<Type, String>builder()
                    .put(Type.TINYINT,
                            "_ZN9starrocks15BitmapFunctions25bitmap_intersect_finalizeIaEEN13starrocks_udf9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.SMALLINT,
                            "_ZN9starrocks15BitmapFunctions25bitmap_intersect_finalizeIsEEN13starrocks_udf9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.INT,
                            "_ZN9starrocks15BitmapFunctions25bitmap_intersect_finalizeIiEEN13starrocks_udf9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.BIGINT,
                            "_ZN9starrocks15BitmapFunctions25bitmap_intersect_finalizeIlEEN13starrocks_udf9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.LARGEINT,
                            "_ZN9starrocks15BitmapFunctions25bitmap_intersect_finalizeInEEN13starrocks_udf9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.FLOAT,
                            "_ZN9starrocks15BitmapFunctions25bitmap_intersect_finalizeIfEEN13starrocks_udf9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.DOUBLE,
                            "_ZN9starrocks15BitmapFunctions25bitmap_intersect_finalizeIdEEN13starrocks_udf9BigIntValEPNS2_15FunctionContextERKNS2_9StringValE")
                    .put(Type.DATE,
                            "_ZN9starrocks15BitmapFunctions25bitmap_intersect_finalizeINS_13DateTimeValueEEEN13starrocks_udf9BigIntValEPNS3_15FunctionContextERKNS3_9StringValE")
                    .put(Type.DATETIME,
                            "_ZN9starrocks15BitmapFunctions25bitmap_intersect_finalizeINS_13DateTimeValueEEEN13starrocks_udf9BigIntValEPNS3_15FunctionContextERKNS3_9StringValE")
                    .put(Type.DECIMALV2,
                            "_ZN9starrocks15BitmapFunctions25bitmap_intersect_finalizeINS_14DecimalV2ValueEEEN13starrocks_udf9BigIntValEPNS3_15FunctionContextERKNS3_9StringValE")
                    .put(Type.CHAR,
                            "_ZN9starrocks15BitmapFunctions25bitmap_intersect_finalizeINS_11StringValueEEEN13starrocks_udf9BigIntValEPNS3_15FunctionContextERKNS3_9StringValE")
                    .put(Type.VARCHAR,
                            "_ZN9starrocks15BitmapFunctions25bitmap_intersect_finalizeINS_11StringValueEEEN13starrocks_udf9BigIntValEPNS3_15FunctionContextERKNS3_9StringValE")
                    .build();
    // All of the registered user functions. The key is the user facing name (e.g. "myUdf"),
    // and the values are all the overloaded variants (e.g. myUdf(double), myUdf(string))
    // This includes both UDFs and UDAs. Updates are made thread safe by synchronizing
    // on this map. Functions are sorted in a canonical order defined by
    // FunctionResolutionOrder.
    private final HashMap<String, List<Function>> functions;
    /**
     * Use for vectorized engine, but we can't use vectorized function directly, because we
     * need to check whether the expression tree can use vectorized function from bottom to
     * top, we must to re-analyze function_call_expr when vectorized function is can't used
     * if we choose to use the vectorized function here. So... we need bind vectorized function
     * to row function when init.
     */
    private final Map<String, List<Function>> vectorizedFunctions;
    // cmy: This does not contain any user defined functions. All UDFs handle null values by themselves.
    private ImmutableSet<String> nonNullResultWithNullParamFunctions;

    public FunctionSet() {
        functions = Maps.newHashMap();
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
        ScalarBuiltins.initBuiltins(this);
        ArithmeticExpr.initBuiltins(this);
        BinaryPredicate.initBuiltins(this);
        CastExpr.initBuiltins(this);
        IsNullPredicate.initBuiltins(this);
        LikePredicate.initBuiltins(this);
        InPredicate.initBuiltins(this);
        TableFunction.initBuiltins(this);

        VectorizedBuiltinFunctions.initBuiltins(this);

        // Populate all aggregate builtins.
        initAggregateBuiltins();
    }

    public void buildNonNullResultWithNullParamFunction(Set<String> funcNames) {
        ImmutableSet.Builder<String> setBuilder = new ImmutableSet.Builder<String>();
        for (String funcName : funcNames) {
            setBuilder.add(funcName);
        }
        this.nonNullResultWithNullParamFunctions = setBuilder.build();
    }

    public boolean isNonNullResultWithNullParamFunctions(String funcName) {
        return nonNullResultWithNullParamFunctions.contains(funcName);
    }

    public Function getFunction(Function desc, Function.CompareMode mode) {
        List<Function> fns = functions.get(desc.functionName());
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
        List<Function> fns = functions.computeIfAbsent(fn.functionName(), k -> Lists.newArrayList());
        fns.add(fn);
    }

    /**
     * Add a builtin with the specified name and signatures to this
     * This defaults to not using a Prepare/Close function.
     */
    public void addScalarBuiltin(String fnName, String symbol, boolean userVisible,
                                 boolean varArgs, Type retType, Type... args) {
        addScalarBuiltin(fnName, symbol, userVisible, null, null, varArgs, retType, args);
    }

    /**
     * Add a builtin with the specified name and signatures to this db.
     */
    public void addScalarBuiltin(String fnName, String symbol, boolean userVisible,
                                 String prepareFnSymbol, String closeFnSymbol, boolean varArgs,
                                 Type retType, Type... args) {
        List<Type> argsType = Arrays.stream(args).collect(Collectors.toList());
        addBuiltin(ScalarFunction.createBuiltin(
                fnName, argsType, varArgs, retType,
                symbol, prepareFnSymbol, closeFnSymbol, userVisible));
    }

    // for vectorized engine
    public void addVectorizedScalarBuiltin(long fid, String fnName, boolean varArgs,
                                           Type retType, Type... args) {

        List<Type> argsType = Arrays.stream(args).collect(Collectors.toList());
        addVectorizedBuiltin(ScalarFunction.createVectorizedBuiltin(fid, fnName, argsType, varArgs, retType));
    }

    private void addVectorizedBuiltin(Function fn) {
        if (findVectorizedFunction(fn, Function.CompareMode.IS_INDISTINGUISHABLE) != null) {
            return;
        }

        List<Function> fns = vectorizedFunctions.computeIfAbsent(fn.functionName(), k -> Lists.newArrayList());
        Function scalarFn = findScalarFunction(fn);
        if (scalarFn != null) {
            scalarFn.setFunctionId(fn.getFunctionId());
            scalarFn.setIsVectorized(fn.isVectorized());
        } else {
            List<Function> scalarFns = functions.computeIfAbsent(fn.functionName(), k -> Lists.newArrayList());
            scalarFns.add(fn);
        }
        fns.add(fn);
    }

    private Function findScalarFunction(Function desc) {
        List<Function> fns = functions.get(desc.functionName());
        if (fns == null) {
            return null;
        }
        // First check for identical
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_IDENTICAL)) {
                return f;
            }
        }
        return null;
    }

    private Function findVectorizedFunction(Function desc, Function.CompareMode mode) {
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
        if (mode == Function.CompareMode.IS_IDENTICAL) {
            return null;
        }

        // Next check for indistinguishable
        for (Function f : fns) {
            if (f.compare(desc, Function.CompareMode.IS_INDISTINGUISHABLE)) {
                return f;
            }
        }
        if (mode == Function.CompareMode.IS_INDISTINGUISHABLE) {
            return null;
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
        final String prefix = "_ZN9starrocks18AggregateFunctions";
        final String initNull = prefix + "9init_nullEPN13starrocks_udf15FunctionContextEPNS1_6AnyValE";
        final String initNullString = prefix
                + "16init_null_stringEPN13starrocks_udf15FunctionContextEPNS1_9StringValE";
        final String stringValSerializeOrFinalize = prefix
                + "32string_val_serialize_or_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE";
        final String stringValGetValue = prefix
                + "20string_val_get_valueEPN13starrocks_udf15FunctionContextERKNS1_9StringValE";

        // Type stringType[] = {Type.CHAR, Type.VARCHAR};
        // count(*)
        addBuiltin(AggregateFunction.createBuiltin(FunctionSet.COUNT,
                new ArrayList<Type>(), Type.BIGINT, Type.BIGINT,
                prefix + "9init_zeroIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
                prefix + "17count_star_updateEPN13starrocks_udf15FunctionContextEPNS1_9BigIntValE",
                prefix + "11count_mergeEPN13starrocks_udf15FunctionContextERKNS1_9BigIntValEPS4_",
                null, null,
                prefix + "17count_star_removeEPN13starrocks_udf15FunctionContextEPNS1_9BigIntValE",
                null, false, true, true));

        for (Type t : Type.getSupportedTypes()) {
            if (t.isNull()) {
                continue; // NULL is handled through type promotion.
            }
            if (t.isChar()) {
                continue; // promoted to STRING
            }
            // Count
            addBuiltin(AggregateFunction.createBuiltin(FunctionSet.COUNT,
                    Lists.newArrayList(t), Type.BIGINT, Type.BIGINT,
                    prefix + "9init_zeroIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
                    prefix + "12count_updateEPN13starrocks_udf15FunctionContextERKNS1_6AnyValEPNS1_9BigIntValE",
                    prefix + "11count_mergeEPN13starrocks_udf15FunctionContextERKNS1_9BigIntValEPS4_",
                    null, null,
                    prefix + "12count_removeEPN13starrocks_udf15FunctionContextERKNS1_6AnyValEPNS1_9BigIntValE",
                    null, false, true, true));

            if (t.isPseudoType()) {
                continue; // Only function `Count` support pseudo types now.
            }

            // count in multi distinct
            if (t.isChar() || t.isVarchar()) {
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.VARCHAR,
                        prefix + "26count_distinct_string_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                        prefix + "28count_distinct_string_updateEPN13starrocks_udf15FunctionContextERNS1_9StringValEPS4_",
                        prefix + "27count_distinct_string_mergeEPN13starrocks_udf15FunctionContextERNS1_9StringValEPS4_",
                        prefix + "31count_distinct_string_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                        null,
                        null,
                        prefix + "30count_distinct_string_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                        false, true, true));
            } else if (t.isTinyint() || t.isSmallint() || t.isInt() || t.isBigint() || t.isLargeint() || t.isDouble()) {
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.VARCHAR,
                        prefix + MULTI_DISTINCT_INIT_SYMBOL.get(t),
                        prefix + MULTI_DISTINCT_UPDATE_SYMBOL.get(t),
                        prefix + MULTI_DISTINCT_MERGE_SYMBOL.get(t),
                        prefix + MULTI_DISTINCT_SERIALIZE_SYMBOL.get(t),
                        null,
                        null,
                        prefix + MULTI_DISTINCT_COUNT_FINALIZE_SYMBOL.get(t),
                        false, true, true));
            } else if (t.isDate() || t.isDatetime()) {
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.VARCHAR,
                        prefix + "24count_distinct_date_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                        prefix +
                                "26count_distinct_date_updateEPN13starrocks_udf15FunctionContextERNS1_11DateTimeValEPNS1_9StringValE",
                        prefix + "25count_distinct_date_mergeEPN13starrocks_udf15FunctionContextERNS1_9StringValEPS4_",
                        prefix + "29count_distinct_date_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                        null,
                        null,
                        prefix + "28count_distinct_date_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                        false, true, true));
            } else if (t.isDecimalV2() || t.isDecimalV3()) {
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_count", Lists.newArrayList(t),
                        Type.BIGINT,
                        Type.VARCHAR,
                        prefix +
                                "36count_or_sum_distinct_decimalv2_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                        prefix +
                                "38count_or_sum_distinct_decimalv2_updateEPN13starrocks_udf15FunctionContextERNS1_12DecimalV2ValEPNS1_9StringValE",
                        prefix +
                                "37count_or_sum_distinct_decimalv2_mergeEPN13starrocks_udf15FunctionContextERNS1_9StringValEPS4_",
                        prefix +
                                "41count_or_sum_distinct_decimalv2_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                        null,
                        null,
                        prefix + "33count_distinct_decimalv2_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                        false, true, true));
            }

            // sum in multi distinct
            if (t.isTinyint() || t.isSmallint() || t.isInt() || t.isFloat()) {
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_sum", Lists.newArrayList(t),
                        MULTI_DISTINCT_SUM_RETURN_TYPE.get(t),
                        Type.VARCHAR,
                        prefix + MULTI_DISTINCT_INIT_SYMBOL.get(t),
                        prefix + MULTI_DISTINCT_UPDATE_SYMBOL.get(t),
                        prefix + MULTI_DISTINCT_MERGE_SYMBOL.get(t),
                        prefix + MULTI_DISTINCT_SERIALIZE_SYMBOL.get(t),
                        null,
                        null,
                        prefix + MULTI_DISTINCT_SUM_FINALIZE_SYMBOL.get(t),
                        false, true, true));
            } else if (t.isBigint() || t.isLargeint() || t.isDouble()) {
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_sum", Lists.newArrayList(t),
                        t,
                        Type.VARCHAR,
                        prefix + MULTI_DISTINCT_INIT_SYMBOL.get(t),
                        prefix + MULTI_DISTINCT_UPDATE_SYMBOL.get(t),
                        prefix + MULTI_DISTINCT_MERGE_SYMBOL.get(t),
                        prefix + MULTI_DISTINCT_SERIALIZE_SYMBOL.get(t),
                        null,
                        null,
                        prefix + MULTI_DISTINCT_SUM_FINALIZE_SYMBOL.get(t),
                        false, true, true));
            } else if (t.isDecimalV2() || t.isDecimalV3()) {
                addBuiltin(AggregateFunction.createBuiltin("multi_distinct_sum", Lists.newArrayList(t),
                        MULTI_DISTINCT_SUM_RETURN_TYPE.get(t),
                        Type.VARCHAR,
                        prefix +
                                "36count_or_sum_distinct_decimalv2_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                        prefix +
                                "38count_or_sum_distinct_decimalv2_updateEPN13starrocks_udf15FunctionContextERNS1_12DecimalV2ValEPNS1_9StringValE",
                        prefix +
                                "37count_or_sum_distinct_decimalv2_mergeEPN13starrocks_udf15FunctionContextERNS1_9StringValEPS4_",
                        prefix +
                                "41count_or_sum_distinct_decimalv2_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                        null,
                        null,
                        prefix + "31sum_distinct_decimalv2_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                        false, true, true));
            }
            // Min
            String minMaxInit = t.isStringType() ? initNullString : initNull;
            String minMaxSerializeOrFinalize = t.isStringType() ? stringValSerializeOrFinalize : null;
            String minMaxGetValue = t.isStringType() ? stringValGetValue : null;
            addBuiltin(AggregateFunction.createBuiltin("min",
                    Lists.newArrayList(t), t, t, minMaxInit,
                    prefix + MIN_UPDATE_SYMBOL.get(t),
                    prefix + MIN_UPDATE_SYMBOL.get(t),
                    minMaxSerializeOrFinalize, minMaxGetValue,
                    null, minMaxSerializeOrFinalize, true, true, false));

            // Max
            addBuiltin(AggregateFunction.createBuiltin("max",
                    Lists.newArrayList(t), t, t, minMaxInit,
                    prefix + MAX_UPDATE_SYMBOL.get(t),
                    prefix + MAX_UPDATE_SYMBOL.get(t),
                    minMaxSerializeOrFinalize, minMaxGetValue,
                    null, minMaxSerializeOrFinalize, true, true, false));

            // NDV
            // ndv return string
            addBuiltin(AggregateFunction.createBuiltin("ndv",
                    Lists.newArrayList(t), Type.BIGINT, Type.VARCHAR,
                    "_ZN9starrocks12HllFunctions8hll_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                    "_ZN9starrocks12HllFunctions" + HLL_UPDATE_SYMBOL.get(t),
                    "_ZN9starrocks12HllFunctions9hll_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN9starrocks12HllFunctions13hll_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                    "_ZN9starrocks12HllFunctions12hll_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                    true, false, true));

            //APPROX_COUNT_DISTINCT
            //alias of ndv, compute approx count distinct use HyperLogLog
            addBuiltin(AggregateFunction.createBuiltin("approx_count_distinct",
                    Lists.newArrayList(t), Type.BIGINT, Type.VARCHAR,
                    "_ZN9starrocks12HllFunctions8hll_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                    "_ZN9starrocks12HllFunctions" + HLL_UPDATE_SYMBOL.get(t),
                    "_ZN9starrocks12HllFunctions9hll_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN9starrocks12HllFunctions13hll_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                    "_ZN9starrocks12HllFunctions12hll_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                    true, false, true));

            // BITMAP_UNION_INT
            addBuiltin(AggregateFunction.createBuiltin(BITMAP_UNION_INT,
                    Lists.newArrayList(t), Type.BIGINT, Type.BITMAP,
                    "_ZN9starrocks15BitmapFunctions11bitmap_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                    BITMAP_UNION_INT_SYMBOL.get(t),
                    "_ZN9starrocks15BitmapFunctions12bitmap_unionEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                    "_ZN9starrocks15BitmapFunctions16bitmap_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                    "_ZN9starrocks15BitmapFunctions15bitmap_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                    true, false, true));

            // INTERSECT_COUNT
            addBuiltin(AggregateFunction.createBuiltin(INTERSECT_COUNT,
                    Lists.newArrayList(Type.BITMAP, t, t), Type.BIGINT, Type.VARCHAR, true,
                    BITMAP_INTERSECT_INIT_SYMBOL.get(t),
                    BITMAP_INTERSECT_UPDATE_SYMBOL.get(t),
                    BITMAP_INTERSECT_MERGE_SYMBOL.get(t),
                    BITMAP_INTERSECT_SERIALIZE_SYMBOL.get(t),
                    null,
                    null,
                    BITMAP_INTERSECT_FINALIZE_SYMBOL.get(t),
                    true, false, true));

            if (STDDEV_UPDATE_SYMBOL.containsKey(t)) {
                addBuiltin(AggregateFunction.createBuiltin("stddev",
                        Lists.newArrayList(t), Type.DOUBLE, Type.VARCHAR,
                        prefix + "14knuth_var_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + "15knuth_var_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                        null,
                        prefix + "25knuth_stddev_pop_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                        false, true, false));
                addBuiltin(AggregateFunction.createBuiltin("stddev_samp",
                        Lists.newArrayList(t), Type.DOUBLE, Type.VARCHAR,
                        prefix + "14knuth_var_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + "15knuth_var_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                        null,
                        prefix + "21knuth_stddev_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                        false, true, false));
                addBuiltin(AggregateFunction.createBuiltin("stddev_pop",
                        Lists.newArrayList(t), Type.DOUBLE, Type.VARCHAR,
                        prefix + "14knuth_var_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + "15knuth_var_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                        null,
                        prefix + "25knuth_stddev_pop_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                        false, true, false));
                addBuiltin(AggregateFunction.createBuiltin("std",
                        Lists.newArrayList(t), Type.DOUBLE, Type.VARCHAR,
                        prefix + "14knuth_var_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + "15knuth_var_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                        null,
                        prefix + "25knuth_stddev_pop_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                        false, true, false));
                addBuiltin(AggregateFunction.createBuiltin("variance",
                        Lists.newArrayList(t), Type.DOUBLE, Type.VARCHAR,
                        prefix + "14knuth_var_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + "15knuth_var_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                        null,
                        prefix + "22knuth_var_pop_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                        false, true, false));
                addBuiltin(AggregateFunction.createBuiltin("variance_samp",
                        Lists.newArrayList(t), Type.DOUBLE, Type.VARCHAR,
                        prefix + "14knuth_var_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + "15knuth_var_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                        null,
                        prefix + "18knuth_var_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                        false, true, false));
                addBuiltin(AggregateFunction.createBuiltin("var_samp",
                        Lists.newArrayList(t), Type.DOUBLE, Type.VARCHAR,
                        prefix + "14knuth_var_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + "15knuth_var_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                        null,
                        prefix + "18knuth_var_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                        false, true, false));
                addBuiltin(AggregateFunction.createBuiltin("variance_pop",
                        Lists.newArrayList(t), Type.DOUBLE, Type.VARCHAR,
                        prefix + "14knuth_var_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + "15knuth_var_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                        null,
                        prefix + "22knuth_var_pop_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                        false, true, false));
                addBuiltin(AggregateFunction.createBuiltin("var_pop",
                        Lists.newArrayList(t), Type.DOUBLE, Type.VARCHAR,
                        prefix + "14knuth_var_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                        prefix + STDDEV_UPDATE_SYMBOL.get(t),
                        prefix + "15knuth_var_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                        null,
                        prefix + "22knuth_var_pop_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                        false, true, false));
            }
        }

        // Sum
        String[] sumNames = {"sum", "sum_distinct"};
        for (String name : sumNames) {
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.TINYINT), Type.BIGINT, Type.BIGINT, initNull,
                    prefix + "3sumIN13starrocks_udf10TinyIntValENS2_9BigIntValEEEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN13starrocks_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN13starrocks_udf10TinyIntValENS2_9BigIntValEEEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.SMALLINT), Type.BIGINT, Type.BIGINT, initNull,
                    prefix + "3sumIN13starrocks_udf11SmallIntValENS2_9BigIntValEEEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN13starrocks_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN13starrocks_udf11SmallIntValENS2_9BigIntValEEEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.INT), Type.BIGINT, Type.BIGINT, initNull,
                    prefix + "3sumIN13starrocks_udf6IntValENS2_9BigIntValEEEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN13starrocks_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN13starrocks_udf6IntValENS2_9BigIntValEEEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.DECIMAL32), Type.DECIMAL64, Type.DECIMAL64, initNull,
                    prefix + "3sumIN13starrocks_udf6IntValENS2_9BigIntValEEEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN13starrocks_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN13starrocks_udf6IntValENS2_9BigIntValEEEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.BIGINT), Type.BIGINT, Type.BIGINT, initNull,
                    prefix + "3sumIN13starrocks_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN13starrocks_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN13starrocks_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.DECIMAL64), Type.DECIMAL64, Type.DECIMAL64, initNull,
                    prefix + "3sumIN13starrocks_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN13starrocks_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN13starrocks_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.FLOAT), Type.DOUBLE, Type.DOUBLE, initNull,
                    prefix + "3sumIN13starrocks_udf8FloatValENS2_9DoubleValEEEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN13starrocks_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN13starrocks_udf8FloatValENS2_9DoubleValEEEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.DOUBLE), Type.DOUBLE, Type.DOUBLE, initNull,
                    prefix + "3sumIN13starrocks_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN13starrocks_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN13starrocks_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.DECIMALV2), Type.DECIMALV2, Type.DECIMALV2, initNull,
                    prefix + "3sumIN13starrocks_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN13starrocks_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN13starrocks_udf12DecimalV2ValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.LARGEINT), Type.LARGEINT, Type.LARGEINT, initNull,
                    prefix + "3sumIN13starrocks_udf11LargeIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN13starrocks_udf11LargeIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN13starrocks_udf11LargeIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
            addBuiltin(AggregateFunction.createBuiltin(name,
                    Lists.newArrayList(Type.DECIMAL128), Type.DECIMAL128, Type.DECIMAL128, initNull,
                    prefix + "3sumIN13starrocks_udf11LargeIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    prefix + "3sumIN13starrocks_udf11LargeIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, null,
                    prefix + "10sum_removeIN13starrocks_udf11LargeIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
                    null, false, true, false));
        }

        // HLL_UNION_AGG
        addBuiltin(AggregateFunction.createBuiltin("hll_union_agg",
                Lists.newArrayList(Type.HLL), Type.BIGINT, Type.HLL,
                "_ZN9starrocks12HllFunctions8hll_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                "_ZN9starrocks12HllFunctions9hll_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN9starrocks12HllFunctions9hll_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN9starrocks12HllFunctions13hll_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                "_ZN9starrocks12HllFunctions13hll_get_valueEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                null,
                "_ZN9starrocks12HllFunctions12hll_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                true, true, true));

        // HLL_UNION
        addBuiltin(AggregateFunction.createBuiltin("hll_union",
                Lists.newArrayList(Type.HLL), Type.HLL, Type.HLL,
                "_ZN9starrocks12HllFunctions8hll_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                "_ZN9starrocks12HllFunctions9hll_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN9starrocks12HllFunctions9hll_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN9starrocks12HllFunctions13hll_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                "_ZN9starrocks12HllFunctions13hll_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                true, false, true));

        // HLL_RAW_AGG is alias of HLL_UNION
        addBuiltin(AggregateFunction.createBuiltin("hll_raw_agg",
                Lists.newArrayList(Type.HLL), Type.HLL, Type.HLL,
                "_ZN9starrocks12HllFunctions8hll_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                "_ZN9starrocks12HllFunctions9hll_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN9starrocks12HllFunctions9hll_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN9starrocks12HllFunctions13hll_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                "_ZN9starrocks12HllFunctions13hll_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                true, false, true));

        // bitmap
        addBuiltin(AggregateFunction.createBuiltin(BITMAP_UNION, Lists.newArrayList(Type.BITMAP),
                Type.BITMAP,
                Type.BITMAP,
                "_ZN9starrocks15BitmapFunctions11bitmap_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                "_ZN9starrocks15BitmapFunctions12bitmap_unionEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN9starrocks15BitmapFunctions12bitmap_unionEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN9starrocks15BitmapFunctions16bitmap_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                "_ZN9starrocks15BitmapFunctions16bitmap_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                true, false, true));

        addBuiltin(AggregateFunction.createBuiltin(BITMAP_UNION_COUNT, Lists.newArrayList(Type.BITMAP),
                Type.BIGINT,
                Type.BITMAP,
                "_ZN9starrocks15BitmapFunctions11bitmap_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                "_ZN9starrocks15BitmapFunctions12bitmap_unionEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN9starrocks15BitmapFunctions12bitmap_unionEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN9starrocks15BitmapFunctions16bitmap_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                "_ZN9starrocks15BitmapFunctions16bitmap_get_valueEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                null,
                "_ZN9starrocks15BitmapFunctions15bitmap_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                true, true, true));
        // TODO(ml): supply function symbol
        addBuiltin(AggregateFunction.createBuiltin(BITMAP_INTERSECT, Lists.newArrayList(Type.BITMAP),
                Type.BITMAP, Type.BITMAP,
                "_ZN9starrocks15BitmapFunctions20nullable_bitmap_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                "_ZN9starrocks15BitmapFunctions16bitmap_intersectEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN9starrocks15BitmapFunctions16bitmap_intersectEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                "_ZN9starrocks15BitmapFunctions16bitmap_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                "_ZN9starrocks15BitmapFunctions16bitmap_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                true, false, true));

        //PercentileApprox
        addBuiltin(AggregateFunction.createBuiltin("percentile_approx",
                Lists.newArrayList(Type.DOUBLE, Type.DOUBLE), Type.DOUBLE, Type.VARCHAR,
                prefix + "22percentile_approx_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                prefix +
                        "24percentile_approx_updateIN13starrocks_udf9DoubleValEEEvPNS2_15FunctionContextERKT_RKS3_PNS2_9StringValE",
                prefix + "23percentile_approx_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                prefix + "27percentile_approx_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                prefix + "26percentile_approx_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                false, false, false));
        addBuiltin(AggregateFunction.createBuiltin("percentile_approx",
                Lists.newArrayList(Type.DOUBLE, Type.DOUBLE, Type.DOUBLE), Type.DOUBLE, Type.VARCHAR,
                prefix + "22percentile_approx_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                prefix +
                        "24percentile_approx_updateIN13starrocks_udf9DoubleValEEEvPNS2_15FunctionContextERKT_RKS3_SA_PNS2_9StringValE",
                prefix + "23percentile_approx_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                prefix + "27percentile_approx_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                prefix + "26percentile_approx_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                false, false, false));

        addBuiltin(AggregateFunction.createBuiltin("percentile_union",
                Lists.newArrayList(Type.PERCENTILE), Type.PERCENTILE, Type.PERCENTILE,
                prefix + "22percentile_approx_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                prefix + "23percentile_approx_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                prefix + "23percentile_approx_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                prefix + "27percentile_approx_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                prefix + "27percentile_approx_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                false, false, false));

        // Avg
        // TODO: switch to CHAR(sizeof(AvgIntermediateType) when that becomes available
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.BOOLEAN), Type.DOUBLE, Type.VARCHAR,
                prefix + "8avg_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                prefix + "10avg_updateIN13starrocks_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "9avg_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "13avg_get_valueEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                prefix + "10avg_removeIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "12avg_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.TINYINT), Type.DOUBLE, Type.VARCHAR,
                prefix + "8avg_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                prefix + "10avg_updateIN13starrocks_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "9avg_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "13avg_get_valueEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                prefix + "10avg_removeIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "12avg_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.SMALLINT), Type.DOUBLE, Type.VARCHAR,
                prefix + "8avg_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                prefix + "10avg_updateIN13starrocks_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "9avg_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "13avg_get_valueEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                prefix + "10avg_removeIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "12avg_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.INT), Type.DOUBLE, Type.VARCHAR,
                prefix + "8avg_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                prefix + "10avg_updateIN13starrocks_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "9avg_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "13avg_get_valueEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                prefix + "10avg_removeIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "12avg_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.DECIMAL32), Type.DECIMAL64, Type.VARCHAR,
                prefix + "8avg_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                prefix + "10avg_updateIN13starrocks_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "9avg_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "13avg_get_valueEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                prefix + "10avg_removeIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "12avg_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.BIGINT), Type.DOUBLE, Type.VARCHAR,
                prefix + "8avg_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                prefix + "10avg_updateIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "9avg_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "13avg_get_valueEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                prefix + "10avg_removeIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "12avg_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.DECIMAL64), Type.DECIMAL64, Type.VARCHAR,
                prefix + "8avg_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                prefix + "10avg_updateIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "9avg_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "13avg_get_valueEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                prefix + "10avg_removeIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "12avg_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.FLOAT), Type.DOUBLE, Type.VARCHAR,
                prefix + "8avg_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                prefix + "10avg_updateIN13starrocks_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "9avg_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "13avg_get_valueEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                prefix + "10avg_removeIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "12avg_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.DOUBLE), Type.DOUBLE, Type.VARCHAR,
                prefix + "8avg_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                prefix + "10avg_updateIN13starrocks_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "9avg_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "13avg_get_valueEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                prefix + "10avg_removeIN13starrocks_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE",
                prefix + "12avg_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.DECIMALV2), Type.DECIMALV2, Type.VARCHAR,
                prefix + "18decimalv2_avg_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                prefix + "20decimalv2_avg_updateEPN13starrocks_udf15FunctionContextERKNS1_12DecimalV2ValEPNS1_9StringValE",
                prefix + "19decimalv2_avg_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                prefix + "23decimalv2_avg_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                prefix + "23decimalv2_avg_get_valueEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                prefix + "20decimalv2_avg_removeEPN13starrocks_udf15FunctionContextERKNS1_12DecimalV2ValEPNS1_9StringValE",
                prefix + "22decimalv2_avg_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.DECIMAL128), Type.DECIMAL128, Type.VARCHAR,
                prefix + "18decimalv2_avg_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                prefix + "20decimalv2_avg_updateEPN13starrocks_udf15FunctionContextERKNS1_12DecimalV2ValEPNS1_9StringValE",
                prefix + "19decimalv2_avg_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                prefix + "23decimalv2_avg_serializeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                prefix + "23decimalv2_avg_get_valueEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                prefix + "20decimalv2_avg_removeEPN13starrocks_udf15FunctionContextERKNS1_12DecimalV2ValEPNS1_9StringValE",
                prefix + "22decimalv2_avg_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        // Avg(Timestamp)
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.DATE), Type.DATE, Type.VARCHAR,
                prefix + "8avg_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                prefix + "20timestamp_avg_updateEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValEPNS1_9StringValE",
                prefix + "9avg_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "23timestamp_avg_get_valueEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                prefix + "20timestamp_avg_removeEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValEPNS1_9StringValE",
                prefix + "22timestamp_avg_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        addBuiltin(AggregateFunction.createBuiltin("avg",
                Lists.newArrayList(Type.DATETIME), Type.DATETIME, Type.DATETIME,
                prefix + "8avg_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                prefix + "20timestamp_avg_updateEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValEPNS1_9StringValE",
                prefix + "9avg_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "23timestamp_avg_get_valueEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                prefix + "20timestamp_avg_removeEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValEPNS1_9StringValE",
                prefix + "22timestamp_avg_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                false, true, false));
        // Group_concat(string)
        addBuiltin(AggregateFunction.createBuiltin("group_concat",
                Lists.newArrayList(Type.VARCHAR), Type.VARCHAR, Type.VARCHAR, initNullString,
                prefix + "20string_concat_updateEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                prefix + "19string_concat_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "22string_concat_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                false, false, false));
        // Group_concat(string, string)
        addBuiltin(AggregateFunction.createBuiltin("group_concat",
                Lists.newArrayList(Type.VARCHAR, Type.VARCHAR), Type.VARCHAR, Type.VARCHAR,
                initNullString,
                prefix + "20string_concat_updateEPN13starrocks_udf15FunctionContextERKNS1_9StringValES6_PS4_",
                prefix + "19string_concat_mergeEPN13starrocks_udf15FunctionContextERKNS1_9StringValEPS4_",
                stringValSerializeOrFinalize,
                prefix + "22string_concat_finalizeEPN13starrocks_udf15FunctionContextERKNS1_9StringValE",
                false, false, false));

        // analytic functions
        // Rank
        addBuiltin(AggregateFunction.createAnalyticBuiltin("rank",
                Collections.emptyList(), Type.BIGINT, Type.VARCHAR,
                prefix + "9rank_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                prefix + "11rank_updateEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                null,
                prefix + "14rank_get_valueEPN13starrocks_udf15FunctionContextERNS1_9StringValE",
                prefix + "13rank_finalizeEPN13starrocks_udf15FunctionContextERNS1_9StringValE"));
        // Dense rank
        addBuiltin(AggregateFunction.createAnalyticBuiltin("dense_rank",
                Collections.emptyList(), Type.BIGINT, Type.VARCHAR,
                prefix + "9rank_initEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                prefix + "17dense_rank_updateEPN13starrocks_udf15FunctionContextEPNS1_9StringValE",
                null,
                prefix + "20dense_rank_get_valueEPN13starrocks_udf15FunctionContextERNS1_9StringValE",
                prefix + "13rank_finalizeEPN13starrocks_udf15FunctionContextERNS1_9StringValE"));
        addBuiltin(AggregateFunction.createAnalyticBuiltin("row_number",
                Collections.emptyList(), Type.BIGINT, Type.BIGINT,
                prefix + "9init_zeroIN13starrocks_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
                prefix + "17count_star_updateEPN13starrocks_udf15FunctionContextEPNS1_9BigIntValE",
                prefix + "11count_mergeEPN13starrocks_udf15FunctionContextERKNS1_9BigIntValEPS4_",
                null, null));

        addBuiltin(AggregateFunction.createBuiltin(DICT_MERGE, Lists.newArrayList(Type.VARCHAR),
                Type.VARCHAR, Type.VARCHAR, Strings.EMPTY, Strings.EMPTY, Strings.EMPTY, Strings.EMPTY,
                Strings.EMPTY, true, false, false));

        addBuiltin(AggregateFunction.createBuiltin(DICT_MERGE, Lists.newArrayList(Type.ARRAY_VARCHAR),
                Type.VARCHAR, Type.VARCHAR, Strings.EMPTY, Strings.EMPTY, Strings.EMPTY, Strings.EMPTY,
                Strings.EMPTY, true, false, false));

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
                    "first_value", Lists.newArrayList(t), t, t,
                    t.isStringType() ? initNullString : initNull,
                    prefix + FIRST_VALUE_UPDATE_SYMBOL.get(t),
                    null,
                    t.isVarchar() ? stringValGetValue : null,
                    t.isVarchar() ? stringValSerializeOrFinalize : null));
            // Implements FIRST_VALUE for some windows that require rewrites during planning.
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "first_value_rewrite", Lists.newArrayList(t, Type.BIGINT), t, t,
                    t.isStringType() ? initNullString : initNull,
                    prefix + FIRST_VALUE_REWRITE_UPDATE_SYMBOL.get(t),
                    null,
                    t.isVarchar() ? stringValGetValue : null,
                    t.isVarchar() ? stringValSerializeOrFinalize : null,
                    false));

            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "last_value", Lists.newArrayList(t), t, t,
                    t.isStringType() ? initNullString : initNull,
                    prefix + LAST_VALUE_UPDATE_SYMBOL.get(t),
                    prefix + LAST_VALUE_REMOVE_SYMBOL.get(t),
                    t.isVarchar() ? stringValGetValue : null,
                    t.isVarchar() ? stringValSerializeOrFinalize : null));

            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lag", Lists.newArrayList(t, Type.BIGINT, t), t, t,
                    prefix + OFFSET_FN_INIT_SYMBOL.get(t),
                    prefix + OFFSET_FN_UPDATE_SYMBOL.get(t),
                    null, null, null));
            addBuiltin(AggregateFunction.createAnalyticBuiltin(
                    "lead", Lists.newArrayList(t, Type.BIGINT, t), t, t,
                    prefix + OFFSET_FN_INIT_SYMBOL.get(t),
                    prefix + OFFSET_FN_UPDATE_SYMBOL.get(t),
                    null, null, null));

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
        for (Map.Entry<String, List<Function>> entry : functions.entrySet()) {
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
            LOG.warn("could not determine polymorphic type because has unknown types");
            return null;
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
            newFn.setIsVectorized(fn.isVectorized());
            newFn.setHasVarArgs(fn.hasVarArgs());
            newFn.setId(fn.getId());
            newFn.setUserVisible(fn.isUserVisible());
            return newFn;
        }
        if (fn instanceof AggregateFunction) {
            AggregateFunction newFn = new AggregateFunction(fn.getFunctionName(), Arrays.asList(realTypes), retType,
                    ((AggregateFunction) fn).getIntermediateType(), fn.getLocation(),
                    ((AggregateFunction) fn).getUpdateFnSymbol(),
                    ((AggregateFunction) fn).getInitFnSymbol(),
                    ((AggregateFunction) fn).getSerializeFnSymbol(),
                    ((AggregateFunction) fn).getMergeFnSymbol(),
                    ((AggregateFunction) fn).getGetValueFnSymbol(),
                    ((AggregateFunction) fn).getRemoveFnSymbol(),
                    ((AggregateFunction) fn).getFinalizeFnSymbol());
            newFn.setFunctionId(fn.getFunctionId());
            newFn.setChecksum(fn.getChecksum());
            newFn.setBinaryType(fn.getBinaryType());
            newFn.setIsVectorized(fn.isVectorized());
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
