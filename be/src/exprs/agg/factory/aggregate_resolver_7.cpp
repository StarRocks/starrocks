// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"

namespace starrocks::vectorized {

void AggregateFuncResolver::register_7() {
    add_object_mapping<TYPE_BOOLEAN, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_TINYINT, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_SMALLINT, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_INT, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_BIGINT, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_LARGEINT, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_FLOAT, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_DOUBLE, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_CHAR, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_VARCHAR, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_DECIMALV2, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_DATETIME, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_DATE, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_DECIMAL32, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_DECIMAL64, TYPE_HLL>("hll_raw");
    add_object_mapping<TYPE_DECIMAL128, TYPE_HLL>("hll_raw");

    add_object_mapping<TYPE_BIGINT, TYPE_DOUBLE>("percentile_approx");
    add_object_mapping<TYPE_DOUBLE, TYPE_DOUBLE>("percentile_approx");
    add_object_mapping<TYPE_PERCENTILE, TYPE_PERCENTILE>("percentile_union");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE>("percentile_cont");
    add_aggregate_mapping<TYPE_DATE, TYPE_DATE>("percentile_cont");
    add_aggregate_mapping<TYPE_DATETIME, TYPE_DATETIME>("percentile_cont");

    add_array_mapping<TYPE_ARRAY, TYPE_VARCHAR>("dict_merge");
    add_array_mapping<TYPE_ARRAY, TYPE_ARRAY>("retention");

    // sum, avg, distinct_sum use decimal128 as intermediate or result type to avoid overflow
    add_decimal_mapping<TYPE_DECIMAL32, TYPE_DECIMAL128, true>("decimal_avg");
    add_decimal_mapping<TYPE_DECIMAL64, TYPE_DECIMAL128, true>("decimal_avg");
    add_decimal_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, true>("decimal_avg");
    add_decimal_mapping<TYPE_DECIMAL32, TYPE_DECIMAL128, true>("decimal_sum");
    add_decimal_mapping<TYPE_DECIMAL64, TYPE_DECIMAL128, true>("decimal_sum");
    add_decimal_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, true>("decimal_sum");
    add_decimal_mapping<TYPE_DECIMAL32, TYPE_DECIMAL128>("decimal_multi_distinct_sum");
    add_decimal_mapping<TYPE_DECIMAL64, TYPE_DECIMAL128>("decimal_multi_distinct_sum");
    add_decimal_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>("decimal_multi_distinct_sum");
    // This first type is the 4th type input of windowfunnel.
    // And the 1st type is BigInt, 2nd is datetime, 3rd is mode(default 0).
    add_array_mapping<TYPE_INT, TYPE_INT>("window_funnel");
    add_array_mapping<TYPE_BIGINT, TYPE_INT>("window_funnel");
    add_array_mapping<TYPE_DATETIME, TYPE_INT>("window_funnel");
    add_array_mapping<TYPE_DATE, TYPE_INT>("window_funnel");
}

} // namespace starrocks::vectorized