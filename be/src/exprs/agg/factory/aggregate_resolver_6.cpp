// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"

namespace starrocks::vectorized {

void AggregateFuncResolver::register_6() {
    ADD_ALL_TYPE("first_value", true);
    ADD_ALL_TYPE("last_value", true);
    ADD_ALL_TYPE("lead", true);
    add_object_mapping<TYPE_OBJECT, TYPE_OBJECT, true>("lead");
    add_object_mapping<TYPE_HLL, TYPE_HLL, true>("lead");
    ADD_ALL_TYPE("lag", true);
    add_object_mapping<TYPE_OBJECT, TYPE_OBJECT, true>("lag");
    add_object_mapping<TYPE_HLL, TYPE_HLL, true>("lag");

    add_object_mapping<TYPE_HLL, TYPE_HLL>("hll_union");
    add_object_mapping<TYPE_HLL, TYPE_HLL>("hll_raw_agg");
    add_object_mapping<TYPE_HLL, TYPE_BIGINT>("hll_union_agg");

    add_object_mapping<TYPE_OBJECT, TYPE_OBJECT>("bitmap_union");
    add_object_mapping<TYPE_OBJECT, TYPE_OBJECT>("bitmap_intersect");
    add_object_mapping<TYPE_OBJECT, TYPE_BIGINT, true>("bitmap_union_count");

    // This first type is the second type input of intersect_count.
    // And the first type is Bitmap.
    add_object_mapping<TYPE_TINYINT, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_SMALLINT, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_INT, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_BIGINT, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_LARGEINT, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_FLOAT, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_DOUBLE, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_DATE, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_DATETIME, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_DECIMALV2, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_CHAR, TYPE_BIGINT>("intersect_count");
    add_object_mapping<TYPE_VARCHAR, TYPE_BIGINT>("intersect_count");

    add_object_mapping<TYPE_BOOLEAN, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_TINYINT, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_SMALLINT, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_INT, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_BIGINT, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_LARGEINT, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_FLOAT, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_DOUBLE, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_CHAR, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_VARCHAR, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_DECIMALV2, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_DATETIME, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_DATE, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_DECIMAL32, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_DECIMAL64, TYPE_BIGINT>("ndv");
    add_object_mapping<TYPE_DECIMAL128, TYPE_BIGINT>("ndv");

    add_object_mapping<TYPE_BOOLEAN, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_TINYINT, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_SMALLINT, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_INT, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_BIGINT, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_LARGEINT, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_FLOAT, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_DOUBLE, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_CHAR, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_VARCHAR, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_DECIMALV2, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_DATETIME, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_DATE, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_DECIMAL32, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_DECIMAL64, TYPE_BIGINT>("approx_count_distinct");
    add_object_mapping<TYPE_DECIMAL128, TYPE_BIGINT>("approx_count_distinct");
}

} // namespace starrocks::vectorized