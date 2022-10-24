// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"

namespace starrocks::vectorized {

void AggregateFuncResolver::register_3() {
    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_CHAR, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_DATETIME, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_DATE, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_BIGINT>("multi_distinct_count");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_BIGINT>("multi_distinct_count");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_CHAR, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_DATETIME, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_DATE, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_BIGINT>("multi_distinct_count2");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_BIGINT>("multi_distinct_count2");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_BIGINT>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_LARGEINT>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_DECIMAL64>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_DECIMAL64>("multi_distinct_sum");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>("multi_distinct_sum");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_BIGINT>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_BIGINT>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_BIGINT>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_INT, TYPE_BIGINT>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_LARGEINT>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_DECIMAL64>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_DECIMAL64>("multi_distinct_sum2");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128>("multi_distinct_sum2");
}
} // namespace starrocks::vectorized