// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"

namespace starrocks::vectorized {

void AggregateFuncResolver::register_1() {
    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_DOUBLE, true>("avg");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_DOUBLE, true>("avg");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_DOUBLE, true>("avg");
    add_aggregate_mapping<TYPE_INT, TYPE_DOUBLE, true>("avg");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_DOUBLE, true>("avg");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_DOUBLE, true>("avg");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_DOUBLE, true>("avg");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_DOUBLE, true>("avg");
    add_aggregate_mapping<TYPE_DECIMALV2, TYPE_DECIMALV2, true>("avg");
    add_aggregate_mapping<TYPE_DATETIME, TYPE_DATETIME, true>("avg");
    add_aggregate_mapping<TYPE_DATE, TYPE_DATE, true>("avg");
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_DECIMAL128, true>("avg");
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_DECIMAL128, true>("avg");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, true>("avg");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_INT, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_CHAR, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_DATETIME, TYPE_ARRAY>("array_agg");
    add_aggregate_mapping<TYPE_DATE, TYPE_ARRAY>("array_agg");
}

} // namespace starrocks::vectorized