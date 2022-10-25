// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"

namespace starrocks::vectorized {

void AggregateFuncResolver::register_5() {
    ADD_ALL_TYPE("first_value", true);
    ADD_ALL_TYPE("last_value", true);
    ADD_ALL_TYPE("lead", true);
    add_object_mapping<TYPE_OBJECT, TYPE_OBJECT, true>("lead");
    add_object_mapping<TYPE_HLL, TYPE_HLL, true>("lead");
    ADD_ALL_TYPE("lag", true);
    add_object_mapping<TYPE_OBJECT, TYPE_OBJECT, true>("lag");
    add_object_mapping<TYPE_HLL, TYPE_HLL, true>("lag");

    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, true>("dense_rank");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, true>("rank");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, true>("row_number");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT, true>("ntile");

    add_aggregate_mapping("exchange_bytes", TYPE_BIGINT, TYPE_BIGINT, false,
                          std::make_shared<ExchangePerfAggregateFunction<AggExchangePerfType::BYTES>>());
    add_aggregate_mapping("exchange_speed", TYPE_BIGINT, TYPE_VARCHAR, false,
                          std::make_shared<ExchangePerfAggregateFunction<AggExchangePerfType::SPEED>>());

    add_aggregate_mapping<TYPE_BIGINT, TYPE_BIGINT>("exchange_bytes");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_VARCHAR>("exchange_speed");
    add_aggregate_mapping<TYPE_CHAR, TYPE_VARCHAR>("group_concat");
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_VARCHAR>("group_concat");

    add_aggregate_mapping<TYPE_BOOLEAN, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_TINYINT, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_SMALLINT, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_INT, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_BIGINT, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_LARGEINT, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_FLOAT, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_DOUBLE, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_DATE, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_DATETIME, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_DECIMAL32, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_DECIMAL64, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_DECIMAL128, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_CHAR, TYPE_VARCHAR>("histogram");
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_VARCHAR>("histogram");
}

} // namespace starrocks::vectorized