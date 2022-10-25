// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/exchange_perf.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"

namespace starrocks::vectorized {

template <PrimitiveType pt>
struct HisBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeHistogramAggregationFunction<pt>(); }
};

void AggregateFuncResolver::register_5() {
    ADD_ALL_TYPE("first_value", true);
    ADD_ALL_TYPE("last_value", true);

    ADD_ALL_TYPE("lead", true);
    add_object_mapping<TYPE_OBJECT, TYPE_OBJECT, true>("lead");
    add_object_mapping<TYPE_HLL, TYPE_HLL, true>("lead");

    ADD_ALL_TYPE("lag", true);
    add_object_mapping<TYPE_OBJECT, TYPE_OBJECT, true>("lag");
    add_object_mapping<TYPE_HLL, TYPE_HLL, true>("lag");

    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_BIGINT>("dense_rank", true,
                                                            AggregateFactory::MakeDenseRankWindowFunction());
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_BIGINT>("rank", true, AggregateFactory::MakeRankWindowFunction());
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_BIGINT>("row_number", true,
                                                            AggregateFactory::MakeRowNumberWindowFunction());
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_BIGINT>("ntile", true, AggregateFactory::MakeNtileWindowFunction());
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_BIGINT>(
            "exchange_bytes", false, AggregateFactory::MakeExchangePerfAggregateFunction<AggExchangePerfType::BYTES>());
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_VARCHAR>(
            "exchange_speed", false, AggregateFactory::MakeExchangePerfAggregateFunction<AggExchangePerfType::SPEED>());

    add_aggregate_mapping<TYPE_CHAR, TYPE_VARCHAR>("group_concat");
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_VARCHAR>("group_concat");

    add_aggregate_mapping_notnull<TYPE_BOOLEAN, TYPE_VARCHAR, HisBuilder>("histogram", false);
    add_aggregate_mapping_notnull<TYPE_TINYINT, TYPE_VARCHAR, HisBuilder>("histogram", false);
    add_aggregate_mapping_notnull<TYPE_SMALLINT, TYPE_VARCHAR, HisBuilder>("histogram", false);
    add_aggregate_mapping_notnull<TYPE_INT, TYPE_VARCHAR, HisBuilder>("histogram", false);
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_VARCHAR, HisBuilder>("histogram", false);
    add_aggregate_mapping_notnull<TYPE_LARGEINT, TYPE_VARCHAR, HisBuilder>("histogram", false);
    add_aggregate_mapping_notnull<TYPE_FLOAT, TYPE_VARCHAR, HisBuilder>("histogram", false);
    add_aggregate_mapping_notnull<TYPE_DOUBLE, TYPE_VARCHAR, HisBuilder>("histogram", false);
    add_aggregate_mapping_notnull<TYPE_DATE, TYPE_VARCHAR, HisBuilder>("histogram", false);
    add_aggregate_mapping_notnull<TYPE_DATETIME, TYPE_VARCHAR, HisBuilder>("histogram", false);
    add_aggregate_mapping_notnull<TYPE_DECIMAL32, TYPE_VARCHAR, HisBuilder>("histogram", false);
    add_aggregate_mapping_notnull<TYPE_DECIMAL64, TYPE_VARCHAR, HisBuilder>("histogram", false);
    add_aggregate_mapping_notnull<TYPE_DECIMAL128, TYPE_VARCHAR, HisBuilder>("histogram", false);
    add_aggregate_mapping_notnull<TYPE_CHAR, TYPE_VARCHAR, HisBuilder>("histogram", false);
    add_aggregate_mapping_notnull<TYPE_VARCHAR, TYPE_VARCHAR, HisBuilder>("histogram", false);
}

} // namespace starrocks::vectorized