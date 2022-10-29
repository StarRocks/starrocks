// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/exchange_perf.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {

template <PrimitiveType pt>
inline constexpr PrimitiveType HistogramResult = TYPE_VARCHAR;

template <PrimitiveType pt>
struct HisBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeHistogramAggregationFunction<pt>(); }
};

void AggregateFuncResolver::register_utility() {
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_BIGINT>(
            "exchange_bytes", false, AggregateFactory::MakeExchangePerfAggregateFunction<AggExchangePerfType::BYTES>());
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_VARCHAR>(
            "exchange_speed", false, AggregateFactory::MakeExchangePerfAggregateFunction<AggExchangePerfType::SPEED>());

    AGGREGATE_ALL_TYPE_NOTNULL_FROM_TRAIT("histogram", false, HistogramResult, HisBuilder);
}

} // namespace starrocks::vectorized
