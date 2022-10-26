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
inline constexpr PrimitiveType FirstLastResult = pt;

template <PrimitiveType pt>
struct HisBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeHistogramAggregationFunction<pt>(); }
};
template <PrimitiveType pt>
struct FirstValueBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeFirstValueWindowFunction<pt>(); }
};
template <PrimitiveType pt>
struct LastValueBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeLastValueWindowFunction<pt>(); }
};
template <PrimitiveType pt>
struct LeadLagBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeLeadLagWindowFunction<pt>(); }
};

void AggregateFuncResolver::register_5() {
    AGGREGATE_ALL_TYPE_NOTNULL_FROM_TRAIT("first_value", true, FirstLastResult, FirstValueBuilder);
    AGGREGATE_ALL_TYPE_NOTNULL_FROM_TRAIT("last_value", true, FirstLastResult, FirstValueBuilder);
    AGGREGATE_ALL_TYPE_NOTNULL_FROM_TRAIT("lead", true, FirstLastResult, LeadLagBuilder);
    AGGREGATE_ALL_TYPE_NOTNULL_FROM_TRAIT("lag", true, FirstLastResult, LeadLagBuilder);

    add_object_mapping<TYPE_OBJECT, TYPE_OBJECT, true>("lead");
    add_object_mapping<TYPE_HLL, TYPE_HLL, true>("lead");
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

    AGGREGATE_ALL_TYPE_NOTNULL_FROM_TRAIT("histogram", false, HistogramResult, HisBuilder);
}

} // namespace starrocks::vectorized