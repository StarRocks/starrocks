// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/exchange_perf.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "runtime/primitive_type.h"
#include "runtime/primitive_type_infra.h"

namespace starrocks::vectorized {

struct HistogramDispatcher {
    template <PrimitiveType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_aggregate<pt>) {
            resolver->add_aggregate_mapping_notnull<pt, TYPE_VARCHAR>(
                    "histogram", false, AggregateFactory::MakeHistogramAggregationFunction<pt>());
        }
    }
};

void AggregateFuncResolver::register_utility() {
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_BIGINT>(
            "exchange_bytes", false, AggregateFactory::MakeExchangePerfAggregateFunction<AggExchangePerfType::BYTES>());
    add_aggregate_mapping_notnull<TYPE_BIGINT, TYPE_VARCHAR>(
            "exchange_speed", false, AggregateFactory::MakeExchangePerfAggregateFunction<AggExchangePerfType::SPEED>());

    for (auto type : aggregate_types()) {
        type_dispatch_all(type, HistogramDispatcher(), this);
    }
}

} // namespace starrocks::vectorized
