// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/distinct.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "exprs/agg/sum.h"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {

struct SumDispatcher {
    template <PrimitiveType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_decimal<pt>) {
            resolver->add_decimal_mapping<pt, TYPE_DECIMAL128, true>("decimal_sum");
        } else if constexpr (pt_is_numeric<pt> || pt_is_decimalv2<pt>) {
            using SumState = SumAggregateState<RunTimeCppType<SumResultPT<pt>>>;
            resolver->add_aggregate_mapping<pt, SumResultPT<pt>, SumState>(
                    "sum", true, AggregateFactory::MakeSumAggregateFunction<pt>());
        }
    }
};

struct DistinctDispatcher {
    template <PrimitiveType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_aggregate<pt>) {
            using DistinctState = DistinctAggregateState<pt, SumResultPT<pt>>;
            using DistinctState2 = DistinctAggregateStateV2<pt, SumResultPT<pt>>;
            resolver->add_aggregate_mapping<pt, TYPE_BIGINT, DistinctState>(
                    "multi_distinct_count", false, AggregateFactory::MakeCountDistinctAggregateFunction<pt>());
            resolver->add_aggregate_mapping<pt, TYPE_BIGINT, DistinctState2>(
                    "multi_distinct_count2", false, AggregateFactory::MakeCountDistinctAggregateFunctionV2<pt>());

            resolver->add_aggregate_mapping<pt, SumResultPT<pt>, DistinctState>(
                    "multi_distinct_sum", false, AggregateFactory::MakeSumDistinctAggregateFunction<pt>());
            resolver->add_aggregate_mapping<pt, SumResultPT<pt>, DistinctState2>(
                    "multi_distinct_sum2", false, AggregateFactory::MakeSumDistinctAggregateFunctionV2<pt>());
        }
    }
};

void AggregateFuncResolver::register_sumcount() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, SumDispatcher(), this);
    }

    _infos_mapping.emplace(std::make_tuple("count", TYPE_BIGINT, TYPE_BIGINT, false, false),
                           AggregateFactory::MakeCountAggregateFunction<false>());
    _infos_mapping.emplace(std::make_tuple("count", TYPE_BIGINT, TYPE_BIGINT, true, false),
                           AggregateFactory::MakeCountAggregateFunction<true>());
    _infos_mapping.emplace(std::make_tuple("count", TYPE_BIGINT, TYPE_BIGINT, false, true),
                           AggregateFactory::MakeCountNullableAggregateFunction<false>());
    _infos_mapping.emplace(std::make_tuple("count", TYPE_BIGINT, TYPE_BIGINT, true, true),
                           AggregateFactory::MakeCountNullableAggregateFunction<true>());
}

void AggregateFuncResolver::register_distinct() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, DistinctDispatcher(), this);
    }
}

} // namespace starrocks::vectorized
