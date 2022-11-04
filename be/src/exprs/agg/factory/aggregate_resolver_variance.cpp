// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "exprs/agg/variance.h"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {

struct StdDispatcher {
    template <PrimitiveType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_numeric<pt>) {
            using VarState = DevFromAveAggregateState<RunTimeCppType<DevFromAveResultPT<pt>>>;
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "variance", false, AggregateFactory::MakeVarianceAggregateFunction<pt, false>());
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "variance_pop", false, AggregateFactory::MakeVarianceAggregateFunction<pt, false>());
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "var_pop", false, AggregateFactory::MakeVarianceAggregateFunction<pt, false>());

            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "variance_samp", false, AggregateFactory::MakeVarianceAggregateFunction<pt, true>());
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "var_samp", false, AggregateFactory::MakeVarianceAggregateFunction<pt, true>());

            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "stddev", false, AggregateFactory::MakeStddevAggregateFunction<pt, false>());
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "std", false, AggregateFactory::MakeStddevAggregateFunction<pt, false>());
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "stddev_pop", false, AggregateFactory::MakeStddevAggregateFunction<pt, false>());
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "stddev_samp", false, AggregateFactory::MakeStddevAggregateFunction<pt, true>());
        }
    }
};

void AggregateFuncResolver::register_variance() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, StdDispatcher(), this);
    }
}

} // namespace starrocks::vectorized
