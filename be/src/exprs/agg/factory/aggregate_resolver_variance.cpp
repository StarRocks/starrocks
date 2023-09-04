// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/covariance.h"
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
                    "variance", true, AggregateFactory::MakeVarianceAggregateFunction<pt, false>());
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "variance_pop", true, AggregateFactory::MakeVarianceAggregateFunction<pt, false>());
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "var_pop", true, AggregateFactory::MakeVarianceAggregateFunction<pt, false>());

            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "variance_samp", true, AggregateFactory::MakeVarianceAggregateFunction<pt, true>());
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "var_samp", true, AggregateFactory::MakeVarianceAggregateFunction<pt, true>());

            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "stddev", true, AggregateFactory::MakeStddevAggregateFunction<pt, false>());
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "std", true, AggregateFactory::MakeStddevAggregateFunction<pt, false>());
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "stddev_pop", true, AggregateFactory::MakeStddevAggregateFunction<pt, false>());
            resolver->add_aggregate_mapping<pt, DevFromAveResultPT<pt>, VarState>(
                    "stddev_samp", true, AggregateFactory::MakeStddevAggregateFunction<pt, true>());
        }
    };
};

struct CorVarDispatcher {
    template <PrimitiveType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_numeric<pt>) {
            using VarState = CovarianceCorelationAggregateState<false>;
            resolver->add_aggregate_mapping_variadic<pt, TYPE_DOUBLE, VarState>(
                    "covar_pop", true, AggregateFactory::MakeCovarianceAggregateFunction<pt, false>());

            resolver->add_aggregate_mapping_variadic<pt, TYPE_DOUBLE, VarState>(
                    "covar_samp", true, AggregateFactory::MakeCovarianceAggregateFunction<pt, true>());

            using CorrState = CovarianceCorelationAggregateState<true>;
            resolver->add_aggregate_mapping_variadic<pt, TYPE_DOUBLE, CorrState>(
                    "corr", true, AggregateFactory::MakeCorelationAggregateFunction<pt>());
        }
    }
};

void AggregateFuncResolver::register_variance() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, StdDispatcher(), this);
        type_dispatch_all(type, CorVarDispatcher(), this);
    }
}

} // namespace starrocks::vectorized
