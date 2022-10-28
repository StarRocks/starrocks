// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "exprs/agg/variance.h"

namespace starrocks::vectorized {

template <PrimitiveType pt>
using VarStateTrait = DevFromAveAggregateState<RunTimeCppType<DevFromAveResultPT<pt>>>;

template <PrimitiveType pt>
struct VarBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeVarianceAggregateFunction<pt, false>(); }
};
template <PrimitiveType pt>
struct VarSampBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeVarianceAggregateFunction<pt, true>(); }
};

template <PrimitiveType pt>
struct StdBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeStddevAggregateFunction<pt, false>(); }
};
template <PrimitiveType pt>
struct StdSampBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeStddevAggregateFunction<pt, true>(); }
};

void AggregateFuncResolver::register_variance() {
    for (auto var_name : std::vector<std::string>{"variance", "variance_pop", "var_pop"}) {
        AGGREGATE_NUMERIC1_TYPE_FROM_TRAIT(var_name, false, DevFromAveResultPT, VarStateTrait, VarBuilder);
    }

    for (auto var_name : std::vector<std::string>{"variance_samp", "var_samp"}) {
        AGGREGATE_NUMERIC1_TYPE_FROM_TRAIT(var_name, false, DevFromAveResultPT, VarStateTrait, VarSampBuilder);
    }

    for (auto var_name : std::vector<std::string>{"stddev", "std", "stddev_pop"}) {
        AGGREGATE_NUMERIC1_TYPE_FROM_TRAIT(var_name, false, DevFromAveResultPT, VarStateTrait, StdBuilder);
    }

    AGGREGATE_NUMERIC1_TYPE_FROM_TRAIT("stddev_samp", false, DevFromAveResultPT, VarStateTrait, StdSampBuilder);
}

} // namespace starrocks::vectorized