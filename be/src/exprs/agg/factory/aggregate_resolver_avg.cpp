// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/avg.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"

namespace starrocks::vectorized {

template <PrimitiveType pt>
using AvgStateTrait = AvgAggregateState<RunTimeCppType<ImmediateAvgResultPT<pt>>>;

template <PrimitiveType pt>
inline constexpr PrimitiveType ArrayAvgResultTrait = TYPE_ARRAY;

template <PrimitiveType pt>
struct AvgBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeAvgAggregateFunction<pt>(); }
};

template <PrimitiveType pt>
struct ArrayAvgBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeArrayAggAggregateFunction<pt>(); }
};

void AggregateFuncResolver::register_avg() {
    AGGREGATE_ALL_TYPE_FROM_TRAIT("avg", true, AvgResultPT, AvgStateTrait, AvgBuilder);
    AGGREGATE_ALL_TYPE_FROM_TRAIT("array_avg", true, ArrayAvgResultTrait, AvgStateTrait, ArrayAvgBuilder);

    add_aggregate_mapping<TYPE_CHAR, TYPE_ARRAY, AvgStateTrait, ArrayAvgBuilder>("array_avg", true);
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_ARRAY, AvgStateTrait, ArrayAvgBuilder>("array_avg", true);
}

} // namespace starrocks::vectorized