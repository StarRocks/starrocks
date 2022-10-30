// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/array_agg.h"
#include "exprs/agg/avg.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"

namespace starrocks::vectorized {

template <PrimitiveType pt>
using AvgStateTrait = AvgAggregateState<RunTimeCppType<ImmediateAvgResultPT<pt>>>;

template <PrimitiveType pt>
inline constexpr PrimitiveType ArrayResult = TYPE_ARRAY;

template <PrimitiveType pt>
struct AvgBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeAvgAggregateFunction<pt>(); }
};

template <PrimitiveType pt>
using ArrayAggStateTrait = ArrayAggAggregateState<pt>;

template <PrimitiveType pt>
struct ArrayAggBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeArrayAggAggregateFunction<pt>(); }
};

void AggregateFuncResolver::register_avg() {
    AGGREGATE_ALL_TYPE_FROM_TRAIT("avg", true, AvgResultPT, AvgStateTrait, AvgBuilder);

    AGGREGATE_ALL_TYPE_FROM_TRAIT("array_agg", false, ArrayResult, ArrayAggStateTrait, ArrayAggBuilder);
    add_aggregate_mapping<TYPE_CHAR, TYPE_ARRAY, ArrayAggStateTrait, ArrayAggBuilder>("array_agg", false);
    add_aggregate_mapping<TYPE_VARCHAR, TYPE_ARRAY, ArrayAggStateTrait, ArrayAggBuilder>("array_agg", false);
}

} // namespace starrocks::vectorized
