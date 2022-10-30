// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "runtime/primitive_type.h"
#include "types/hll.h"

namespace starrocks::vectorized {

template <PrimitiveType pt>
struct NDVBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeHllNdvAggregateFunction<pt>(); }
};

template <PrimitiveType pt>
using NDVStateTrait = HyperLogLog;
template <PrimitiveType pt>
inline constexpr PrimitiveType CountResult = TYPE_BIGINT;

template <PrimitiveType pt>
using IntersectCountStateTrait = BitmapIntersectAggregateState<BitmapRuntimeCppType<pt>>;

template <PrimitiveType pt>
struct IntersectCountBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeIntersectCountAggregateFunction<pt>(); }
};

template <PrimitiveType pt>
struct HLLRawBuilder {
    AggregateFunctionPtr operator()() { return AggregateFactory::MakeHllRawAggregateFunction<pt>(); }
};

template <PrimitiveType pt>
using HLLStateTrait = HyperLogLog;

template <PrimitiveType pt>
inline constexpr PrimitiveType HLLResult = TYPE_HLL;

void AggregateFuncResolver::register_approx() {
    AGGREGATE_ALL_OBJECT_TYPE_FROM_TRAIT("hll_raw", false, HLLResult, HLLStateTrait, HLLRawBuilder);
    add_object_mapping<TYPE_HLL, TYPE_HLL, false, HyperLogLog>("hll_union",
                                                               AggregateFactory::MakeHllUnionAggregateFunction());
    add_object_mapping<TYPE_HLL, TYPE_HLL, false, HyperLogLog>("hll_raw_agg",
                                                               AggregateFactory::MakeHllUnionAggregateFunction());
    add_object_mapping<TYPE_HLL, TYPE_BIGINT, false, HyperLogLog>(
            "hll_union_agg", AggregateFactory::MakeHllUnionCountAggregateFunction());

    AGGREGATE_ALL_OBJECT_TYPE_FROM_TRAIT("intersect_count", false, CountResult, IntersectCountStateTrait,
                                         IntersectCountBuilder);

    for (auto func_name : std::vector<std::string>{"ndv", "approx_count_distinct"}) {
        AGGREGATE_ALL_OBJECT_TYPE_FROM_TRAIT(func_name, false, CountResult, NDVStateTrait, NDVBuilder);
    }
}

} // namespace starrocks::vectorized
