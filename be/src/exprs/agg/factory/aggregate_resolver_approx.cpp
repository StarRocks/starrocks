// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "runtime/primitive_type.h"
#include "types/hll.h"

namespace starrocks::vectorized {

struct HLLUnionBuilder {
    template <PrimitiveType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_fixedlength<pt> || pt_is_string<pt>) {
            resolver->add_aggregate_mapping<pt, TYPE_HLL, HyperLogLog>(
                    "hll_raw", false, AggregateFactory::MakeHllRawAggregateFunction<pt>());

            using IntersectCountState = BitmapIntersectAggregateState<BitmapRuntimeCppType<pt>>;
            resolver->add_aggregate_mapping<pt, TYPE_BIGINT, IntersectCountState>(
                    "intersect_count", false, AggregateFactory::MakeIntersectCountAggregateFunction<pt>());

            resolver->add_aggregate_mapping<pt, TYPE_BIGINT, HyperLogLog>(
                    "ndv", false, AggregateFactory::MakeHllNdvAggregateFunction<pt>());

            resolver->add_aggregate_mapping<pt, TYPE_BIGINT, HyperLogLog>(
                    "approx_count_distinct", false, AggregateFactory::MakeHllNdvAggregateFunction<pt>());
        }
    }
};

void AggregateFuncResolver::register_approx() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, HLLUnionBuilder(), this);
    }
    add_aggregate_mapping<TYPE_HLL, TYPE_HLL, HyperLogLog>("hll_union", false,
                                                           AggregateFactory::MakeHllUnionAggregateFunction());
    add_aggregate_mapping<TYPE_HLL, TYPE_HLL, HyperLogLog>("hll_raw_agg", false,
                                                           AggregateFactory::MakeHllUnionAggregateFunction());
    add_aggregate_mapping<TYPE_HLL, TYPE_BIGINT, HyperLogLog>("hll_union_agg", false,
                                                              AggregateFactory::MakeHllUnionCountAggregateFunction());
}

} // namespace starrocks::vectorized
