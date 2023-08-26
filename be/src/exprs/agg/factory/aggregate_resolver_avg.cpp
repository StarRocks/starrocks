// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/array_agg.h"
#include "exprs/agg/avg.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {

struct AvgDispatcher {
    template <PrimitiveType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_aggregate<pt> && !pt_is_string<pt>) {
            auto func = AggregateFactory::MakeAvgAggregateFunction<pt>();
            using AvgState = AvgAggregateState<RunTimeCppType<ImmediateAvgResultPT<pt>>>;
            resolver->add_aggregate_mapping<pt, AvgResultPT<pt>, AvgState>("avg", true, func);
        }
    }
};

struct ArrayAggDispatcher {
    template <PrimitiveType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_aggregate<pt> || pt_is_json<pt>) {
            auto func = std::make_shared<ArrayAggAggregateFunction<pt, false>>();
            using AggState = ArrayAggAggregateState<pt, false>;
            resolver->add_aggregate_mapping<pt, TYPE_ARRAY, AggState, AggregateFunctionPtr, false>("array_agg", false,
                                                                                                   func);
        }
    }
};

struct ArrayAggDistinctDispatcher {
    template <PrimitiveType pt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (pt_is_aggregate<pt>) {
            using CppType = RunTimeCppType<pt>;
            if constexpr (pt_is_largeint<pt>) {
                using MyHashSet = phmap::flat_hash_set<CppType, Hash128WithSeed<PhmapSeed1>>;
                auto func = std::make_shared<ArrayAggAggregateFunction<pt, true, MyHashSet>>();
                using AggState = ArrayAggAggregateState<pt, true, MyHashSet>;
                resolver->add_aggregate_mapping<pt, TYPE_ARRAY, AggState, AggregateFunctionPtr, false>(
                        "distinct_array_agg", false, func);
            } else if constexpr (pt_is_fixedlength<pt>) {
                using MyHashSet = phmap::flat_hash_set<CppType, StdHash<CppType>>;
                auto func = std::make_shared<ArrayAggAggregateFunction<pt, true, MyHashSet>>();
                using AggState = ArrayAggAggregateState<pt, true, MyHashSet>;
                resolver->add_aggregate_mapping<pt, TYPE_ARRAY, AggState, AggregateFunctionPtr, false>(
                        "distinct_array_agg", false, func);
            } else if constexpr (pt_is_string<pt>) {
                using MyHashSet = phmap::flat_hash_set<CppType, SliceHash>;
                auto func = std::make_shared<ArrayAggAggregateFunction<pt, true, MyHashSet>>();
                using AggState = ArrayAggAggregateState<pt, true, MyHashSet>;
                resolver->add_aggregate_mapping<pt, TYPE_ARRAY, AggState, AggregateFunctionPtr, false>(
                        "distinct_array_agg", false, func);
            } else {
                throw std::runtime_error("distinct_array_agg does not support " + type_to_string(pt));
            }
        }
    }
};

void AggregateFuncResolver::register_avg() {
    for (auto type : aggregate_types()) {
        type_dispatch_all(type, AvgDispatcher(), this);
        type_dispatch_all(type, ArrayAggDispatcher(), this);
        type_dispatch_all(type, ArrayAggDistinctDispatcher(), this);
    }
    type_dispatch_all(TYPE_JSON, ArrayAggDispatcher(), this);
    add_decimal_mapping<TYPE_DECIMAL32, TYPE_DECIMAL128, true>("decimal_avg");
    add_decimal_mapping<TYPE_DECIMAL64, TYPE_DECIMAL128, true>("decimal_avg");
    add_decimal_mapping<TYPE_DECIMAL128, TYPE_DECIMAL128, true>("decimal_avg");
}

} // namespace starrocks::vectorized
