// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

// sum_map registration is a key x value cartesian (~160 full SumMapAggregateFunction
// instantiations) and used to dominate aggregate_resolver_avg.cpp (~440s of its ~640s).
// The dispatchers live here so the registration can be split across several small TUs
// (aggregate_resolver_sum_map{1,2,3}.cpp), each handling a subset of key types, so no single
// TU carries the whole cartesian.

#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "exprs/agg/sum_map.h"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"

namespace starrocks {

// For a fixed key type kt, register sum_map over all numeric value types.
template <LogicalType kt>
struct SumMapValueTypeDispatcher {
    template <LogicalType vt>
    void operator()(AggregateFuncResolver* resolver) {
        // Only register for numeric value types
        if constexpr (lt_is_numeric<vt>) {
            using KeyCppType = RunTimeCppType<kt>;
            if constexpr (lt_is_largeint<kt>) {
                using MyHashMap = phmap::flat_hash_map<KeyCppType, size_t, Hash128WithSeed<PhmapSeed1>>;
                auto func = new SumMapAggregateFunction<kt, vt, MyHashMap>();
                using State = SumMapAggregateFunctionState<kt, vt, MyHashMap>;
                resolver->add_aggregate_mapping<kt, vt, State>("sum_map", false, func);
            } else if constexpr (lt_is_fixedlength<kt>) {
                using MyHashMap = phmap::flat_hash_map<KeyCppType, size_t, StdHash<KeyCppType>>;
                auto func = new SumMapAggregateFunction<kt, vt, MyHashMap>();
                using State = SumMapAggregateFunctionState<kt, vt, MyHashMap>;
                resolver->add_aggregate_mapping<kt, vt, State>("sum_map", false, func);
            } else if constexpr (lt_is_string<kt>) {
                using MyHashMap =
                        phmap::flat_hash_map<SliceWithHash, size_t, HashOnSliceWithHash, EqualOnSliceWithHash>;
                auto func = new SumMapAggregateFunction<kt, vt, MyHashMap>();
                using State = SumMapAggregateFunctionState<kt, vt, MyHashMap>;
                resolver->add_aggregate_mapping<kt, vt, State>("sum_map", false, func);
            }
        }
    }
};

struct SumMapDispatcher {
    template <LogicalType kt>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr (lt_is_aggregate<kt>) {
            // For each key type, try all numeric value types
            type_dispatch_all(TYPE_BOOLEAN, SumMapValueTypeDispatcher<kt>(), resolver);
            type_dispatch_all(TYPE_TINYINT, SumMapValueTypeDispatcher<kt>(), resolver);
            type_dispatch_all(TYPE_SMALLINT, SumMapValueTypeDispatcher<kt>(), resolver);
            type_dispatch_all(TYPE_INT, SumMapValueTypeDispatcher<kt>(), resolver);
            type_dispatch_all(TYPE_BIGINT, SumMapValueTypeDispatcher<kt>(), resolver);
            type_dispatch_all(TYPE_LARGEINT, SumMapValueTypeDispatcher<kt>(), resolver);
            type_dispatch_all(TYPE_FLOAT, SumMapValueTypeDispatcher<kt>(), resolver);
            type_dispatch_all(TYPE_DOUBLE, SumMapValueTypeDispatcher<kt>(), resolver);
        }
    }
};

// Register sum_map for an explicit set of key types (compile-time), so each caller TU only
// instantiates the cartesian for its own key subset.
template <LogicalType... kts>
inline void register_sum_map_for_keys(AggregateFuncResolver* resolver) {
    (SumMapDispatcher{}.template operator()<kts>(resolver), ...);
}

} // namespace starrocks
