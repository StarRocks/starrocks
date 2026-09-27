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

#include <vector>

#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/factory/aggregate_resolver.hpp"
#include "exprs/agg/maxmin.h"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"

namespace starrocks {

// max_by/min_by is an arg x ret cartesian (~1150 full aggregate instantiations) and dominated
// aggregate_resolver_minmaxany.cpp. These dispatchers live here so the registration can be split
// across aggregate_resolver_maxminby{1,2,3}.cpp, each handling a subset of outer key types.

template <LogicalType ret_type, bool is_max_by>
struct MaxMinByDispatcherInner {
    template <LogicalType arg_type>
    void operator()(AggregateFuncResolver* resolver) {
        if constexpr ((lt_is_aggregate<arg_type> || lt_is_json<arg_type>)&&(
                              lt_is_aggregate<ret_type> || lt_is_json<ret_type> || lt_is_collection<ret_type>)) {
            if constexpr (is_max_by) {
                resolver->add_aggregate_mapping_notnull<arg_type, ret_type>(
                        "max_by", true, AggregateFactory::MakeMaxByAggregateFunction<arg_type, false>());
                resolver->add_aggregate_mapping_notnull<arg_type, ret_type>(
                        "max_by_v2", true, AggregateFactory::MakeMaxByAggregateFunction<arg_type, true>());
            } else {
                resolver->add_aggregate_mapping_notnull<arg_type, ret_type>(
                        "min_by", true, AggregateFactory::MakeMinByAggregateFunction<arg_type, false>());
                resolver->add_aggregate_mapping_notnull<arg_type, ret_type>(
                        "min_by_v2", true, AggregateFactory::MakeMinByAggregateFunction<arg_type, true>());
            }
        }
    }
};

template <bool is_max_by>
struct MaxMinByDispatcher {
    template <LogicalType lt>
    void operator()(AggregateFuncResolver* resolver, LogicalType ret_type) {
        type_dispatch_all(ret_type, MaxMinByDispatcherInner<lt, is_max_by>(), resolver);
    }
};

// Register max_by/min_by for one fixed outer type over all ret types.
template <LogicalType outer_lt>
inline void register_maxminby_for_outer(AggregateFuncResolver* resolver, const std::vector<LogicalType>& ret_types) {
    for (auto ret_type : ret_types) {
        MaxMinByDispatcher<true>{}.template operator()<outer_lt>(resolver, ret_type);
        MaxMinByDispatcher<false>{}.template operator()<outer_lt>(resolver, ret_type);
    }
}

} // namespace starrocks
