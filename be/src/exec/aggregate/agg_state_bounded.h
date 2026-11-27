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

#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "gen_cpp/Exprs_types.h"
#include "types/type_descriptor.h"

namespace starrocks {

// Returns true only when every aggregate function in the list provably has a
// fixed-size state. Boundedness is derived from aggregate-state metadata instead
// of a hand-maintained name list: is_pod_state() is computed by the compiler as
// std::is_trivially_destructible_v<State>, so a true result guarantees the state
// owns no heap memory and its size stays sizeof(State) no matter how many input
// rows a group receives (sum/count/avg/min/max/variance/stddev/...).
//
// Everything else conservatively reports false so spill stays available:
// collect-style states that grow with input rows (array_agg, group_concat,
// multi_distinct_count, bitmap_union, ...), agg-state combinators, UDAFs, and
// any function that fails to resolve. Non-POD but bounded states (string
// min/max, HLL sketches) also report false; they only lose the small-limit
// pruning optimization, never correctness.
inline bool all_agg_states_bounded(const std::vector<TExpr>& aggregate_functions, int func_version) {
    for (const auto& agg_expr : aggregate_functions) {
        if (agg_expr.nodes.empty() || !agg_expr.nodes[0].__isset.fn) {
            return false;
        }
        const TExprNode& node = agg_expr.nodes[0];
        const TFunction& fn = node.fn;
        // agg-state combinators carry their own state description; stay conservative.
        if (fn.__isset.agg_state_desc) {
            return false;
        }
        const AggregateFunction* func = nullptr;
        if (fn.name.function_name == "count") {
            // count may have no argument (count(*)); resolve it the same way Aggregator does.
            func = get_aggregate_function("count", TYPE_BIGINT, TYPE_BIGINT, node.is_nullable);
        } else if (!fn.arg_types.empty()) {
            std::vector<TypeDescriptor> arg_types;
            arg_types.reserve(fn.arg_types.size());
            for (const auto& arg_type : fn.arg_types) {
                arg_types.push_back(TypeDescriptor::from_thrift(arg_type));
            }
            TypeDescriptor return_type = TypeDescriptor::from_thrift(fn.ret_type);
            bool is_arrow_input = fn.__isset.input_type && fn.input_type == "arrow";
            func = get_aggregate_function(fn.name.function_name, return_type, arg_types, node.is_nullable,
                                          fn.binary_type, func_version, is_arrow_input);
        }
        if (func == nullptr || !func->is_pod_state()) {
            return false;
        }
    }
    return true;
}

} // namespace starrocks
