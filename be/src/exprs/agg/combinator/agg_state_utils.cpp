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

#include "exprs/agg/combinator/agg_state_utils.h"

#include <fmt/format.h>

#include "exprs/agg/combinator/agg_state_combinator.h"
#include "exprs/agg/combinator/agg_state_combine.h"
#include "exprs/agg/combinator/agg_state_if.h"
#include "exprs/agg/combinator/agg_state_merge.h"
#include "exprs/agg/combinator/agg_state_union.h"
#include "exprs/agg/combinator/state_function.h"
#include "exprs/agg/combinator/state_merge_function.h"
#include "exprs/agg/combinator/state_union_function.h"

namespace starrocks {

bool AggStateUtils::is_count_function(const std::string& func_name) noexcept {
    return func_name == FUNCTION_COUNT || func_name == (FUNCTION_COUNT + AGG_STATE_IF_SUFFIX) ||
           func_name == (FUNCTION_COUNT + AGG_STATE_UNION_SUFFIX) ||
           func_name == (FUNCTION_COUNT + AGG_STATE_MERGE_SUFFIX) ||
           func_name == (FUNCTION_COUNT + AGG_STATE_COMBINE_SUFFIX);
}

// Get the aggregate state descriptor from the aggregate function.
const AggStateDesc* AggStateUtils::get_agg_state_desc(const AggregateFunction* agg_func) {
    if (dynamic_cast<const AggStateUnion*>(agg_func)) {
        auto* agg_state_union = down_cast<const AggStateUnion*>(agg_func);
        return agg_state_union->get_agg_state_desc();
    } else if (dynamic_cast<const AggStateMerge*>(agg_func)) {
        auto* agg_state_merge = down_cast<const AggStateMerge*>(agg_func);
        return agg_state_merge->get_agg_state_desc();
    } else if (dynamic_cast<const AggStateCombine*>(agg_func)) {
        auto* agg_state_merge = down_cast<const AggStateCombine*>(agg_func);
        return agg_state_merge->get_agg_state_desc();
    } else if (dynamic_cast<const AggStateIf*>(agg_func)) {
        auto* agg_state_if = down_cast<const AggStateIf*>(agg_func);
        return agg_state_if->get_agg_state_desc();
    }
    return nullptr;
}

// Get the aggregate state function according to the agg_state_desc and function name.
// If the function is not an aggregate state function, return nullptr.
StatusOr<AggregateFunctionPtr> AggStateUtils::get_agg_state_function(const AggStateDesc& agg_state_desc,
                                                                     const std::string& func_name,
                                                                     const std::vector<TypeDescriptor>& arg_types) {
    auto nested_func_name = agg_state_desc.get_func_name();
    bool is_merge_or_union = AggStateUtils::is_agg_state_merge(nested_func_name, func_name) ||
                             AggStateUtils::is_agg_state_union(nested_func_name, func_name);
    if (is_merge_or_union && arg_types.size() != 1) {
        return Status::InternalError(
                fmt::format("Invalid agg function plan: {} with (arg type {})", func_name, arg_types.size()));
    }

    if (AggStateUtils::is_agg_state_merge(nested_func_name, func_name)) {
        // aggregate _merge combinator
        auto* nested_func = AggStateDesc::get_agg_state_func(&agg_state_desc);
        if (nested_func == nullptr) {
            return Status::InternalError(fmt::format(
                    "Merge combinator function {} fails to get the nested agg func: {}", func_name, nested_func_name));
        }
        return std::make_shared<AggStateMerge>(std::move(agg_state_desc), nested_func);
    } else if (AggStateUtils::is_agg_state_union(nested_func_name, func_name)) {
        // aggregate _union combinator
        auto* nested_func = AggStateDesc::get_agg_state_func(&agg_state_desc);
        if (nested_func == nullptr) {
            return Status::InternalError(fmt::format(
                    "Union combinator function {} fails to get the nested agg func: {}", func_name, nested_func_name));
        }
        return std::make_shared<AggStateUnion>(std::move(agg_state_desc), nested_func);
    } else if (AggStateUtils::is_agg_state_combine(nested_func_name, func_name)) {
        // aggregate _combine combinator
        auto* nested_func = AggStateDesc::get_agg_state_func(&agg_state_desc);
        if (nested_func == nullptr) {
            return Status::InternalError(
                    fmt::format("Combine combinator function {} fails to get the nested agg func: {}", func_name,
                                nested_func_name));
        }
        return std::make_shared<AggStateCombine>(std::move(agg_state_desc), nested_func);
    } else if (AggStateUtils::is_agg_state_if(nested_func_name, func_name)) {
        // aggregate _if combinator
        auto* nested_func = AggStateDesc::get_agg_state_func(&agg_state_desc);
        if (nested_func == nullptr) {
            return Status::InternalError(fmt::format("if combinator function {} fails to get the nested agg func: {}",
                                                     func_name, nested_func_name));
        }
        return std::make_shared<AggStateIf>(std::move(agg_state_desc), nested_func);
    } else {
        return Status::InternalError(fmt::format("Agg function combinator is not implemented: {}", func_name));
    }
}

// Get the aggregate state function according to the TAggStateDesc and function name.
// If the function is not an aggregate state function, return nullptr.
StateCombinatorPtr AggStateUtils::get_agg_state_function(const TAggStateDesc& desc, const std::string& func_name,
                                                         const TypeDescriptor& return_type,
                                                         std::vector<bool> arg_nullables) {
    if (is_agg_state_function(func_name)) {
        auto agg_state_desc = AggStateDesc::from_thrift(desc);
        // For _state combinator function, it's created according to the agg_state_desc rather than fid.
        return std::make_shared<StateFunction>(agg_state_desc, return_type, std::move(arg_nullables));
    } else if (is_agg_state_union_function(func_name)) {
        auto agg_state_desc = AggStateDesc::from_thrift(desc);
        // For _state combinator function, it's created according to the agg_state_desc rather than fid.
        return std::make_shared<StateUnionFunction>(agg_state_desc, return_type, std::move(arg_nullables));
    } else if (is_agg_state_merge_function(func_name)) {
        auto agg_state_desc = AggStateDesc::from_thrift(desc);
        // For _state combinator function, it's created according to the agg_state_desc rather than fid.
        return std::make_shared<StateMergeFunction>(agg_state_desc, return_type, std::move(arg_nullables));
    } else {
        return nullptr;
    }
}

} // namespace starrocks