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

#include "exprs/agg/combinator/state_combinator.h"
#include "runtime/agg_state_desc.h"

namespace starrocks {
class AggStateDesc;

// A collection of utility functions for aggregate state.
class AggStateUtils {
public:
    // scalar function: suffix for aggregate state combinator functions
    static constexpr const char* STATE_FUNCTION_SUFFIX = "_state";
    static constexpr const char* STATE_UNION_FUNCTION_SUFFIX = "_state_union";
    static constexpr const char* STATE_MERGE_FUNCTION_SUFFIX = "_state_merge";

    // aggregate function: suffixes for aggregate state combinator functions
    static constexpr const char* AGG_STATE_UNION_SUFFIX = "_union";
    static constexpr const char* AGG_STATE_MERGE_SUFFIX = "_merge";
    static constexpr const char* AGG_STATE_COMBINE_SUFFIX = "_combine";
    static constexpr const char* AGG_STATE_IF_SUFFIX = "_if";
    static constexpr std::string FUNCTION_COUNT = "count";
    static constexpr std::string FUNCTION_COUNT_NULLABLE = "count_nullable";

    static bool is_agg_state_function(const std::string& func_name) noexcept {
        return !func_name.empty() && func_name.ends_with(STATE_FUNCTION_SUFFIX);
    }

    static bool is_agg_state_union_function(const std::string& func_name) noexcept {
        return !func_name.empty() && func_name.ends_with(STATE_UNION_FUNCTION_SUFFIX);
    }

    static bool is_agg_state_merge_function(const std::string& func_name) noexcept {
        return !func_name.empty() && func_name.ends_with(STATE_MERGE_FUNCTION_SUFFIX);
    }

    static bool is_agg_state_if(const std::string& func_name) noexcept {
        return !func_name.empty() && func_name.ends_with(AGG_STATE_IF_SUFFIX);
    }

    static bool is_agg_state_union(const std::string& nested_func_name, const std::string& func_name) noexcept {
        return nested_func_name + AGG_STATE_UNION_SUFFIX == func_name;
    }

    static bool is_agg_state_merge(const std::string& nested_func_name, const std::string& func_name) noexcept {
        return nested_func_name + AGG_STATE_MERGE_SUFFIX == func_name;
    }

    static bool is_agg_state_combine(const std::string& nested_func_name, const std::string& func_name) noexcept {
        return nested_func_name + AGG_STATE_COMBINE_SUFFIX == func_name;
    }

    static bool is_agg_state_if(const std::string& nested_func_name, const std::string& func_name) noexcept {
        return nested_func_name + AGG_STATE_IF_SUFFIX == func_name;
    }

    static bool is_count_function(const std::string& func_name) noexcept;

    // Get the aggregate state descriptor from the aggregate function.
    static const AggStateDesc* get_agg_state_desc(const AggregateFunction* agg_func);

    // Get the aggregate state function according to the agg_state_desc and function name.
    // If the function is not an aggregate state function, return nullptr.
    static StatusOr<AggregateFunctionPtr> get_agg_state_function(const AggStateDesc& agg_state_desc,
                                                                 const std::string& func_name,
                                                                 const std::vector<TypeDescriptor>& arg_types);

    // Get the aggregate state function according to the TAggStateDesc and function name.
    // If the function is not an aggregate state function, return nullptr.
    static StateCombinatorPtr get_agg_state_function(const TAggStateDesc& desc, const std::string& func_name,
                                                     const TypeDescriptor& return_type,
                                                     std::vector<bool> arg_nullables);
};

} // namespace starrocks