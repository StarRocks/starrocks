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

#include <arrow/result.h>
#include <arrow/status.h>

#include <string>
#include <unordered_map>

#include "common/status.h"

namespace starrocks::lake::format {

static std::unordered_map<std::string, std::string> filter_map_by_key_prefix(
        const std::unordered_map<std::string, std::string>& input, const std::string& prefix) {
    std::unordered_map<std::string, std::string> result;
    for (auto it = input.begin(); it != input.end(); ++it) {
        if (it->first.compare(0, prefix.size(), prefix) == 0) {
            result[it->first] = it->second;
        }
    }
    return result;
}

template <typename T>
static T get_int_or_default(std::unordered_map<std::string, std::string>& options, const std::string& key,
                            T default_value) {
    auto itr = options.find(key);
    if (itr != options.end()) {
        return std::stoi(itr->second);
    }
    return default_value;
}

static arrow::Status to_arrow_status(const Status& status) {
    if (status.ok()) {
        return arrow::Status::OK();
    }

    return arrow::Status::Invalid(status.to_string());
}

#define FORMAT_ASSIGN_OR_RETURN_IMPL(varname, lhs, rhs) \
    auto&& varname = (rhs);                             \
    if (UNLIKELY(!varname.ok())) {                      \
        return to_arrow_status(varname.status());       \
    }                                                   \
    lhs = std::move(varname).value();

#define FORMAT_ASSIGN_OR_RAISE_ARROW_STATUS(lhs, rexpr) \
    FORMAT_ASSIGN_OR_RETURN_IMPL(ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr);

} // namespace starrocks::lake::format
