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

#include <fmt/format.h>

#include <string_view>

#include "exprs/function_context.h"
#include "gutil/compiler_util.h"

namespace starrocks {

// The max_array_size session variable caps the number of elements in an array built by an array
// function. Every array-producing function is meant to honor it. Currently supporting functions:
//   array_agg
inline bool reject_if_array_too_large(FunctionContext* ctx, std::string_view func_name, size_t element_count) {
    const ssize_t limit = ctx->get_max_array_size();
    if (LIKELY(limit <= 0 || element_count <= static_cast<size_t>(limit))) {
        return false;
    }
    ctx->set_error(fmt::format("{} produced an array of {} elements, exceeding the limit {} set by the session "
                               "variable max_array_size.",
                               func_name, element_count, limit)
                           .c_str(),
                   false);
    return true;
}

} // namespace starrocks
