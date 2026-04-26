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
#include "column/column.h"
#include "types/logical_type.h"

namespace starrocks {
template <template <bool, bool, LogicalType Type, typename... Args> typename Function, LogicalType Type,
          typename... Args>
ColumnPtr dispatch_nonull_template(ColumnPtr& col0, ColumnPtr& col1, Args&&... args) {
    bool a = col0->is_constant();
    bool b = col1->is_constant();
    if (a && b) {
        return Function<true, true, Type>::eval(col0, col1, std::forward<Args>(args)...);
    } else if (a && !b) {
        return Function<true, false, Type>::eval(col0, col1, std::forward<Args>(args)...);
    } else if (!a && b) {
        return Function<false, true, Type>::eval(col0, col1, std::forward<Args>(args)...);
    } else if (!a && !b) {
        return Function<false, false, Type>::eval(col0, col1, std::forward<Args>(args)...);
    }
    __builtin_unreachable();
};

} // namespace starrocks
