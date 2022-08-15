// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once
#include "column/column.h"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {
template <template <bool, bool, PrimitiveType Type, typename... Args> typename Function, PrimitiveType Type,
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

} // namespace starrocks::vectorized