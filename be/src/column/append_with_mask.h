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

#include "column/column_visitor_adapter.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"

namespace starrocks {

template <bool PositiveSelect>
Status append_with_mask(Column* dst, const Column& src, const uint8_t* mask, size_t count);

extern template Status append_with_mask<true>(Column*, const Column&, const uint8_t*, size_t);
extern template Status append_with_mask<false>(Column*, const Column&, const uint8_t*, size_t);

template <bool PositiveSelect>
class AppendWithMaskVisitor : public ColumnVisitorMutableAdapter<AppendWithMaskVisitor<PositiveSelect>> {
    using Base = ColumnVisitorMutableAdapter<AppendWithMaskVisitor<PositiveSelect>>;

public:
    AppendWithMaskVisitor(const Column& src, const uint8_t* mask, size_t count);

    Status do_visit(NullableColumn* column);
    Status do_visit(BinaryColumn* column);
    Status do_visit(LargeBinaryColumn* column);

    template <typename T>
    Status do_visit(FixedLengthColumnBase<T>* column) {
        return append_fixed_length(column);
    }

    template <typename ColumnType>
    Status do_visit(ColumnType* column) {
        append_fallback(column);
        return Status::OK();
    }

private:
    bool is_selected(uint8_t value) const;
    size_t count_selected() const;

    template <typename T>
    Status append_fixed_length(FixedLengthColumnBase<T>* column);

    template <typename T>
    Status append_binary_impl(BinaryColumnBase<T>* column);

    Status append_binary(BinaryColumn* column) { return append_binary_impl(column); }
    Status append_binary(LargeBinaryColumn* column) { return append_binary_impl(column); }

    Status apply(Column* dst, const Column* src);

    template <typename ColumnType>
    void append_fallback(ColumnType* column);

    const Column& _src;
    const uint8_t* _mask;
    size_t _count;
};

} // namespace starrocks
