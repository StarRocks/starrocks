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

#include <type_traits>

#include "column/type_traits.h"
#include "gutil/strings/fastmem.h"
#include "types/logical_type.h"

namespace starrocks {

// Type traits from aggregate functions
template <LogicalType lt, typename = guard::Guard>
struct AggDataTypeTraits {};

template <LogicalType lt>
struct AggDataTypeTraits<lt, FixedLengthLTGuard<lt>> {
    using ColumnType = RunTimeColumnType<lt>;
    using ValueType = RunTimeCppValueType<lt>;
    using RefType = RunTimeCppType<lt>;

    static void assign_value(ValueType& value, const RefType& ref) { value = ref; }
    static void assign_value(ColumnType* column, size_t row, const RefType& ref) { column->get_data()[row] = ref; }

    static void append_value(ColumnType* column, const ValueType& value) { column->append(value); }

    static RefType get_row_ref(const ColumnType& column, size_t row) { return column.get_data()[row]; }

    static void update_max(ValueType& current, const RefType& input) { current = std::max<ValueType>(current, input); }
    static void update_min(ValueType& current, const RefType& input) { current = std::min<ValueType>(current, input); }
};

// For pointer ref types
template <LogicalType lt>
struct AggDataTypeTraits<lt, ObjectFamilyLTGuard<lt>> {
    using ColumnType = RunTimeColumnType<lt>;
    using ValueType = RunTimeCppValueType<lt>;
    using RefType = RunTimeCppType<lt>;

    static void assign_value(ValueType& value, RefType ref) { value = *ref; }
    static void assign_value(ColumnType* column, size_t row, const RefType& ref) { *column->get_object(row) = *ref; }
    static void assign_value(ColumnType* column, size_t row, const ValueType& ref) { *column->get_object(row) = ref; }

    static void append_value(ColumnType* column, const ValueType& value) { column->append(&value); }

    static const RefType get_row_ref(const ColumnType& column, size_t row) { return column.get_object(row); }

    static void update_max(ValueType& current, const RefType& input) { current = std::max<ValueType>(current, *input); }
    static void update_min(ValueType& current, const RefType& input) { current = std::min<ValueType>(current, *input); }
};

template <LogicalType lt>
struct AggDataTypeTraits<lt, StringLTGuard<lt>> {
    using ColumnType = RunTimeColumnType<lt>;
    using ValueType = Buffer<uint8_t>;
    using RefType = Slice;

    static void assign_value(ValueType& value, const RefType& ref) {
        value.resize(ref.size);
        strings::memcpy_inlined(value.data(), ref.data, ref.size);
    }

    static void append_value(ColumnType* column, const ValueType& value) {
        column->append(Slice(value.data(), value.size()));
    }

    static RefType get_row_ref(const ColumnType& column, size_t row) { return column.get_slice(row); }

    static void update_max(ValueType& current, const RefType& input) {
        if (Slice(current.data(), current.size()).compare(input) < 0) {
            current.resize(input.size);
            memcpy(current.data(), input.data, input.size);
        }
    }
    static void update_min(ValueType& current, const RefType& input) {
        if (Slice(current.data(), current.size()).compare(input) > 0) {
            current.resize(input.size);
            memcpy(current.data(), input.data, input.size);
        }
    }
};

template <LogicalType lt>
using AggDataValueType = typename AggDataTypeTraits<lt>::ValueType;
template <LogicalType lt>
using AggDataRefType = typename AggDataTypeTraits<lt>::RefType;

} // namespace starrocks