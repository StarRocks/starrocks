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

#include <initializer_list>
#include <optional>
#include <type_traits>
#include <vector>

#include "column/array_column.h"
#include "column/binary_column.h"
#include "column/column_helper.h"
#include "types/datum.h"
#include "types/type_descriptor.h"

namespace starrocks {

// A NULL row for ColumnTestHelper::build_array_column.
struct NullArrayRow {};

template <class T>
class ArrayRow;

template <class T>
struct IsArrayRow : std::false_type {};
template <class T>
struct IsArrayRow<ArrayRow<T>> : std::true_type {};

// One row for ColumnTestHelper::build_array_column: {1, std::nullopt, 3} is an array with a NULL element, {} is an
// empty array and NullArrayRow{} is a NULL array. Rows nest for ARRAY<ARRAY<...>>: an ArrayRow<ArrayRow<int32_t>>
// such as {{1, 2}, {}, NullArrayRow{}} holds its inner arrays directly, with NullArrayRow{} for a NULL inner array.
template <class T>
class ArrayRow {
public:
    using Element = std::conditional_t<IsArrayRow<T>::value, T, std::optional<T>>;

    ArrayRow(std::initializer_list<Element> elements) : _elements(elements) {}
    ArrayRow(NullArrayRow) : _is_null(true) {}

    bool is_null() const { return _is_null; }
    const std::vector<Element>& elements() const { return _elements; }

    Datum to_datum() const {
        if (_is_null) {
            return {};
        }
        DatumArray array;
        array.reserve(_elements.size());
        for (const auto& element : _elements) {
            if constexpr (IsArrayRow<T>::value) {
                array.emplace_back(element.to_datum());
            } else {
                array.emplace_back(element.has_value() ? Datum(*element) : Datum());
            }
        }
        return Datum(std::move(array));
    }

private:
    std::vector<Element> _elements;
    bool _is_null = false;
};

class ColumnTestHelper {
public:
    template <class T>
    static MutableColumnPtr build_column(const std::vector<T>& values) {
        if constexpr (std::is_integral_v<T> || std::is_floating_point_v<T>) {
            auto data = ColumnTraits<T>::ColumnType::create();
            data->append_numbers(values.data(), values.size() * sizeof(T));
            return data;
        } else if constexpr (std::is_same_v<T, Slice>) {
            auto data = BinaryColumn::create();
            data->append_strings(values.data(), values.size());
            return data;
        } else {
            throw std::runtime_error("Type is not supported in build_column:%s" + std::string(typeid(T).name()));
        }
    }

    template <class T>
    static MutableColumnPtr build_nullable_column(const std::vector<T>& values) {
        auto null_column = NullColumn::create(values.size(), 0);
        auto data_column = build_column<T>(values);
        return NullableColumn::create(std::move(data_column), std::move(null_column));
    }

    template <class T>
    static MutableColumnPtr build_nullable_column(const std::vector<T>& values, const std::vector<uint8_t>& nullflags) {
        DCHECK_EQ(values.size(), nullflags.size());
        auto null = NullColumn::create();
        null->append_numbers(nullflags.data(), nullflags.size());
        auto data = build_column<T>(values);
        return NullableColumn::create(std::move(data), std::move(null));
    }

    // Builds a column of |type|, an ARRAY of T, from |rows|. It replaces appending one DatumArray per row, which
    // instantiates the Datum variant for every element at every call site. For ARRAY<ARRAY<U>>, T is ArrayRow<U>.
    template <class T>
    static MutableColumnPtr build_array_column(const TypeDescriptor& type, bool nullable,
                                               const std::vector<ArrayRow<T>>& rows) {
        auto column = ColumnHelper::create_column(type, nullable);
        for (const auto& row : rows) {
            if (row.is_null()) {
                column->append_nulls(1);
            } else {
                column->append_datum(row.to_datum());
            }
        }
        return column;
    }

    template <LogicalType TYPE>
    static MutableColumnPtr create_nullable_column() {
        return NullableColumn::create(RunTimeColumnType<TYPE>::create(), RunTimeColumnType<TYPE_NULL>::create());
    }

    // Re-store the offsets of the BinaryColumn under |column|, which may be nullable, in 64-bit storage without
    // changing their values. This is how a BinaryColumn looks after its payload has crossed 4GB, but it only holds a
    // few bytes, so tests can cover the 64-bit offset paths without allocating gigabytes.
    static void force_large_offsets(Column* column) {
        auto* binary = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(column));
        const auto& offsets = binary->get_offset();
        AdaptiveOffsets::Large large_offsets;
        large_offsets.resize(offsets.size());
        for (size_t i = 0; i < offsets.size(); ++i) {
            large_offsets[i] = offsets[i];
        }
        binary->get_offset().set_large_buffer(std::move(large_offsets));
        binary->invalidate_slice_cache();
    }
};

} // namespace starrocks
