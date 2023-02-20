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

#include "column/array_column.h"
#include "column/column_helper.h"

namespace starrocks {

class ColumnTestHelper {
public:
    template <class T>
    static ColumnPtr build_column(const std::vector<T>& values) {
        if constexpr (std::is_same_v<T, uint8_t>) {
            auto data = UInt8Column::create();
            data->append_numbers(values.data(), values.size() * sizeof(T));
            return data;
        } else if constexpr (std::is_same_v<T, int32_t>) {
            auto data = Int32Column::create();
            data->append_numbers(values.data(), values.size() * sizeof(T));
            return data;
        } else if constexpr (std::is_same_v<T, int64_t>) {
            auto data = Int64Column::create();
            data->append_numbers(values.data(), values.size() * sizeof(T));
            return data;
        } else if constexpr (std::is_same_v<T, Slice>) {
            auto data = BinaryColumn::create();
            data->append_strings(values);
            return data;
        } else if constexpr (std::is_same_v<T, double>) {
            auto data = DoubleColumn ::create();
            data->append_numbers(values.data(), values.size() * sizeof(T));
            return data;
        } else {
            throw std::runtime_error("Type is not supported in build_column:%s" + std::string(typeid(T).name()));
        }
    }

    template <class T>
    static ColumnPtr build_nullable_column(const std::vector<T>& values, const std::vector<uint8_t>& nullflags) {
        DCHECK_EQ(values.size(), nullflags.size());
        auto null = NullColumn::create();
        null->append_numbers(nullflags.data(), nullflags.size());
        auto data = build_column<T>(values);
        return NullableColumn::create(std::move(data), std::move(null));
    }

    template <LogicalType TYPE>
    static ColumnPtr create_nullable_column() {
        return NullableColumn::create(RunTimeColumnType<TYPE>::create(), RunTimeColumnType<TYPE_NULL>::create());
    }
};

} // namespace starrocks
