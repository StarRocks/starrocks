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

#include "column/column_helper.h"
#include "util/date_func.h"

namespace starrocks {

ColumnPtr ColumnHelper::convert_time_column_from_double_to_str(const ColumnPtr& column) {
    auto get_binary_column = [](const DoubleColumn* data_column, size_t size) -> MutableColumnPtr {
        auto new_data_column = BinaryColumn::create();
        new_data_column->reserve(size);

        for (int row = 0; row < size; ++row) {
            auto time = data_column->immutable_data()[row];
            std::string time_str = time_str_from_double(time);
            new_data_column->append(time_str);
        }

        return new_data_column;
    };

    ColumnPtr res;

    if (column->only_null()) {
        res = std::move(column);
    } else if (column->is_nullable()) {
        auto* nullable_column = down_cast<const NullableColumn*>(column.get());
        auto* data_column = down_cast<const DoubleColumn*>(nullable_column->data_column().get());
        res = NullableColumn::create(get_binary_column(data_column, column->size()),
                                     std::move(nullable_column->null_column()));
    } else if (column->is_constant()) {
        auto* const_column = down_cast<const ConstColumn*>(column.get());
        std::string time_str = time_str_from_double(const_column->get(0).get_double());
        res = ColumnHelper::create_const_column<TYPE_VARCHAR>(time_str, column->size());
    } else {
        auto* data_column = down_cast<const DoubleColumn*>(column.get());
        res = get_binary_column(data_column, column->size());
    }

    return res;
}

} // namespace starrocks
