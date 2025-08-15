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
#include "column/column_helper.h"
#include "column/column_view/column_view_base.h"
#include "common/cow.h"

namespace starrocks {
class ColumnView final : public CowFactory<ColumnFactory<ColumnViewBase, ColumnView>, ColumnView, Column> {
public:
    using Super = CowFactory<ColumnFactory<ColumnViewBase, ColumnView>, ColumnView, Column>;
    explicit ColumnView(ColumnPtr&& default_column, long concat_rows_limit, long concat_bytes_limit)
            : Super(std::move(default_column), concat_rows_limit, concat_bytes_limit) {}
    ColumnView(const ColumnView& column_view) : Super(column_view) {}
    ColumnView(ColumnView&& column_view) = delete;
    bool is_view() const override { return true; }
    bool is_json_view() const override { return ColumnHelper::get_data_column(_default_column.get())->is_json(); }
    bool is_variant_view() const override { return ColumnHelper::get_data_column(_default_column.get())->is_variant(); }
    bool is_map_view() const override { return ColumnHelper::get_data_column(_default_column.get())->is_map(); }
    bool is_array_view() const override { return ColumnHelper::get_data_column(_default_column.get())->is_array(); }
    bool is_binary_view() const override { return ColumnHelper::get_data_column(_default_column.get())->is_binary(); }
    bool is_struct_view() const override { return ColumnHelper::get_data_column(_default_column.get())->is_struct(); }
};
} // namespace starrocks
