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

#include "exprs/struct_functions.h"

#include "column/struct_column.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

StatusOr<ColumnPtr> StructFunctions::struct_ctor(FunctionContext* context, const Columns& columns) {
    Columns field_columns;
    for (auto& column : columns) {
        field_columns.emplace_back(column->clone());
    }
    return StructColumn::create(std::move(field_columns));
}

StatusOr<ColumnPtr> StructFunctions::named_struct(FunctionContext* context, const Columns& columns) {
    if (columns.size() % 2 != 0) {
        return Status::InvalidArgument("named_struct expects an even number of arguments.");
    }

    Columns field_columns;
    std::vector<std::string> field_names;
    for (std::size_t i = 0; i < columns.size() - 1; i += 2) {
        auto* field_name_column = ColumnHelper::get_data_column(columns[i].get());
        auto* field_column = columns[i + 1].get();

        if (!field_name_column->is_binary() || field_name_column->size() != 1) {
            return Status::InvalidArgument(strings::Substitute(
                    "Only foldable string expressions are allowed to appear at even position, pos $0 got: [$1]", i,
                    field_name_column->debug_string()));
        }

        field_names.emplace_back(down_cast<BinaryColumn*>(field_name_column)->get(0).get_slice().to_string());
        field_columns.emplace_back(field_column->clone());
    }
    return StructColumn::create(std::move(field_columns), std::move(field_names));
}

} // namespace starrocks
