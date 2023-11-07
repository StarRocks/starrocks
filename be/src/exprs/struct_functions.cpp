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

#include "column/column_helper.h"
#include "column/struct_column.h"

namespace starrocks {

StatusOr<ColumnPtr> StructFunctions::new_struct(FunctionContext* context, const Columns& columns) {
    ColumnPtr res = context->create_column(context->get_return_type(), false);

    StructColumn* st = down_cast<StructColumn*>(res.get());
    auto fields = st->fields_column();

    DCHECK_EQ(fields.size(), columns.size());

    for (int i = 0; i < columns.size(); i++) {
        auto& column = columns[i];
        if (column->only_null()) {
            fields[i]->append_nulls(column->size());
        } else if (column->is_constant()) {
            auto* cc = ColumnHelper::get_data_column(column.get());
            fields[i]->append_value_multiple_times(*cc, 0, column->size());
        } else {
            fields[i]->append(*column, 0, column->size());
        }
    }

    return res;
}

StatusOr<ColumnPtr> StructFunctions::named_struct(FunctionContext* context, const Columns& columns) {
    Columns cols;
    for (int i = 1; i < columns.size(); i = i + 2) {
        cols.emplace_back(columns[i]);
    }

    return new_struct(context, cols);
}
} // namespace starrocks
