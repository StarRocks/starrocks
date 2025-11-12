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

#include "column/column_view/column_view_helper.h"

#include "column/column_helper.h"
#include "column/column_view/column_view.h"
#include "runtime/types.h"
#include "types/logical_type.h"

namespace starrocks {
static bool should_use_view(LogicalType ltype) {
    switch (ltype) {
    case TYPE_ARRAY:
    case TYPE_STRUCT:
    case TYPE_MAP:
    case TYPE_JSON:
    case TYPE_VARIANT:
    case TYPE_VARCHAR:
    case TYPE_CHAR:
    case TYPE_BINARY:
    case TYPE_VARBINARY:
        return true;
    default:
        return false;
    }
    return false;
}
std::optional<MutableColumnPtr> ColumnViewHelper::create_column_view(const TypeDescriptor& type_desc, bool nullable,
                                                                     long concat_rows_limit, long concat_bytes_limit) {
    if (!should_use_view(type_desc.type)) {
        return {};
    }
    ColumnPtr default_column = ColumnHelper::create_column(type_desc, nullable);
    if (default_column->is_nullable()) {
        down_cast<NullableColumn*>(default_column.get())->append_default_not_null_value();
    } else {
        default_column->append_default();
    }
    return ColumnView::create(std::move(default_column), concat_rows_limit, concat_bytes_limit);
}
} // namespace starrocks
