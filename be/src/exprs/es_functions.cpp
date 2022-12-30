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

#include "exprs/es_functions.h"

#include "column/column_builder.h"
#include "column/column_viewer.h"

namespace starrocks {

StatusOr<ColumnPtr> ESFunctions::match(FunctionContext* context, const Columns& columns) {
    auto size = columns[0]->size();
    ColumnBuilder<TYPE_BOOLEAN> result(size);
    for (int row = 0; row < size; ++row) {
        result.append(true);
    }

    return result.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks
