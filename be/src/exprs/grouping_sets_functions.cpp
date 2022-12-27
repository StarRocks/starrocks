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

#include "exprs/grouping_sets_functions.h"

#include "column/column_helper.h"
#include "column/column_viewer.h"

namespace starrocks {

StatusOr<ColumnPtr> GroupingSetsFunctions::grouping_id(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 1);
    return columns[0];
}

StatusOr<ColumnPtr> GroupingSetsFunctions::grouping(FunctionContext* context, const Columns& columns) {
    DCHECK_EQ(columns.size(), 1);
    return columns[0];
}

} // namespace starrocks
