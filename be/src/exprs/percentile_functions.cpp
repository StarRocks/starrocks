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

#include "exprs/percentile_functions.h"

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "gutil/strings/substitute.h"
#include "util/percentile_value.h"
#include "util/string_parser.hpp"

namespace starrocks {

StatusOr<ColumnPtr> PercentileFunctions::percentile_hash(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_DOUBLE> viewer(columns[0]);

    auto percentile_column = PercentileColumn::create();
    size_t size = columns[0]->size();
    for (int row = 0; row < size; ++row) {
        PercentileValue value;
        if (!viewer.is_null(row)) {
            value.add(viewer.value(row));
        }
        percentile_column->append(&value);
    }

    if (ColumnHelper::is_all_const(columns)) {
        return ConstColumn::create(percentile_column, columns[0]->size());
    } else {
        return percentile_column;
    }
}

StatusOr<ColumnPtr> PercentileFunctions::percentile_empty(FunctionContext* context, const Columns& columns) {
    PercentileValue value;
    return ColumnHelper::create_const_column<TYPE_PERCENTILE>(&value, 1);
}

StatusOr<ColumnPtr> PercentileFunctions::percentile_approx_raw(FunctionContext* context, const Columns& columns) {
    ColumnViewer<TYPE_PERCENTILE> viewer1(columns[0]);
    ColumnViewer<TYPE_DOUBLE> viewer2(columns[1]);
    size_t size = columns[0]->size();
    ColumnBuilder<TYPE_DOUBLE> builder(size);
    for (int row = 0; row < size; ++row) {
        if (viewer1.is_null(row) || viewer2.is_null(row)) {
            builder.append_null();
        } else {
            double result = viewer1.value(row)->quantile(viewer2.value(row));
            builder.append(result);
        }
    }
    return builder.build(columns[0]->is_constant());
}

} // namespace starrocks
