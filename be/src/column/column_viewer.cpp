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

#include "column/column_viewer.h"

#include "column/column_helper.h"
#include "types/logical_type_infra.h"
#include "util/percentile_value.h"
#include "util/phmap/phmap.h"

namespace starrocks {

static inline size_t not_const_mask(const ColumnPtr& column) {
    return !column->only_null() && !column->is_constant() ? -1 : 0;
}

static inline size_t null_mask(const ColumnPtr& column) {
    return !column->only_null() && !column->is_constant() && column->is_nullable() ? -1 : 0;
}

template <LogicalType Type>
ColumnViewer<Type>::ColumnViewer(const ColumnPtr& column)
        : _not_const_mask(not_const_mask(column)), _null_mask(null_mask(column)) {
    if (column->only_null()) {
        _null_column = ColumnHelper::one_size_null_column;
        _column = RunTimeColumnType<Type>::create();
        _column->append_default();
    } else if (column->is_constant()) {
        auto v = ColumnHelper::as_raw_column<ConstColumn>(column);
        _column = ColumnHelper::cast_to<Type>(v->data_column());
        _null_column = ColumnHelper::one_size_not_null_column;
    } else if (column->is_nullable()) {
        auto v = ColumnHelper::as_raw_column<NullableColumn>(column);
        _column = ColumnHelper::cast_to<Type>(v->data_column());
        _null_column = ColumnHelper::as_column<NullColumn>(v->null_column());
    } else {
        _column = ColumnHelper::cast_to<Type>(column);
        _null_column = ColumnHelper::one_size_not_null_column;
    }

    _data = _column->get_data().data();
    _null_data = _null_column->get_data().data();
}

#define M(TYPE) template class ColumnViewer<TYPE>;

APPLY_FOR_ALL_SCALAR_TYPE_WITH_NULL(M);
#undef M

template class ColumnViewer<TYPE_HLL>;
template class ColumnViewer<TYPE_OBJECT>;
template class ColumnViewer<TYPE_PERCENTILE>;

} // namespace starrocks
