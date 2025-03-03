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

#include "column/type_traits.h"
#include "column/vectorized_fwd.h"

namespace starrocks {

/**
 * Wrap a column, support :
 *      1.column type auto conversion.
 *      2.unpack complex column.
 *      3.expand const column value.
 *
 * example like:
 *      ColumnViewer<TYPE_INT> c = ColumnViewer(IntColumn);
 *      ColumnViewer<TYPE_INT> c = ColumnViewer(ConstColumn<IntColumn>);
 *      ColumnViewer<TYPE_INT> c = ColumnViewer(NullableColumn<IntColumn>);
 * will produce function:
 *      int value(size_t row_idx);
 *      bool is_null(size_t row_idx);
 *
 * @tparam Type
 */

template <LogicalType Type>
class ColumnViewer {
public:
    static auto constexpr TYPE = Type;

    explicit ColumnViewer(const ColumnPtr& column);

    const RunTimeCppType<Type> value(const size_t idx) const { return _data[idx & _not_const_mask]; }

    const bool is_null(const size_t idx) const { return _null_data[idx & _null_mask]; }

    size_t size() const { return _column->size(); }

    const NullColumnPtr& null_column() const { return _null_column; };

    typename RunTimeColumnType<Type>::Ptr column() const { return _column; };

private:
    // column ptr
    typename RunTimeColumnType<Type>::Ptr _column;

    NullColumnPtr _null_column;

    // raw pointer
    const RunTimeCppType<Type>* _data;

    const NullColumn::ValueType* _null_data;

    const size_t _not_const_mask;
    const size_t _null_mask;
};

} // namespace starrocks
