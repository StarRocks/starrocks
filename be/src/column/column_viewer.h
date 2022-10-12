// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "column/type_traits.h"
#include "column/vectorized_fwd.h"

namespace starrocks {
namespace vectorized {

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

template <PrimitiveType Type>
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
    RunTimeCppType<Type>* _data;

    NullColumn::ValueType* _null_data;

    const size_t _not_const_mask;
    const size_t _null_mask;
};

} // namespace vectorized
} // namespace starrocks
