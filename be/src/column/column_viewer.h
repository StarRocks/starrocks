// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "column/column_helper.h"

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

static inline size_t not_const_mask(const ColumnPtr& column) {
    return !column->only_null() && !column->is_constant() ? -1 : 0;
}

static inline size_t null_mask(const ColumnPtr& column) {
    return !column->only_null() && !column->is_constant() && column->is_nullable() ? -1 : 0;
}

template <PrimitiveType Type>
class ColumnViewer {
public:
    static auto constexpr TYPE = Type;
    explicit ColumnViewer(const ColumnPtr& column)
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
