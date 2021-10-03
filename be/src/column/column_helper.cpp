// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "column/column_helper.h"

#include <runtime/types.h>

#include "column/array_column.h"
#include "common/config.h"
#include "gutil/casts.h"
#include "runtime/types.h"
#include "simd/simd.h"

namespace starrocks::vectorized {

NullColumnPtr ColumnHelper::one_size_not_null_column = NullColumn::create(1, 0);

NullColumnPtr ColumnHelper::one_size_null_column = NullColumn::create(1, 1);

NullColumnPtr ColumnHelper::s_all_not_null_column = nullptr;

void ColumnHelper::init_static_variable() {
    ColumnHelper::s_all_not_null_column = NullColumn::create(config::vector_chunk_size, 0);
}

Column::Filter& ColumnHelper::merge_nullable_filter(Column* column) {
    if (column->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(column);
        auto nulls = nullable_column->null_column_data().data();
        auto& sel_vec = (down_cast<UInt8Column*>(nullable_column->mutable_data_column()))->get_data();
        // NOTE(zc): Must use uint8_t* to enable auto-vectorized.
        auto selected = sel_vec.data();
        size_t num_rows = sel_vec.size();
        // we treat null(1) as false(0)
        for (size_t i = 0; i < num_rows; ++i) {
            selected[i] &= !nulls[i];
        }
        return sel_vec;
    } else {
        return (down_cast<UInt8Column*>(column))->get_data();
    }
}

void ColumnHelper::merge_two_filters(const ColumnPtr& column, Column::Filter* __restrict filter, bool* all_zero) {
    if (column->is_nullable()) {
        auto* nullable_column = as_raw_column<NullableColumn>(column);

        // NOTE(zc): Must use uint8_t* to enable auto-vectorized.
        auto nulls = nullable_column->null_column_data().data();
        auto datas = (down_cast<UInt8Column*>(nullable_column->mutable_data_column()))->get_data().data();
        auto num_rows = nullable_column->size();
        // we treat null(1) as false(0)
        for (size_t j = 0; j < num_rows; ++j) {
            (*filter)[j] &= (!nulls[j]) & datas[j];
        }
    } else {
        size_t num_rows = column->size();
        auto datas = as_raw_column<UInt8Column>(column)->get_data().data();
        for (size_t j = 0; j < num_rows; ++j) {
            (*filter)[j] &= datas[j];
        }
    }

    if (all_zero != nullptr) {
        // filter has just been updated, cache locality is good here.
        // noted that here we don't need to count zero, but to check is there any non-zero.
        // filter values are 0/1, we can use memchr here.
        *all_zero = (memchr(filter->data(), 0x1, filter->size()) == nullptr);
    }
}

void ColumnHelper::merge_filters(const Columns& columns, Column::Filter* __restrict filter) {
    DCHECK_GT(columns.size(), 0);

    // All filters must be the same length, there is no const filter
    for (const auto& column : columns) {
        bool all_zero = false;
        merge_two_filters(column, filter, &all_zero);
        if (all_zero) {
            break;
        }
    }
}

void ColumnHelper::merge_two_filters(Column::Filter* __restrict filter, const uint8_t* __restrict selected,
                                     bool* all_zero) {
    uint8_t* data = filter->data();
    size_t num_rows = filter->size();
    for (size_t i = 0; i < num_rows; i++) {
        data[i] = data[i] & selected[i];
    }
    if (all_zero != nullptr) {
        *all_zero = (memchr(filter->data(), 0x1, num_rows) == nullptr);
    }
}

size_t ColumnHelper::count_nulls(const starrocks::vectorized::ColumnPtr& col) {
    if (!col->is_nullable()) {
        return 0;
    }

    if (col->only_null()) {
        return col->size();
    }

    const Buffer<uint8_t>& null_data = as_raw_column<NullableColumn>(col)->null_column_data();
    // @Warn: be careful, should rewrite the code if NullColumn type changed!
    return SIMD::count_nonzero(null_data);
}

size_t ColumnHelper::count_true_with_notnull(const starrocks::vectorized::ColumnPtr& col) {
    if (col->only_null()) {
        return 0;
    }

    if (col->is_constant()) {
        bool is_true = ColumnHelper::get_const_value<TYPE_BOOLEAN>(col);
        return is_true ? col->size() : 0;
    }

    if (col->is_nullable()) {
        auto tmp = ColumnHelper::as_raw_column<NullableColumn>(col);
        const Buffer<uint8_t>& null_data = tmp->null_column_data();
        const Buffer<uint8_t>& bool_data = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(tmp->data_column())->get_data();

        int null_count = SIMD::count_nonzero(null_data);
        int true_count = SIMD::count_nonzero(bool_data);

        if (null_count == col->size()) {
            return 0;
        } else if (null_count == 0) {
            return true_count;
        } else {
            // In fact, the null_count maybe is different with true_count, but it's no impact
            return null_count;
        }
    } else {
        const Buffer<uint8_t>& bool_data = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(col)->get_data();
        return SIMD::count_nonzero(bool_data);
    }
}

size_t ColumnHelper::count_false_with_notnull(const starrocks::vectorized::ColumnPtr& col) {
    if (col->only_null()) {
        return 0;
    }

    if (col->is_constant()) {
        bool is_true = ColumnHelper::get_const_value<TYPE_BOOLEAN>(col);
        return is_true ? 0 : col->size();
    }

    if (col->is_nullable()) {
        auto tmp = ColumnHelper::as_raw_column<NullableColumn>(col);
        const Buffer<uint8_t>& null_data = tmp->null_column_data();
        const Buffer<uint8_t>& bool_data = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(tmp->data_column())->get_data();

        int null_count = SIMD::count_nonzero(null_data);
        int false_count = SIMD::count_zero(bool_data);

        if (null_count == col->size()) {
            return 0;
        } else if (null_count == 0) {
            return false_count;
        } else {
            // In fact, the null_count maybe is different with false_count, but it's no impact
            return null_count;
        }
    } else {
        const Buffer<uint8_t>& bool_data = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(col)->get_data();
        return SIMD::count_zero(bool_data);
    }
}

ColumnPtr ColumnHelper::create_const_null_column(size_t chunk_size) {
    auto nullable_column = NullableColumn::create(Int8Column::create(), NullColumn::create());
    nullable_column->append_nulls(1);
    return ConstColumn::create(nullable_column, chunk_size);
}

ColumnPtr ColumnHelper::create_column(const TypeDescriptor& type_desc, bool nullable) {
    return create_column(type_desc, nullable, false, 0);
}

ColumnPtr ColumnHelper::create_column(const TypeDescriptor& type_desc, bool nullable, bool is_const, size_t size) {
    auto type = type_desc.type;
    if (VLOG_ROW_IS_ON) {
        VLOG_ROW << "PrimitiveType " << type << " nullable " << nullable << " is_const " << is_const;
    }
    if (nullable && is_const) {
        return ColumnHelper::create_const_null_column(size);
    }

    if (type == TYPE_NULL) {
        if (is_const) {
            return ColumnHelper::create_const_null_column(size);
        } else if (nullable) {
            return NullableColumn::create(BooleanColumn::create(), NullColumn::create());
        }
    }

    ColumnPtr p;
    switch (type_desc.type) {
    case TYPE_BOOLEAN:
        p = BooleanColumn::create();
        break;
    case TYPE_TINYINT:
        p = Int8Column::create();
        break;
    case TYPE_SMALLINT:
        p = Int16Column::create();
        break;
    case TYPE_INT:
        p = Int32Column::create();
        break;
    case TYPE_BIGINT:
        p = Int64Column::create();
        break;
    case TYPE_LARGEINT:
        p = Int128Column::create();
        break;
    case TYPE_FLOAT:
        p = FloatColumn::create();
        break;
    case TYPE_DOUBLE:
        p = DoubleColumn::create();
        break;
    case TYPE_DECIMALV2:
        p = DecimalColumn::create();
        break;
    case TYPE_DATE:
        p = DateColumn::create();
        break;
    case TYPE_DATETIME:
        p = TimestampColumn::create();
        break;
    case TYPE_TIME:
        p = DoubleColumn::create();
        break;
    case TYPE_VARCHAR:
    case TYPE_CHAR:
        p = BinaryColumn::create();
        break;
    case TYPE_HLL:
        p = HyperLogLogColumn::create();
        break;
    case TYPE_OBJECT:
        p = BitmapColumn::create();
        break;
    case TYPE_DECIMAL32: {
        p = Decimal32Column::create(type_desc.precision, type_desc.scale);
        break;
    }
    case TYPE_DECIMAL64: {
        p = Decimal64Column::create(type_desc.precision, type_desc.scale);
        break;
    }
    case TYPE_DECIMAL128: {
        p = Decimal128Column::create(type_desc.precision, type_desc.scale);
        break;
    }
    case TYPE_PERCENTILE:
        p = PercentileColumn ::create();
        break;
    case TYPE_ARRAY: {
        auto offsets = UInt32Column::create();
        auto data = create_column(type_desc.children[0], true);
        p = ArrayColumn::create(std::move(data), std::move(offsets));
        break;
    }
    case INVALID_TYPE:
    case TYPE_NULL:
    case TYPE_BINARY:
    case TYPE_DECIMAL:
    case TYPE_STRUCT:
    case TYPE_MAP:
        CHECK(false) << "unreachable path: " << type_desc.type;
        return nullptr;
    }

    if (is_const) {
        return ConstColumn::create(p);
    }
    if (nullable) {
        return NullableColumn::create(p, NullColumn::create());
    }
    return p;
}

bool ColumnHelper::is_all_const(const Columns& columns) {
    return std::all_of(std::begin(columns), std::end(columns), [](const ColumnPtr& col) { return col->is_constant(); });
}

using ColumnsConstIterator = Columns::const_iterator;
bool ColumnHelper::is_all_const(ColumnsConstIterator const& begin, ColumnsConstIterator const& end) {
    for (auto it = begin; it < end; ++it) {
        if (!(*it)->is_constant()) {
            return false;
        }
    }
    return true;
}

size_t ColumnHelper::compute_bytes_size(ColumnsConstIterator const& begin, ColumnsConstIterator const& end) {
    size_t n = 0;
    size_t row_num = (*begin)->size();
    for (auto it = begin; it < end; ++it) {
        ColumnPtr const& col = *it;
        // const null column is neglected
        if (col->only_null()) {
            continue;
        }
        auto binary = ColumnHelper::get_binary_column(col.get());
        if (col->is_constant()) {
            n += binary->get_bytes().size() * row_num;
        } else {
            n += binary->get_bytes().size();
        }
    }
    return n;
}

} // namespace starrocks::vectorized
