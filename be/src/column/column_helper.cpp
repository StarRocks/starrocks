// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "column/column_helper.h"

#include <runtime/types.h>

#include "column/array_column.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/vectorized_fwd.h"
#include "gutil/casts.h"
#include "runtime/primitive_type.h"
#include "runtime/primitive_type_infra.h"
#include "simd/simd.h"
#include "util/date_func.h"
#include "util/percentile_value.h"
#include "util/phmap/phmap.h"

namespace starrocks::vectorized {

NullColumnPtr ColumnHelper::one_size_not_null_column = NullColumn::create(1, 0);

NullColumnPtr ColumnHelper::one_size_null_column = NullColumn::create(1, 1);

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

void ColumnHelper::or_two_filters(Column::Filter* __restrict filter, const uint8_t* __restrict selected) {
    or_two_filters(filter->size(), filter->data(), selected);
}

void ColumnHelper::or_two_filters(size_t count, uint8_t* __restrict data, const uint8_t* __restrict selected) {
    for (size_t i = 0; i < count; i++) {
        data[i] |= selected[i];
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

struct ColumnBuilder {
    template <PrimitiveType ptype>
    ColumnPtr operator()(const TypeDescriptor& type_desc, size_t size) {
        switch (ptype) {
        case INVALID_TYPE:
        case TYPE_NULL:
        case TYPE_BINARY:
        case TYPE_DECIMAL:
        case TYPE_STRUCT:
        case TYPE_ARRAY:
        case TYPE_MAP:
            LOG(FATAL) << "Unsupported column type" << ptype;
        case TYPE_DECIMAL32:
            return Decimal32Column::create(type_desc.precision, type_desc.scale, size);
        case TYPE_DECIMAL64:
            return Decimal64Column::create(type_desc.precision, type_desc.scale, size);
        case TYPE_DECIMAL128:
            return Decimal128Column::create(type_desc.precision, type_desc.scale, size);
        default:
            return RunTimeColumnType<ptype>::create(size);
        }
    }
};

ColumnPtr ColumnHelper::create_column(const TypeDescriptor& type_desc, bool nullable, bool is_const, size_t size) {
    auto type = type_desc.type;
    if (is_const && (nullable || type == TYPE_NULL)) {
        return ColumnHelper::create_const_null_column(size);
    } else if (type == TYPE_NULL) {
        return NullableColumn::create(BooleanColumn::create(size), NullColumn::create(size, DATUM_NULL));
    }

    ColumnPtr p;
    if (type_desc.type == TYPE_ARRAY) {
        auto offsets = UInt32Column::create(size);
        auto data = create_column(type_desc.children[0], true, is_const, size);
        p = ArrayColumn::create(std::move(data), std::move(offsets));
    } else if (type_desc.type == TYPE_MAP) {
        auto offsets = UInt32Column ::create(size);
        auto keys = create_column(type_desc.children[0], true, is_const, size);
        auto values = create_column(type_desc.children[1], true, is_const, size);
        p = MapColumn::create(std::move(keys), std::move(values), std::move(offsets));
    } else {
        p = type_dispatch_column(type_desc.type, ColumnBuilder(), type_desc, size);
    }

    if (is_const) {
        return ConstColumn::create(p, size);
    }
    if (nullable) {
        // Default value is null
        return NullableColumn::create(p, NullColumn::create(size, DATUM_NULL));
    }
    return p;
}

bool ColumnHelper::is_all_const(const Columns& columns) {
    return std::all_of(std::begin(columns), std::end(columns), [](const ColumnPtr& col) { return col->is_constant(); });
}

std::pair<bool, size_t> ColumnHelper::num_packed_rows(const Columns& columns) {
    if (columns.empty()) {
        return {false, 0};
    }

    bool all_const = is_all_const(columns);
    if (!all_const) {
        return {all_const, columns[0]->size()};
    }

    return {all_const, 1};
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

ColumnPtr ColumnHelper::convert_time_column_from_double_to_str(const ColumnPtr& column) {
    auto get_binary_column = [](DoubleColumn* data_column, size_t size) -> ColumnPtr {
        auto new_data_column = BinaryColumn::create();
        new_data_column->reserve(size);

        for (int row = 0; row < size; ++row) {
            auto time = data_column->get_data()[row];
            std::string time_str = time_str_from_double(time);
            new_data_column->append(time_str);
        }

        return new_data_column;
    };

    ColumnPtr res;

    if (column->only_null()) {
        res = column;
    } else if (column->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(column.get());
        auto* data_column = down_cast<DoubleColumn*>(nullable_column->mutable_data_column());
        res = NullableColumn::create(get_binary_column(data_column, column->size()), nullable_column->null_column());
    } else if (column->is_constant()) {
        auto* const_column = down_cast<vectorized::ConstColumn*>(column.get());
        string time_str = time_str_from_double(const_column->get(0).get_double());
        res = vectorized::ColumnHelper::create_const_column<TYPE_VARCHAR>(time_str, column->size());
    } else {
        auto* data_column = down_cast<DoubleColumn*>(column.get());
        res = get_binary_column(data_column, column->size());
    }

    return res;
}

bool ChunkSlice::empty() const {
    return !chunk || offset == chunk->num_rows();
}

size_t ChunkSlice::rows() const {
    return chunk->num_rows() - offset;
}

void ChunkSlice::reset(vectorized::ChunkUniquePtr input) {
    chunk = std::move(input);
}

size_t ChunkSlice::skip(size_t skip_rows) {
    size_t real_skipped = std::min(rows(), skip_rows);
    offset += real_skipped;
    if (empty()) {
        chunk.reset();
        offset = 0;
    }

    return real_skipped;
}

// Cutoff required rows from this chunk
vectorized::ChunkPtr ChunkSlice::cutoff(size_t required_rows) {
    DCHECK(!empty());
    size_t cut_rows = std::min(rows(), required_rows);
    auto res = chunk->clone_empty(cut_rows);
    res->append(*chunk, offset, cut_rows);
    offset += cut_rows;
    if (empty()) {
        chunk.reset();
        offset = 0;
    }
    return res;
}
} // namespace starrocks::vectorized
