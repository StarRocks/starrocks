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

#include "column/column_helper.h"

#include <runtime/types.h>

#include "column/adaptive_nullable_column.h"
#include "column/array_column.h"
#include "column/chunk.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "gutil/casts.h"
#include "simd/simd.h"
#include "storage/chunk_helper.h"
#include "types/logical_type_infra.h"
#include "util/date_func.h"
#include "util/percentile_value.h"
#include "util/phmap/phmap.h"

namespace starrocks {

Filter& ColumnHelper::merge_nullable_filter(Column* column) {
    if (column->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(column);
        auto nulls = nullable_column->null_column_data().data();
        auto& sel_vec = (down_cast<UInt8Column*>(nullable_column->mutable_data_column()))->get_data();
        // NOTE(zc): Must use uint8_t* to enable auto-vectorized.
        auto selected = sel_vec.data();
        size_t num_rows = sel_vec.size();
        // we treat null(1) as false(0)
        for (size_t i = 0; i < num_rows; ++i) {
            selected[i] = static_cast<uint8_t>(selected[i] & !nulls[i]);
        }
        return sel_vec;
    } else {
        return (down_cast<UInt8Column*>(column))->get_data();
    }
}

void ColumnHelper::merge_two_filters(const ColumnPtr& column, Filter* __restrict filter, bool* all_zero) {
    if (column->is_nullable()) {
        const auto* nullable_column = as_raw_const_column<NullableColumn>(column);

        // NOTE(zc): Must use uint8_t* to enable auto-vectorized.
        const auto nulls = nullable_column->null_column_data().data();
        const auto datas = (down_cast<const UInt8Column*>(nullable_column->data_column().get()))->get_data().data();
        auto num_rows = nullable_column->size();
        // we treat null(1) as false(0)
        for (size_t j = 0; j < num_rows; ++j) {
            (*filter)[j] = static_cast<uint8_t>((*filter)[j] & (!nulls[j]) & datas[j]);
        }
    } else {
        size_t num_rows = column->size();
        const auto datas = as_raw_const_column<UInt8Column>(column)->get_data().data();
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

void ColumnHelper::merge_filters(const Columns& columns, Filter* __restrict filter) {
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

void ColumnHelper::merge_two_filters(Filter* __restrict filter, const uint8_t* __restrict selected, bool* all_zero) {
    uint8_t* data = filter->data();
    size_t num_rows = filter->size();
    for (size_t i = 0; i < num_rows; i++) {
        data[i] = data[i] & selected[i];
    }
    if (all_zero != nullptr) {
        *all_zero = (memchr(filter->data(), 0x1, num_rows) == nullptr);
    }
}

void ColumnHelper::or_two_filters(Filter* __restrict filter, const uint8_t* __restrict selected) {
    or_two_filters(filter->size(), filter->data(), selected);
}

void ColumnHelper::or_two_filters(size_t count, uint8_t* __restrict data, const uint8_t* __restrict selected) {
    for (size_t i = 0; i < count; i++) {
        data[i] |= selected[i];
    }
}

size_t ColumnHelper::count_nulls(const starrocks::ColumnPtr& col) {
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

size_t ColumnHelper::count_true_with_notnull(const starrocks::ColumnPtr& col) {
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

        size_t null_count = SIMD::count_nonzero(null_data);
        size_t true_count = SIMD::count_nonzero(bool_data);

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

size_t ColumnHelper::count_false_with_notnull(const starrocks::ColumnPtr& col) {
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

        size_t null_count = SIMD::count_nonzero(null_data);
        size_t false_count = SIMD::count_zero(bool_data);

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

MutableColumnPtr ColumnHelper::create_const_null_column(size_t chunk_size) {
    auto nullable_column = NullableColumn::create(Int8Column::create(), NullColumn::create());
    nullable_column->append_nulls(1);
    return ConstColumn::create(std::move(nullable_column), chunk_size);
}

size_t ColumnHelper::find_nonnull(const Column* col, size_t start, size_t end) {
    DCHECK_LE(start, end);

    if (!col->has_null()) {
        return start;
    }
    auto& null = as_raw_column<NullableColumn>(col)->immutable_null_column_data();
    return SIMD::find_zero(null, start, end - start);
}

size_t ColumnHelper::last_nonnull(const Column* col, size_t start, size_t end) {
    DCHECK_LE(start, end);
    DCHECK_LE(end, col->size());

    if (!col->has_null()) {
        return end - 1;
    }

    if (start == end) {
        return end;
    }

    auto& null = as_raw_column<NullableColumn>(col)->immutable_null_column_data();
    for (size_t i = end - 1;;) {
        if (null[i] == 0) {
            return i;
        }
        if (i == start) {
            break;
        }
        i--;
    }
    return end;
}

int64_t ColumnHelper::find_first_not_equal(const Column* column, int64_t target, int64_t start, int64_t end) {
    while (start + 1 < end) {
        int64_t mid = start + (end - start) / 2;
        if (column->compare_at(target, mid, *column, 1) == 0) {
            start = mid;
        } else {
            end = mid;
        }
    }
    if (column->compare_at(target, end - 1, *column, 1) == 0) {
        return end;
    }
    return end - 1;
}

// expression trees' return column should align return type when some return columns maybe diff from the required
// return type, as well the null flag. e.g., concat_ws returns col from create_const_null_column(), it's type is
// Nullable(int8), but required return type is nullable(string), so col need align return type to nullable(string).
ColumnPtr ColumnHelper::align_return_type(ColumnPtr&& old_col, const TypeDescriptor& type_desc, size_t num_rows,
                                          const bool is_nullable) {
    MutableColumnPtr new_column = (std::move(*old_col)).mutate();
    if (old_col->only_null()) {
        new_column = ColumnHelper::create_column(type_desc, true);
        new_column->append_nulls(num_rows);
    } else if (old_col->is_constant()) {
        // Note: we must create a new column every time here,
        // because result_columns[i] is shared_ptr
        new_column = ColumnHelper::create_column(type_desc, false);
        auto* const_column = down_cast<const ConstColumn*>(old_col.get());
        new_column->append(*const_column->data_column(), 0, 1);
        new_column->assign(num_rows, 0);
    }
    if (is_nullable && !new_column->is_nullable()) {
        new_column = NullableColumn::create(std::move(new_column), NullColumn::create(new_column->size(), 0));
    }
    return new_column;
}

MutableColumnPtr ColumnHelper::create_column(const TypeDescriptor& type_desc, bool nullable) {
    return create_column(type_desc, nullable, false, 0);
}

struct ColumnBuilder {
    template <LogicalType ltype>
    MutableColumnPtr operator()(const TypeDescriptor& type_desc, size_t size) {
        switch (ltype) {
        case TYPE_UNKNOWN:
        case TYPE_NULL:
        case TYPE_BINARY:
        case TYPE_DECIMAL:
        case TYPE_STRUCT:
        case TYPE_ARRAY:
        case TYPE_MAP:
            LOG(FATAL) << "Unsupported column type" << ltype;
        case TYPE_DECIMAL32:
            return Decimal32Column::create(type_desc.precision, type_desc.scale, size);
        case TYPE_DECIMAL64:
            return Decimal64Column::create(type_desc.precision, type_desc.scale, size);
        case TYPE_DECIMAL128:
            return Decimal128Column::create(type_desc.precision, type_desc.scale, size);
        default:
            return RunTimeColumnType<ltype>::create(size);
        }
    }
};

MutableColumnPtr ColumnHelper::create_column(const TypeDescriptor& type_desc, bool nullable, bool is_const, size_t size,
                                             bool use_adaptive_nullable_column) {
    auto type = type_desc.type;
    if (is_const && (nullable || type == TYPE_NULL)) {
        return ColumnHelper::create_const_null_column(size);
    } else if (type == TYPE_NULL) {
        if (use_adaptive_nullable_column) {
            return AdaptiveNullableColumn::create(BooleanColumn::create(size), NullColumn::create(size, DATUM_NULL));
        } else {
            return NullableColumn::create(BooleanColumn::create(size), NullColumn::create(size, DATUM_NULL));
        }
    }

    MutableColumnPtr p;
    if (type_desc.type == LogicalType::TYPE_ARRAY) {
        auto offsets = UInt32Column::create(size);
        auto data = create_column(type_desc.children[0], true, is_const, size);
        p = ArrayColumn::create(std::move(data), std::move(offsets));
    } else if (type_desc.type == LogicalType::TYPE_MAP) {
        MutableColumnPtr offsets = UInt32Column ::create(size);
        MutableColumnPtr keys = nullptr;
        MutableColumnPtr values = nullptr;
        if (type_desc.children[0].is_unknown_type()) {
            keys = create_column(TypeDescriptor{TYPE_NULL}, true, is_const, size);
        } else {
            keys = create_column(type_desc.children[0], true, is_const, size);
        }
        if (type_desc.children[1].is_unknown_type()) {
            values = create_column(TypeDescriptor{TYPE_NULL}, true, is_const, size);
        } else {
            values = create_column(type_desc.children[1], true, is_const, size);
        }
        p = MapColumn::create(std::move(keys), std::move(values), std::move(offsets));
    } else if (type_desc.type == LogicalType::TYPE_STRUCT) {
        size_t field_size = type_desc.children.size();
        DCHECK_EQ(field_size, type_desc.field_names.size());
        MutableColumns columns;
        for (size_t i = 0; i < field_size; i++) {
            auto field_column = create_column(type_desc.children[i], true, is_const, size);
            columns.emplace_back(std::move(field_column));
        }
        p = StructColumn::create(std::move(columns), type_desc.field_names);
    } else {
        p = type_dispatch_column(type_desc.type, ColumnBuilder(), type_desc, size);
    }

    if (is_const) {
        return ConstColumn::create(std::move(p), size);
    }
    if (nullable) {
        if (use_adaptive_nullable_column) {
            return AdaptiveNullableColumn::create(std::move(p), NullColumn::create(size, DATUM_NULL));
        } else {
            return NullableColumn::create(std::move(p), NullColumn::create(size, DATUM_NULL));
        }
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

std::pair<bool, size_t> ColumnHelper::num_packed_rows(const Column* column) {
    if (column->is_constant()) {
        return {true, 1};
    }
    return {false, column->size()};
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
    auto get_binary_column = [](const DoubleColumn* data_column, size_t size) -> MutableColumnPtr {
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
        res = std::move(column);
    } else if (column->is_nullable()) {
        auto* nullable_column = down_cast<const NullableColumn*>(column.get());
        auto* data_column = down_cast<const DoubleColumn*>(nullable_column->data_column().get());
        res = NullableColumn::create(get_binary_column(data_column, column->size()),
                                     nullable_column->null_column()->as_mutable_ptr());
    } else if (column->is_constant()) {
        auto* const_column = down_cast<const ConstColumn*>(column.get());
        std::string time_str = time_str_from_double(const_column->get(0).get_double());
        res = ColumnHelper::create_const_column<TYPE_VARCHAR>(time_str, column->size());
    } else {
        auto* data_column = down_cast<const DoubleColumn*>(column.get());
        res = get_binary_column(data_column, column->size());
    }

    return res;
}

std::tuple<UInt32Column::Ptr, ColumnPtr, NullColumnPtr> ColumnHelper::unpack_array_column(const ColumnPtr& column) {
    DCHECK(!column->is_nullable() && !column->is_constant());
    DCHECK(column->is_array());

    const ArrayColumn* array_column = down_cast<const ArrayColumn*>(column.get());
    auto elements_column = down_cast<const NullableColumn*>(array_column->elements_column().get())->data_column();
    auto null_column = down_cast<const NullableColumn*>(array_column->elements_column().get())->null_column();
    auto offsets_column = array_column->offsets_column();
    return {offsets_column, elements_column, null_column};
}

template <class Ptr>
bool ChunkSliceTemplate<Ptr>::empty() const {
    return !chunk || offset == chunk->num_rows();
}

template <class Ptr>
size_t ChunkSliceTemplate<Ptr>::rows() const {
    return chunk->num_rows() - offset;
}

template <class Ptr>
void ChunkSliceTemplate<Ptr>::reset(Ptr input) {
    chunk = std::move(input);
}

template <class Ptr>
size_t ChunkSliceTemplate<Ptr>::skip(size_t skip_rows) {
    size_t real_skipped = std::min(rows(), skip_rows);
    offset += real_skipped;
    if (empty()) {
        chunk.reset();
        offset = 0;
    }

    return real_skipped;
}

// Cutoff required rows from this chunk
template <class Ptr>
ChunkUniquePtr ChunkSliceTemplate<Ptr>::cutoff(size_t required_rows) {
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

// Specialized for SegmentedChunkPtr
template <>
ChunkUniquePtr ChunkSliceTemplate<SegmentedChunkPtr>::cutoff(size_t required_rows) {
    DCHECK(!empty());
    // cutoff a chunk from current segment, if it doesn't meet the requirement just let it be
    ChunkPtr segment = chunk->segments()[segment_id];
    size_t segment_offset = offset % chunk->segment_size();
    size_t cut_rows = std::min(segment->num_rows() - segment_offset, required_rows);

    auto res = segment->clone_empty(cut_rows);
    res->append(*segment, segment_offset, cut_rows);
    offset += cut_rows;

    // move to next segment
    segment_id = offset / chunk->segment_size();

    if (empty()) {
        chunk->reset();
        offset = 0;
    }
    return res;
}

template struct ChunkSliceTemplate<ChunkPtr>;
template struct ChunkSliceTemplate<ChunkUniquePtr>;
template struct ChunkSliceTemplate<SegmentedChunkPtr>;

} // namespace starrocks
