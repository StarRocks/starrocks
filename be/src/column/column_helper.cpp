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

#ifdef __x86_64__
#include <immintrin.h>
#endif
#if defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_acle.h>
#include <arm_neon.h>
#endif

#include "base/simd/simd.h"
#include "column/adaptive_nullable_column.h"
#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_view/column_view_helper.h"
#include "column/column_visitor_adapter.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "gutil/casts.h"
#include "runtime/decimalv2_value.h"
#include "storage/chunk_helper.h"
#include "storage/decimal12.h"
#include "storage/uint24.h"
#include "types/date_value.h"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"
#include "types/timestamp_value.h"
#include "util/date_func.h"
#include "util/int96.h"
#include "util/phmap/phmap.h"

namespace starrocks {
Filter& ColumnHelper::merge_nullable_filter(Column* column) {
    if (column->is_nullable()) {
        auto* nullable_column = down_cast<NullableColumn*>(column);
        auto nulls = nullable_column->null_column_data().data();
        auto& sel_vec = (down_cast<UInt8Column*>(nullable_column->data_column_raw_ptr()))->get_data();
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
        const auto datas =
                (down_cast<const UInt8Column*>(nullable_column->data_column().get()))->immutable_data().data();
        auto num_rows = nullable_column->size();
        // we treat null(1) as false(0)
        for (size_t j = 0; j < num_rows; ++j) {
            (*filter)[j] = static_cast<uint8_t>((*filter)[j] & (!nulls[j]) & datas[j]);
        }
    } else {
        size_t num_rows = column->size();
        const auto datas = as_raw_const_column<UInt8Column>(column)->immutable_data().data();
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

void ColumnHelper::merge_two_anti_filters(const ColumnPtr& column, NullData& null_data, Filter* __restrict filter) {
    size_t num_rows = column->size();
    auto* data_column = get_data_column(column.get());

    if (column->is_nullable()) {
        const auto* nullable_column = as_raw_const_column<NullableColumn>(column);
        const auto nulls = nullable_column->null_column_data().data();
        for (size_t i = 0; i < num_rows; ++i) {
            null_data[i] |= nulls[i];
        }
    }

    const auto* datas = get_cpp_data<TYPE_BOOLEAN>(data_column);
    for (size_t j = 0; j < num_rows; ++j) {
        (*filter)[j] &= datas[j];
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

    const ImmutableNullData null_data = as_raw_column<NullableColumn>(col)->null_column_data();
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
        const ImmutableNullData null_data = static_cast<const NullableColumn*>(tmp)->null_column_data();
        const ImmutableNullData bool_data =
                ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(static_cast<const NullableColumn*>(tmp)->data_column())
                        ->immutable_data();

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
        const ImmutableNullData bool_data = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(col)->immutable_data();
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
        const ImmutableNullData null_data = static_cast<const NullableColumn*>(tmp)->null_column_data();
        const ImmutableNullData bool_data =
                ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(static_cast<const NullableColumn*>(tmp)->data_column())
                        ->get_data();

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
        const ImmutableNullData bool_data = ColumnHelper::cast_to_raw<TYPE_BOOLEAN>(col)->get_data();
        return SIMD::count_zero(bool_data);
    }
}

MutableColumnPtr ColumnHelper::create_const_null_column(size_t chunk_size) {
    auto nullable_column = NullableColumn::create(Int8Column::create(), NullColumn::create());
    nullable_column->append_nulls(1);
    return ConstColumn::create(std::move(nullable_column), chunk_size);
}

class UpdateColumnNullInfoVisitor : public ColumnVisitorMutableAdapter<UpdateColumnNullInfoVisitor> {
public:
    UpdateColumnNullInfoVisitor() : ColumnVisitorMutableAdapter<UpdateColumnNullInfoVisitor>(this) {}

    Status do_visit(ConstColumn* column) {
        return Status::NotSupported("Unsupported const column in column wise comparator");
    }

    template <typename T>
    Status do_visit(ObjectColumn<T>* column) {
        return Status::NotSupported("Unsupported object column in column wise comparator");
    }

    Status do_visit(NullableColumn* column) {
        auto* data_col = column->data_column_raw_ptr();
        RETURN_IF_ERROR(data_col->accept_mutable(this));
        column->update_has_null();
        return Status::OK();
    }

    Status do_visit(ArrayColumn* column) {
        auto* elements_col = column->elements_column_raw_ptr();
        RETURN_IF_ERROR(elements_col->accept_mutable(this));
        return Status::OK();
    }

    Status do_visit(MapColumn* column) {
        auto* keys_col = column->keys_column_raw_ptr();
        auto* values_col = column->values_column_raw_ptr();
        RETURN_IF_ERROR(keys_col->accept_mutable(this));
        RETURN_IF_ERROR(values_col->accept_mutable(this));
        return Status::OK();
    }

    Status do_visit(StructColumn* column) {
        return Status::NotSupported("Unsupported struct column in column wise comparator");
    }

    template <typename T>
    Status do_visit(FixedLengthColumnBase<T>* column) {
        return Status::OK();
    }
    template <typename T>
    Status do_visit(BinaryColumnBase<T>* column) {
        return Status::OK();
    }
};

Status ColumnHelper::update_nested_has_null(Column* column) {
    UpdateColumnNullInfoVisitor visitor;
    RETURN_IF_ERROR(column->accept_mutable(&visitor));
    return Status::OK();
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
MutableColumnPtr ColumnHelper::align_return_type(MutableColumnPtr&& old_col, const TypeDescriptor& type_desc,
                                                 size_t num_rows, bool is_nullable) {
    MutableColumnPtr new_column;
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
    } else {
        new_column = std::move(old_col);
    }
    if (is_nullable && !new_column->is_nullable()) {
        new_column = NullableColumn::create(std::move(new_column), NullColumn::create(new_column->size(), 0));
    }
    return new_column;
}

MutableColumnPtr ColumnHelper::align_return_type(ColumnPtr&& old_col, const TypeDescriptor& type_desc, size_t num_rows,
                                                 bool is_nullable) {
    return align_return_type(Column::mutate(std::move(old_col)), type_desc, num_rows, is_nullable);
}

MutableColumnPtr ColumnHelper::create_column(const TypeDescriptor& type_desc, bool nullable) {
    return create_column(type_desc, nullable, false, 0);
}

MutableColumnPtr ColumnHelper::create_column(const TypeDescriptor& type_desc, bool nullable, bool use_view_if_needed,
                                             long column_view_concat_rows_limit, long column_view_concat_bytes_limit) {
    if (use_view_if_needed) {
        auto opt_column = ColumnViewHelper::create_column_view(type_desc, nullable, column_view_concat_rows_limit,
                                                               column_view_concat_bytes_limit);
        if (opt_column.has_value()) {
            return std::move(opt_column.value());
        }
    }
    return create_column(type_desc, nullable);
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
        case TYPE_DECIMAL256:
            return Decimal256Column::create(type_desc.precision, type_desc.scale, size);
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
        auto offsets = UInt32Column::create(size);
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
            n += binary->get_immutable_bytes().size() * row_num;
        } else {
            n += binary->get_immutable_bytes().size();
        }
    }
    return n;
}

ColumnPtr ColumnHelper::convert_time_column_from_double_to_str(const ColumnPtr& column) {
    auto get_binary_column = [](const DoubleColumn* data_column, size_t size) -> MutableColumnPtr {
        auto new_data_column = BinaryColumn::create();
        new_data_column->reserve(size);

        for (int row = 0; row < size; ++row) {
            auto time = data_column->immutable_data()[row];
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
                                     std::move(nullable_column->null_column()));
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

MutableColumns ColumnHelper::to_mutable_columns(const Columns& columns) {
    MutableColumns mutable_columns;
    mutable_columns.reserve(columns.size());
    for (auto& column : columns) {
        mutable_columns.emplace_back(std::move(*column).mutate());
    }
    return mutable_columns;
}

MutableColumns ColumnHelper::to_mutable_columns(Columns&& columns) {
    MutableColumns mutable_columns;
    mutable_columns.reserve(columns.size());
    for (auto& column : columns) {
        mutable_columns.emplace_back(Column::mutate(std::move(column)));
    }
    columns.clear();
    return mutable_columns;
}

Columns ColumnHelper::to_columns(MutableColumns&& columns) {
    Columns immutable_columns;
    immutable_columns.reserve(columns.size());
    for (auto& column : columns) {
        immutable_columns.emplace_back(std::move(column));
    }
    columns.clear();
    return immutable_columns;
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

template <typename T, bool avx512f>
size_t ColumnHelper::t_filter_range(const Filter& filter, T* dst_data, const T* src_data, size_t from, size_t to) {
    auto start_offset = from;
    auto result_offset = from;

#ifdef __AVX2__
    const uint8_t* f_data = filter.data();
    constexpr size_t data_type_size = sizeof(T);

    constexpr size_t kBatchNums = 256 / (8 * sizeof(uint8_t));
    const __m256i all0 = _mm256_setzero_si256();

    // batch nums is kBatchNums
    // we will process filter at start_offset, start_offset + 1, ..., start_offset + kBatchNums - 1 in one batch
    while (start_offset + kBatchNums <= to) {
        __m256i f = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(f_data + start_offset));
        uint32_t mask = _mm256_movemask_epi8(_mm256_cmpgt_epi8(f, all0));

        if (mask == 0) {
            // all no hit, pass
        } else if (mask == 0xffffffff) {
            // all hit, copy all
            memmove(dst_data + result_offset, src_data + start_offset, kBatchNums * data_type_size);
            result_offset += kBatchNums;

        } else {
            // clang-format off
#define AVX512_COPY(SHIFT, MASK, WIDTH)                                             \
    {                                                                               \
        auto m = (mask >> SHIFT) & MASK;                                            \
        if (m) {                                                                    \
            __m512i dst;                                                            \
            __m512i src = _mm512_loadu_epi##WIDTH(src_data + start_offset + SHIFT); \
            dst = _mm512_mask_compress_epi##WIDTH(dst, m, src);                     \
            _mm512_storeu_epi##WIDTH(dst_data + result_offset, dst);                \
            result_offset += __builtin_popcount(m);                                 \
        }                                                                           \
    }

// In theory we should put k1 in clobbers.
// But since we compile code with AVX2, k1 register is not used.
#define AVX512_ASM_COPY(SHIFT, MASK, WIDTH, WIDTHX)               \
    {                                                             \
        auto m = (mask >> SHIFT) & MASK;                          \
        if (m) {                                                  \
            const T* src = src_data + start_offset + SHIFT;             \
            T* dst = dst_data + result_offset;                    \
            __asm__ volatile("vmovdqu" #WIDTH                     \
                             " (%[s]), %%zmm1\n"                  \
                             "kmovw %[mask], %%k1\n"              \
                             "vpcompress" #WIDTHX                 \
                             " %%zmm1, %%zmm0%{%%k1%}%{z%}\n"     \
                             "vmovdqu" #WIDTH " %%zmm0, (%[d])\n" \
                             : [s] "+r"(src), [d] "+r"(dst)       \
                             : [mask] "r"(m)                      \
                             : "zmm0", "zmm1", "memory");         \
            result_offset += __builtin_popcount(m);               \
        }                                                         \
    }

            if constexpr (avx512f && sizeof(T) == 4) {
                AVX512_ASM_COPY(0, 0xffff, 32, d);
                AVX512_ASM_COPY(16, 0xffff, 32, d);
            } else {
                phmap::priv::BitMask<uint32_t, 32> bitmask(mask);
                for (auto idx : bitmask) {
                    *(dst_data + result_offset++) = *(src_data + start_offset + idx);
                }
            }
        }

        start_offset += kBatchNums;
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    const uint8_t* filter_data = filter.data() + from;
    constexpr size_t data_type_size = sizeof(T);

    constexpr size_t kBatchNums = 128 / (8 * sizeof(uint8_t));
    while (start_offset + kBatchNums < to) {
        const uint8x16_t vfilter = vld1q_u8(filter_data);
        // nibble_mask[i] != 0 ? 0xFF : 0x00
        uint64_t nibble_mask = SIMD::get_nibble_mask(vtstq_u8(vfilter, vfilter));
        if (nibble_mask == 0) {
            // skip
        } else if (nibble_mask == 0xffff'ffff'ffff'ffffull) {
            memmove(dst_data + result_offset, src_data + start_offset, kBatchNums * data_type_size);
            result_offset += kBatchNums;
        } else {
            // Make each nibble only keep the highest bit 1, that is 0b1111 -> 0b1000.
            nibble_mask &= 0x8888'8888'8888'8888ull;
            for (; nibble_mask > 0; nibble_mask &= nibble_mask - 1) {
                uint32_t index = __builtin_ctzll(nibble_mask) >> 2;
                *(dst_data + result_offset++) = *(src_data + start_offset + index);
            }
        }

        start_offset += kBatchNums;
        filter_data += kBatchNums;
    }
#endif
    // clang-format on
    for (auto i = start_offset; i < to; ++i) {
        if (filter[i]) {
            *(dst_data + result_offset) = *(src_data + i);
            result_offset++;
        }
    }

    return result_offset;
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

// Explicit instantiation for t_filter_range
#define INSTANTIATE_T_FILTER_RANGE(T)                                                                   \
    template size_t ColumnHelper::t_filter_range<T, true>(const Filter&, T*, const T*, size_t, size_t); \
    template size_t ColumnHelper::t_filter_range<T, false>(const Filter&, T*, const T*, size_t, size_t);

INSTANTIATE_T_FILTER_RANGE(int8_t)
INSTANTIATE_T_FILTER_RANGE(uint8_t)
INSTANTIATE_T_FILTER_RANGE(int16_t)
INSTANTIATE_T_FILTER_RANGE(uint16_t)
INSTANTIATE_T_FILTER_RANGE(int32_t)
INSTANTIATE_T_FILTER_RANGE(uint32_t)
INSTANTIATE_T_FILTER_RANGE(int64_t)
INSTANTIATE_T_FILTER_RANGE(uint64_t)
INSTANTIATE_T_FILTER_RANGE(int128_t)
INSTANTIATE_T_FILTER_RANGE(int256_t)
INSTANTIATE_T_FILTER_RANGE(float)
INSTANTIATE_T_FILTER_RANGE(double)
INSTANTIATE_T_FILTER_RANGE(uint24_t)
INSTANTIATE_T_FILTER_RANGE(int96_t)
INSTANTIATE_T_FILTER_RANGE(decimal12_t)
INSTANTIATE_T_FILTER_RANGE(DateValue)
INSTANTIATE_T_FILTER_RANGE(DecimalV2Value)
INSTANTIATE_T_FILTER_RANGE(TimestampValue)

#undef INSTANTIATE_T_FILTER_RANGE
} // namespace starrocks
