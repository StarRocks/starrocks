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

#include <utility>
#ifdef __x86_64__
#include <immintrin.h>
#endif
#if defined(__ARM_NEON__) || defined(__aarch64__)
#include <arm_acle.h>
#include <arm_neon.h>
#endif

#include <runtime/types.h>

#include "column/const_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "gutil/bits.h"
#include "gutil/casts.h"
#include "gutil/cpu.h"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"
#include "util/phmap/phmap.h"

namespace starrocks {
struct TypeDescriptor;
}

namespace starrocks {

class ColumnHelper {
public:
    // The input column is nullable or non-nullable uint8 column
    // The result column is not nullable uint8 column
    // For nullable uint8 column, we merge it's null column and data column
    // Used in ExecNode::eval_conjuncts
    static Filter& merge_nullable_filter(Column* column);

    // merge column with filter, and save result to filer.
    // `all_zero` means, after merging, if there is only zero value in filter.
    static void merge_two_filters(const ColumnPtr& column, Filter* __restrict filter, bool* all_zero = nullptr);
    static void merge_filters(const Columns& columns, Filter* __restrict filter);
    static void merge_two_filters(Filter* __restrict filter, const uint8_t* __restrict selected,
                                  bool* all_zero = nullptr);

    // Like merge_filters but use OR operator to merge them
    static void or_two_filters(Filter* __restrict filter, const uint8_t* __restrict selected);
    static void or_two_filters(size_t count, uint8_t* __restrict filter, const uint8_t* __restrict selected);
    static size_t count_nulls(const ColumnPtr& col);

    /**
     * 1 (not null and is true)
     * 0 (null or is false)
     * @param col must be a boolean column
     */
    static size_t count_true_with_notnull(const ColumnPtr& col);

    /**
     * 1 (not null and is false)
     * 0 (null or is true)
     * @param col must be a boolean column
     */
    static size_t count_false_with_notnull(const ColumnPtr& col);

    // Find the first non-null value in [start, end), return end if all null
    static size_t find_nonnull(const Column* col, size_t start, size_t end);

    // Find the non-null value in reversed order in [start, end), return start if all null
    static size_t last_nonnull(const Column* col, size_t start, size_t end);

    template <LogicalType Type>
    static inline ColumnPtr create_const_column(const RunTimeCppType<Type>& value, size_t chunk_size) {
        static_assert(!lt_is_decimal<Type>,
                      "Decimal column can not created by this function because of missing "
                      "precision and scale param");
        auto ptr = RunTimeColumnType<Type>::create();
        ptr->append_datum(Datum(value));
        // @FIXME: BinaryColumn get_data() will call build_slice() to modify the column's memory data,
        // but the operator is thread-unsafe, it's will cause crash in multi-thread(OLAP_SCANNER) when
        // OLAP_SCANNER call expression.
        // Call the get_data() when create ConstColumn is a short-term solution
        ptr->get_data();
        return ConstColumn::create(ptr, chunk_size);
    }

    template <LogicalType LT>
    static inline ColumnPtr create_const_decimal_column(RunTimeCppType<LT> value, int precision, int scale,
                                                        size_t size) {
        static_assert(lt_is_decimal<LT>);
        using ColumnType = RunTimeColumnType<LT>;
        auto data_column = ColumnType::create(precision, scale, 1);
        auto& data = ColumnHelper::cast_to_raw<LT>(data_column)->get_data();
        DCHECK(data.size() == 1);
        data[0] = value;
        return ConstColumn::create(data_column, size);
    }

    // If column is const column, duplicate the data column to chunk_size
    static ColumnPtr unpack_and_duplicate_const_column(size_t chunk_size, const ColumnPtr& column) {
        if (column->is_constant()) {
            auto* const_column = down_cast<ConstColumn*>(column.get());
            const_column->data_column()->assign(chunk_size, 0);
            return const_column->data_column();
        }
        return column;
    }

    static inline bool offsets_equal(const UInt32Column::Ptr& offset0, const UInt32Column::Ptr& offset1) {
        if (offset0->size() != offset1->size()) {
            return false;
        }
        auto data1 = offset0->get_data();
        auto data2 = offset1->get_data();
        return std::equal(data1.begin(), data1.end(), data2.begin());
    }

    static ColumnPtr unfold_const_column(const TypeDescriptor& type_desc, size_t size, const ColumnPtr& column) {
        if (column->only_null()) {
            auto col = ColumnHelper::create_column(type_desc, true);
            [[maybe_unused]] bool ok = col->append_nulls(size);
            DCHECK(ok);
            return col;
        } else if (column->is_constant()) {
            auto* const_column = down_cast<ConstColumn*>(column.get());
            const_column->data_column()->assign(size, 0);
            return const_column->data_column();
        }

        return column;
    }

    static ColumnPtr copy_and_unfold_const_column(const TypeDescriptor& dst_type_desc, bool dst_nullable,
                                                  const ColumnPtr& src_column, int num_rows) {
        ColumnPtr dst_column = create_column(dst_type_desc, dst_nullable);
        dst_column->reserve(num_rows);

        if (src_column->only_null()) {
            // 1. If src is constant and nullable, create an only null dest column.
            DCHECK(dst_nullable);
            [[maybe_unused]] bool ok = dst_column->append_nulls(num_rows);
            DCHECK(ok);
        } else {
            // 2. If src is constant and non-nullable, copy and unfold the constant column.
            auto* const_column = as_raw_column<ConstColumn>(src_column);
            // Note: we must create a new column every time here,
            // because VectorizedLiteral always return a same shared_ptr and we will modify it later.
            dst_column->append(*const_column->data_column(), 0, 1);
            dst_column->assign(num_rows, 0);
        }

        return dst_column;
    }

    // Update column according to whether the dest column and source column are nullable or not.
    static ColumnPtr update_column_nullable(bool dst_nullable, const ColumnPtr& src_column, int num_rows) {
        if (src_column->is_nullable()) {
            if (dst_nullable) {
                // 1. Src column and dest column are both nullable.
                return src_column;
            } else {
                // 2. src column is nullable, and dest column is non-nullable.
                auto* nullable_column = as_raw_column<NullableColumn>(src_column);
                DCHECK(!nullable_column->has_null());
                return nullable_column->data_column();
            }
        } else {
            // 3. Src column and dest column are both non-nullable.
            if (!dst_nullable) {
                return src_column;
            } else {
                // 4. src column is non-nullable, and dest column is nullable.
                ColumnPtr nullable_column = NullableColumn::create(src_column, NullColumn::create(num_rows, 0));
                return nullable_column;
            }
        }
    }

    // Cast to Nullable
    static ColumnPtr cast_to_nullable_column(const ColumnPtr& src_column) {
        if (src_column->is_nullable()) {
            return src_column;
        }
        return NullableColumn::create(src_column, NullColumn::create(src_column->size(), 0));
    }

    // Move the source column according to the specific dest type and nullable.
    static ColumnPtr move_column(const TypeDescriptor& dst_type_desc, bool dst_nullable, const ColumnPtr& src_column,
                                 int num_rows) {
        if (src_column->is_constant()) {
            return copy_and_unfold_const_column(dst_type_desc, dst_nullable, src_column, num_rows);
        }

        return update_column_nullable(dst_nullable, src_column, num_rows);
    }

    // Copy the source column according to the specific dest type and nullable.
    static ColumnPtr clone_column(const TypeDescriptor& dst_type_desc, bool dst_nullable, const ColumnPtr& src_column,
                                  int num_rows) {
        auto dst_column = update_column_nullable(dst_nullable, src_column, num_rows);
        return dst_column->clone_shared();
    }

    // Create an empty column
    static ColumnPtr create_column(const TypeDescriptor& type_desc, bool nullable);

    // expression trees' return column should align return type when some return columns maybe diff from the required
    // return type, as well the null flag. e.g., concat_ws returns col from create_const_null_column(), it's type is
    // Nullable(int8), but required return type is nullable(string), so col need align return type to nullable(string).
    static ColumnPtr align_return_type(const ColumnPtr& old_col, const TypeDescriptor& type_desc, size_t num_rows,
                                       const bool is_nullable);

    // Create a column with specified size, the column will be resized to size
    static ColumnPtr create_column(const TypeDescriptor& type_desc, bool nullable, bool is_const, size_t size,
                                   bool use_adaptive_nullable_column = false);
    /**
     * Cast columnPtr to special type ColumnPtr
     * Plz sure actual column type by yourself
     */
    template <LogicalType Type>
    static inline typename RunTimeColumnType<Type>::Ptr cast_to(const ColumnPtr& value) {
        down_cast<RunTimeColumnType<Type>*>(value.get());
        return std::static_pointer_cast<RunTimeColumnType<Type>>(value);
    }

    /**
     * Cast columnPtr to special type Column*
     * Plz sure actual column type by yourself
     */
    template <LogicalType Type>
    static inline RunTimeColumnType<Type>* cast_to_raw(const ColumnPtr& value) {
        return down_cast<RunTimeColumnType<Type>*>(value.get());
    }

    template <LogicalType Type>
    static inline RunTimeColumnType<Type>* cast_to_raw(Column* value) {
        return down_cast<RunTimeColumnType<Type>*>(value);
    }

    /**
     * Cast columnPtr to special type ColumnPtr
     * Plz sure actual column type by yourself
     */
    template <typename Type>
    static inline typename Type::Ptr as_column(ColumnPtr value) {
        return std::static_pointer_cast<Type>(value);
    }

    template <typename Type>
    static inline const Type* as_raw_column(const Column* value) {
        return down_cast<const Type*>(value);
    }
    /**
     * Cast columnPtr to special type Column*
     * Plz sure actual column type by yourself
     */
    template <typename Type>
    static inline Type* as_raw_column(const ColumnPtr& value) {
        return down_cast<Type*>(value.get());
    }

    template <LogicalType Type>
    static inline RunTimeCppType<Type>* get_cpp_data(const ColumnPtr& value) {
        return cast_to_raw<Type>(value)->get_data().data();
    }

    template <LogicalType Type>
    static inline const RunTimeCppType<Type>* unpack_cpp_data_one_value(const Column* input_column) {
        using ColumnType = RunTimeColumnType<Type>;
        DCHECK(input_column->size() == 1);
        if (input_column->has_null()) return nullptr;
        if (input_column->is_constant()) {
            const auto* const_column = down_cast<const ConstColumn*>(input_column);
            return down_cast<const ColumnType*>(const_column->data_column().get())->get_data().data();
        } else if (input_column->is_nullable()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(input_column);
            return down_cast<const ColumnType*>(nullable_column->data_column().get())->get_data().data();
        } else {
            return down_cast<const ColumnType*>(input_column)->get_data().data();
        }
    }

    template <LogicalType Type>
    static inline RunTimeCppType<Type> get_const_value(const Column* col) {
        const ColumnPtr& c = as_raw_column<ConstColumn>(col)->data_column();
        return cast_to_raw<Type>(c)->get_data()[0];
    }

    template <LogicalType Type>
    static inline RunTimeCppType<Type> get_const_value(const ColumnPtr& col) {
        const ColumnPtr& c = as_raw_column<ConstColumn>(col)->data_column();
        return cast_to_raw<Type>(c)->get_data()[0];
    }

    static Column* get_data_column(Column* column) {
        if (column->is_nullable()) {
            auto* nullable_column = down_cast<NullableColumn*>(column);
            return nullable_column->mutable_data_column();
        } else if (column->is_constant()) {
            auto* const_column = down_cast<ConstColumn*>(column);
            return const_column->mutable_data_column()->get();
        } else {
            return column;
        }
    }

    static const Column* get_data_column(const Column* column) {
        if (column->is_nullable()) {
            auto* nullable_column = down_cast<const NullableColumn*>(column);
            return nullable_column->data_column().get();
        } else if (column->is_constant()) {
            auto* const_column = down_cast<const ConstColumn*>(column);
            return const_column->data_column().get();
        } else {
            return column;
        }
    }

    static BinaryColumn* get_binary_column(Column* column) { return down_cast<BinaryColumn*>(get_data_column(column)); }

    static bool is_all_const(const Columns& columns);

    // Returns
    //  1. whether all the columns are constant.
    //  2. the number of the packed rows. If all the columns are constant, it will be 1,
    //     which could reduce unnecessary calculations.
    //     Don't forget to resize the result constant columns if necessary.
    static std::pair<bool, size_t> num_packed_rows(const Columns& columns);

    using ColumnsConstIterator = Columns::const_iterator;
    static bool is_all_const(ColumnsConstIterator const& begin, ColumnsConstIterator const& end);
    static size_t compute_bytes_size(ColumnsConstIterator const& begin, ColumnsConstIterator const& end);
    template <typename T, bool avx512f>
    static size_t t_filter_range(const Filter& filter, T* data, size_t from, size_t to) {
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
                memmove(data + result_offset, data + start_offset, kBatchNums * data_type_size);
                result_offset += kBatchNums;

            } else {
                // clang-format off
#define AVX512_COPY(SHIFT, MASK, WIDTH)                                         \
    {                                                                           \
        auto m = (mask >> SHIFT) & MASK;                                        \
        if (m) {                                                                \
            __m512i dst;                                                        \
            __m512i src = _mm512_loadu_epi##WIDTH(data + start_offset + SHIFT); \
            dst = _mm512_mask_compress_epi##WIDTH(dst, m, src);                 \
            _mm512_storeu_epi##WIDTH(data + result_offset, dst);                \
            result_offset += __builtin_popcount(m);                             \
        }                                                                       \
    }

// In theory we should put k1 in clobbers.
// But since we compile code with AVX2, k1 register is not used.
#define AVX512_ASM_COPY(SHIFT, MASK, WIDTH, WIDTHX)               \
    {                                                             \
        auto m = (mask >> SHIFT) & MASK;                          \
        if (m) {                                                  \
            T* src = data + start_offset + SHIFT;                 \
            T* dst = data + result_offset;                        \
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
                        *(data + result_offset++) = *(data + start_offset + idx);
                    }
                }
            }

            start_offset += kBatchNums;
        }
#elif defined(__ARM_NEON__) || defined(__aarch64__)
        const uint8_t* f_data = filter.data() + from;
        constexpr size_t data_type_size = sizeof(T);

        constexpr size_t kBatchNums = 128 / (8 * sizeof(uint8_t));
        while (start_offset + kBatchNums < to) {
            uint8x16_t filter = vld1q_u8(f_data);
            if (vmaxvq_u8(filter) == 0) {
                // skip
            } else if (vminvq_u8(filter)) {
                memmove(data + result_offset, data + start_offset, kBatchNums * data_type_size);
                result_offset += kBatchNums;
            } else {
                for (int i = 0; i < kBatchNums; ++i) {
                    // the index for vgetq_lane_u8 should be a literal integer
                    // but in ASAN/DEBUG the loop is unrolled. so we won't call vgetq_lane_u8
                    // in ASAN/DEBUG
#if defined(NDEBUG) && !defined(ADDRESS_SANITIZER)
                    if (vgetq_lane_u8(filter, i)) {
#else
                    if (f_data[i]) {
#endif
                        *(data + result_offset++) = *(data + start_offset + i);
                    }
                }
            }

            start_offset += kBatchNums;
            f_data += kBatchNums;
        }
#endif
        // clang-format on
        for (auto i = start_offset; i < to; ++i) {
            if (filter[i]) {
                *(data + result_offset) = *(data + i);
                result_offset++;
            }
        }

        return result_offset;
    }

    template <typename T>
    static size_t filter_range(const Filter& filter, T* data, size_t from, size_t to) {
        if (base::CPU::instance()->has_avx512f()) {
            return t_filter_range<T, true>(filter, data, from, to);
        } else {
            return t_filter_range<T, false>(filter, data, from, to);
        }
    }

    template <typename T>
    static size_t filter(const Filter& filter, T* data) {
        return filter_range(filter, data, 0, filter.size());
    }

    template <class FastPath, class SlowPath>
    static auto call_nullable_func(const Column* column, FastPath&& fast_path, SlowPath&& slow_path) {
        if (column->is_nullable()) {
            const auto* nullable_column = down_cast<const NullableColumn*>(column);
            const auto& null_data = nullable_column->immutable_null_column_data();
            const Column* data_column = nullable_column->data_column().get();
            if (column->has_null()) {
                return std::forward<SlowPath>(slow_path)(null_data, data_column);
            } else {
                return std::forward<FastPath>(fast_path)(data_column);
            }
        } else {
            return std::forward<FastPath>(fast_path)(column);
        }
    }

    static ColumnPtr create_const_null_column(size_t chunk_size);

    static ColumnPtr convert_time_column_from_double_to_str(const ColumnPtr& column);

    static NullColumnPtr one_size_not_null_column;

    static NullColumnPtr one_size_null_column;
};

// Hold a slice of chunk
template <class Ptr = ChunkUniquePtr>
struct ChunkSliceTemplate {
    Ptr chunk;
    size_t offset = 0;

    bool empty() const;
    size_t rows() const;
    size_t skip(size_t skip_rows);
    Ptr cutoff(size_t required_rows);
    void reset(Ptr input);
};

template <LogicalType ltype>
struct GetContainer {
    using ColumnType = typename RunTimeTypeTraits<ltype>::ColumnType;
    const auto& get_data(const Column* column) { return ColumnHelper::as_raw_column<ColumnType>(column)->get_data(); }
    const auto& get_data(const ColumnPtr& column) {
        return ColumnHelper::as_raw_column<ColumnType>(column.get())->get_data();
    }
};

#define GET_CONTAINER(ltype)                                                                  \
    template <>                                                                               \
    struct GetContainer<ltype> {                                                              \
        const auto& get_data(const Column* column) {                                          \
            return ColumnHelper::as_raw_column<BinaryColumn>(column)->get_proxy_data();       \
        }                                                                                     \
        const auto& get_data(const ColumnPtr& column) {                                       \
            return ColumnHelper::as_raw_column<BinaryColumn>(column.get())->get_proxy_data(); \
        }                                                                                     \
    };
APPLY_FOR_ALL_STRING_TYPE(GET_CONTAINER)
#undef GET_CONTAINER

using ChunkSlice = ChunkSliceTemplate<ChunkUniquePtr>;
using ChunkSharedSlice = ChunkSliceTemplate<ChunkPtr>;

} // namespace starrocks
