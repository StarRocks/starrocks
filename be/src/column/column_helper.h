// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <immintrin.h>
#include <runtime/types.h>

#include "column/const_column.h"
#include "column/type_traits.h"
#include "common/config.h"
#include "gutil/bits.h"
#include "gutil/casts.h"
#include "runtime/primitive_type.h"

namespace starrocks {
struct TypeDescriptor;
}

namespace starrocks::vectorized {

class ColumnHelper {
public:
    static void init_static_variable();

    // The input column is nullable or non-nullable uint8 column
    // The result column is not nullable uint8 column
    // For nullable uint8 column, we merge it's null column and data column
    // Used in ExecNode::eval_conjuncts
    static Column::Filter& merge_nullable_filter(Column* column);

    // merge column with filter, and save result to filer.
    // `all_zero` means, after merging, if there is only zero value in filter.
    static void merge_two_filters(const ColumnPtr column, Column::Filter* __restrict filter, bool* all_zero = nullptr);
    static void merge_filters(const Columns& columns, Column::Filter* __restrict filter);
    static void merge_two_filters(Column::Filter* __restrict filter, const uint8_t* __restrict selected,
                                  bool* all_zero = nullptr);

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

    template <PrimitiveType Type>
    static inline ColumnPtr create_const_column(const RunTimeCppType<Type>& value, size_t chunk_size) {
        static_assert(!pt_is_decimal<Type>,
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

    // If column is const column, duplicate the data column to chunk_size
    static ColumnPtr unpack_and_duplicate_const_column(size_t chunk_size, const ColumnPtr& column) {
        if (column->is_constant()) {
            ConstColumn* const_column = down_cast<ConstColumn*>(column.get());
            const_column->data_column()->assign(chunk_size, 0);
            return const_column->data_column();
        }
        return column;
    }

    static ColumnPtr unfold_const_column(const TypeDescriptor& type_desc, size_t size, const ColumnPtr& column) {
        if (column->only_null()) {
            auto col = ColumnHelper::create_column(type_desc, true);
            [[maybe_unused]] bool ok = col->append_nulls(size);
            DCHECK(ok);
            return col;
        } else if (column->is_constant()) {
            ConstColumn* const_column = down_cast<ConstColumn*>(column.get());
            const_column->data_column()->assign(size, 0);
            return const_column->data_column();
        }

        return column;
    }

    static ColumnPtr create_column(const TypeDescriptor& type_desc, bool nullable);

    // If is_const is true, you must pass the size arg
    static ColumnPtr create_column(const TypeDescriptor& type_desc, bool nullable, bool is_const, size_t size);

    /**
     * Cast columnPtr to special type ColumnPtr
     * Plz sure actual column type by yourself
     */
    template <PrimitiveType Type>
    static inline typename RunTimeColumnType<Type>::Ptr cast_to(const ColumnPtr& value) {
        down_cast<RunTimeColumnType<Type>*>(value.get());
        return std::static_pointer_cast<RunTimeColumnType<Type>>(value);
    }

    /**
     * Cast columnPtr to special type Column*
     * Plz sure actual column type by yourself
     */
    template <PrimitiveType Type>
    static inline RunTimeColumnType<Type>* cast_to_raw(const ColumnPtr& value) {
        return down_cast<RunTimeColumnType<Type>*>(value.get());
    }

    /**
     * Cast columnPtr to special type ColumnPtr
     * Plz sure actual column type by yourself
     */
    template <typename Type>
    static inline typename Type::Ptr as_column(ColumnPtr value) {
        return std::static_pointer_cast<Type>(value);
    }

    /**
     * Cast columnPtr to special type Column*
     * Plz sure actual column type by yourself
     */
    template <typename Type>
    static inline Type* as_raw_column(const ColumnPtr& value) {
        return down_cast<Type*>(value.get());
    }

    template <PrimitiveType Type>
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

    using ColumnsConstIterator = Columns::const_iterator;
    static bool is_all_const(ColumnsConstIterator const& begin, ColumnsConstIterator const& end);
    static size_t compute_bytes_size(ColumnsConstIterator const& begin, ColumnsConstIterator const& end);
    template <typename T>
    static size_t filter_range(const Column::Filter& filter, T* data, size_t from, size_t to) {
        auto start_offset = from;
        auto result_offset = from;

#ifdef __AVX2__
        const uint8_t* f_data = filter.data();
        size_t data_type_size = sizeof(T);

        constexpr size_t kBatchNums = 256 / (8 * sizeof(uint8_t));
        const __m256i all0 = _mm256_setzero_si256();

        while (start_offset + kBatchNums < to) {
            __m256i f = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(f_data + start_offset));
            uint32_t mask = _mm256_movemask_epi8(_mm256_cmpgt_epi8(f, all0));

            if (mask == 0) {
                // all no hit, pass
            } else if (mask == 0xffffffff) {
                // all hit, copy all
                memmove(data + result_offset, data + start_offset, kBatchNums * data_type_size);
                result_offset += kBatchNums;

            } else {
                // skip not hit row, it's will reduce compare when filter layout is sparse,
                // like "00010001...", but is ineffective when the filter layout is dense.
                int zero_count = Bits::CountTrailingZerosNonZero32(mask);
                int i = zero_count;
                while (i < kBatchNums) {
                    mask = zero_count < 31 ? mask >> (zero_count + 1u) : 0;
                    *(data + result_offset) = *(data + start_offset + i);
                    zero_count = Bits::CountTrailingZeros32(mask);
                    result_offset += 1;
                    i += (zero_count + 1);
                }
            }

            start_offset += kBatchNums;
        }
#endif
        for (auto i = start_offset; i < to; ++i) {
            if (filter[i]) {
                *(data + result_offset) = *(data + i);
                result_offset++;
            }
        }

        return result_offset;
    }

    template <typename T>
    static size_t filter(const Column::Filter& filter, T* data) {
        return filter_range(filter, data, 0, filter.size());
    }

    static ColumnPtr create_const_null_column(size_t chunk_size);

    static NullColumnPtr one_size_not_null_column;

    static NullColumnPtr one_size_null_column;

    static NullColumnPtr s_all_not_null_column;
};

} // namespace starrocks::vectorized
