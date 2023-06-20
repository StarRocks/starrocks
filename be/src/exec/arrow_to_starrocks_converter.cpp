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

#include "exec/arrow_to_starrocks_converter.h"

#include <arrow/array.h>

#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/scalar.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "column/array_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exec/arrow_type_traits.h"
#include "exec/parquet_scanner.h"
#include "gutil/strings/fastmem.h"
#include "gutil/strings/substitute.h"
#include "runtime/datetime_value.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "types/logical_type.h"
#include "util/pred_guard.h"

namespace starrocks {

Status illegal_converting_error(const std::string& arrow_type_name, const std::string& type_name) {
    return Status::InternalError(strings::Substitute("Illegal converting from arrow type($0) to StarRocks type($1)",
                                                     arrow_type_name, type_name));
}

DEF_PRED_GUARD(DirectlyCopybleGuard, is_directly_copyable, ArrowTypeId, AT, LogicalType, LT)
#define IS_DIRECTLY_COPYABLE_CTOR(AT, LT) DEF_PRED_CASE_CTOR(is_directly_copyable, AT, LT)
#define IS_DIRECTLY_COPYABLE_R(LT, ...) \
    DEF_BINARY_RELATION_ENTRY_SEP_NONE_R(IS_DIRECTLY_COPYABLE_CTOR, LT, ##__VA_ARGS__)

IS_DIRECTLY_COPYABLE_R(TYPE_TINYINT, ArrowTypeId::INT8, ArrowTypeId::UINT8)
IS_DIRECTLY_COPYABLE_R(TYPE_SMALLINT, ArrowTypeId::INT16, ArrowTypeId::UINT16)
IS_DIRECTLY_COPYABLE_R(TYPE_INT, ArrowTypeId::INT32, ArrowTypeId::UINT32)
IS_DIRECTLY_COPYABLE_R(TYPE_BIGINT, ArrowTypeId::INT64, ArrowTypeId::UINT64)
IS_DIRECTLY_COPYABLE_R(TYPE_FLOAT, ArrowTypeId::FLOAT)
IS_DIRECTLY_COPYABLE_R(TYPE_DOUBLE, ArrowTypeId::DOUBLE)

DEF_PRED_GUARD(AssignableGuard, is_assignable, ArrowTypeId, AT, LogicalType, LT)
#define IS_ASSIGNABLE_CTOR(AT, LT) DEF_PRED_CASE_CTOR(is_assignable, AT, LT)
#define IS_ASSIGNABLE(AT, ...) DEF_BINARY_RELATION_ENTRY_SEP_SEMICOLON(IS_ASSIGNABLE_CTOR, AT, ##__VA_ARGS__)

IS_ASSIGNABLE(ArrowTypeId::BOOL, TYPE_BOOLEAN)
IS_ASSIGNABLE(ArrowTypeId::INT8, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT, TYPE_FLOAT, TYPE_DOUBLE)
IS_ASSIGNABLE(ArrowTypeId::UINT8, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT, TYPE_FLOAT, TYPE_DOUBLE)
IS_ASSIGNABLE(ArrowTypeId::INT16, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT, TYPE_FLOAT, TYPE_DOUBLE)
IS_ASSIGNABLE(ArrowTypeId::UINT16, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT, TYPE_FLOAT, TYPE_DOUBLE)
IS_ASSIGNABLE(ArrowTypeId::INT32, TYPE_BIGINT, TYPE_LARGEINT, TYPE_DOUBLE)
IS_ASSIGNABLE(ArrowTypeId::UINT32, TYPE_BIGINT, TYPE_LARGEINT, TYPE_DOUBLE)
IS_ASSIGNABLE(ArrowTypeId::INT64, TYPE_LARGEINT)
IS_ASSIGNABLE(ArrowTypeId::UINT64, TYPE_LARGEINT)
IS_ASSIGNABLE(ArrowTypeId::HALF_FLOAT, TYPE_FLOAT, TYPE_DOUBLE)
IS_ASSIGNABLE(ArrowTypeId::FLOAT, TYPE_DOUBLE)

size_t fill_null_column(const arrow::Array* array, size_t array_start_idx, size_t num_elements, NullColumn* null_column,
                        size_t column_start_idx) {
    null_column->resize(null_column->size() + num_elements);
    auto* null_data = (&null_column->get_data().front()) + column_start_idx;
    size_t null_count = 0;
    for (size_t i = 0; i < num_elements; ++i) {
        auto is_null = array->IsNull(array_start_idx + i);
        null_data[i] = is_null;
        null_count += is_null;
    }
    return null_count;
}

void fill_filter(const arrow::Array* array, size_t array_start_idx, size_t num_elements, Filter* filter,
                 size_t column_start_idx, ArrowConvertContext* ctx) {
    DCHECK_EQ(filter->size(), column_start_idx + num_elements);
    auto* filter_data = (&filter->front()) + column_start_idx;
    bool all_invalid = true;
    for (size_t i = 0; i < num_elements; ++i) {
        filter_data[i] = array->IsValid(array_start_idx + i);
        all_invalid &= filter_data[i];
    }
    if (UNLIKELY(!all_invalid)) {
        ctx->report_error_message("column type is not null but data is null", "");
    }
}
// A general arrow converter for fixed length type
//
// case#1: is_directly_copy(AT, LT>==true
// if underlying types are identical, the copy the whole memory by calling memcpy
// e.g.
// UINT8 or INT8 in arrow convert to TYPE_TINYINT in StarRocks
// FLOAT in arrow convert to TYPE_FLOAT
//
// case#2: is_assignable<AT, LT>==true
// if underlying type in arrow is narrower than its counterpart in StarRocks and
// assignment operation is lossless, then copy one by one element from arrow to StarRocks
//e.g.
// BOOL(bit) in arrow converts to TYPE_BOOLEAN(uint8_t) in StarRocks
// HALF_FLOAT in arrow convert ot TYPE_FLOAT or TYPE_DOUBLE in StarRocks
//
// If is_strict is true, invalid rows are saved in filter_data, and discarded finally after an
// entire chunk is generated; otherwise, invalid rows are regarded as null values.

template <ArrowTypeId AT, LogicalType LT, bool is_nullable, bool is_strict, typename = guard::Guard,
          typename = guard::Guard>
struct ArrowConverter {
    using ArrowArrayType = ArrowTypeIdToArrayType<AT>;
    using ArrowCppType = ArrowTypeIdToCppType<AT>;
    using CppType = RunTimeCppType<LT>;
    using ColumnType = RunTimeColumnType<LT>;

    static Status apply(const arrow::Array* array, size_t array_start_idx, size_t num_elements, Column* column,
                        size_t column_start_idx, [[maybe_unused]] uint8_t* null_data,
                        [[maybe_unused]] Filter* chunk_filter, ArrowConvertContext* ctx,
                        [[maybe_unused]] ConvertFuncTree* conv_func) {
        auto concrete_array = down_cast<const ArrowArrayType*>(array);
        auto concrete_column = down_cast<ColumnType*>(column);
        concrete_column->resize(column->size() + num_elements);
        CppType* data = &concrete_column->get_data().front() + column_start_idx;
        if constexpr (is_directly_copyable<AT, LT>) {
            static_assert(sizeof(CppType) == sizeof(ArrowCppType));
            const ArrowCppType* array_data = concrete_array->raw_values() + array_start_idx;
            strings::memcpy_inlined(data, array_data, num_elements * sizeof(CppType));
        } else if constexpr (is_assignable<AT, LT>) {
            for (size_t i = 0; i < num_elements; ++i) {
                data[i] = static_cast<CppType>(concrete_array->Value(array_start_idx + i));
            }
        } else {
            static_assert(is_directly_copyable<AT, LT> || is_assignable<AT, LT>);
        }
        return Status::OK();
    }
};

static void simd_offsets_copy(uint32_t* dst_array, const int32_t* src_array, const size_t num_elements,
                              const uint32_t dst_base, const uint32_t src_base) {
    static constexpr size_t element_size = sizeof(uint32_t);
    const size_t num_bytes = element_size * num_elements;
    const char* src_begin = (const char*)src_array;
    const char* src_end = src_begin + num_bytes;
    const char* src_p = src_begin;
    char* dst_p = (char*)dst_array;
    uint32_t base_diff = dst_base - src_base;
#if defined(__AVX2__)
    static constexpr size_t avx2_size = sizeof(__m256i);
    const char* src_end_avx2 = src_begin + (num_bytes & ~(avx2_size - 1));
    const __m256i diffs = _mm256_set1_epi32(base_diff);
    for (; src_p < src_end_avx2; src_p += avx2_size, dst_p += avx2_size) {
        _mm256_storeu_si256((__m256i_u*)dst_p, _mm256_add_epi32(_mm256_loadu_si256((const __m256i_u*)src_p), diffs));
    }
#elif defined(__SSE2__)
    static constexpr size_t sse2_size = sizeof(__m128i);
    const char* src_end_sse2 = src_begin + (num_bytes & ~(sse2_size - 1));
    const __m128i diffs = _mm_set1_epi32(dst_base - src_base);
    for (; src_p < src_end_sse2; src_p += sse2_size, dst_p += sse2_size) {
        _mm_storeu_si128((__m128i_u*)dst_p, _mm_add_epi32(_mm_loadu_si128((const __m128i_u*)src_p), diffs));
    }
#endif
    for (; src_p < src_end; src_p += element_size, dst_p += element_size) {
        *(uint32_t*)dst_p = *(uint32_t*)src_p + base_diff;
    }
}

// for BinaryColumn and ArrowColumn, data transposition optimization can be employed to speedup converting,
// in such cases, underlying data is copied verbatim from arrow to column, but the each element of offsets
// must be added a const diff to. when arrow offset_type is as wide as column's counterpart, SIMD
// optimization can be used to speed up offsets copying.
// {List, Binary, String}Type in arrow use int32_t as offset type, so offsets can be copied via SIMD,
// Large{List, Binary, String}Type use int64_t, so must copy offset elements one by one.
template <typename T>
void offsets_copy(const T* arrow_offsets_data, T arrow_base_offset, size_t num_elements, uint32_t* offsets_data,
                  uint32_t base_offset) {
    if constexpr (sizeof(T) == sizeof(uint32_t)) {
        simd_offsets_copy(offsets_data, arrow_offsets_data, num_elements, base_offset, arrow_base_offset);
    } else {
        for (auto i = 0; i < num_elements; ++i) {
            // never change following code to
            // base_offsets - arrow_base_offset + arrow_offsets_data[i],
            // that would cause underflow for unsigned int;
            offsets_data[i] = base_offset + (arrow_offsets_data[i] - arrow_base_offset);
        }
    }
}

template <LogicalType LT, typename = StringOrBinaryGaurd<LT>>
static inline constexpr uint32_t binary_max_length = (LT == TYPE_VARCHAR || LT == TYPE_VARBINARY)
                                                             ? TypeDescriptor::MAX_VARCHAR_LENGTH
                                                             : TypeDescriptor::MAX_CHAR_LENGTH;

template <ArrowTypeId AT, LogicalType LT, bool is_nullable, bool is_strict>
struct ArrowConverter<AT, LT, is_nullable, is_strict, BinaryATGuard<AT>, StringOrBinaryGaurd<LT>> {
    using ArrowArrayType = ArrowTypeIdToArrayType<AT>;
    using ArrowCppType = ArrowTypeIdToCppType<AT>;
    using CppType = RunTimeCppType<LT>;
    using ColumnType = RunTimeColumnType<LT>;
    static void optimize_not_nullable_fixed_size_binary(const ArrowArrayType* array, size_t array_start_idx,
                                                        size_t num_elements, ColumnType* column,
                                                        size_t column_start_idx) {
        uint32_t width = array->byte_width();
        column->resize(column->size() + num_elements);
        const auto* array_data = array->GetValue(array_start_idx);
        auto& bytes = column->get_bytes();
        auto& offsets = column->get_offset();
        size_t copy_size = width * num_elements;
        bytes.resize(bytes.size() + width * num_elements);
        const auto base_offset = offsets[column_start_idx];
        strings::memcpy_inlined(bytes.data() + base_offset, array_data, copy_size);
        for (auto i = 0; i < num_elements; ++i) {
            offsets[column_start_idx + i + 1] = base_offset + (i + 1) * width;
        }
    }

    static void optimize_nullable_fixed_size_binary(const ArrowArrayType* array, size_t array_start_idx,
                                                    size_t num_elements, ColumnType* column, size_t column_start_idx) {
        uint32_t width = array->byte_width();
        const auto* array_data = array->GetValue(array_start_idx);
        column->resize(column->size() + num_elements);
        auto& bytes = column->get_bytes();
        auto& offsets = column->get_offset();
        size_t bytes_off = bytes.size();
        bytes.resize(bytes_off + width * num_elements);
        auto* bytes_start = (uint8_t*)&bytes.front();
        for (auto i = 0; i < num_elements; ++i) {
            size_t array_idx = array_start_idx + i;
            size_t offsets_idx = column_start_idx + i + 1;
            if (!array->IsNull(array_idx)) {
                strings::memcpy_inlined(bytes_start + bytes_off, array_data + i * width, width);
                bytes_off += width;
            }
            offsets[offsets_idx] = bytes_off;
        }
        bytes.resize(bytes_off);
    }

    static void optimize_non_fixed_size_binary(const ArrowArrayType* array, size_t array_start_idx, size_t num_elements,
                                               ColumnType* column, size_t column_start_idx) {
        using ArrowOffsetType = typename ArrowArrayType::offset_type;

        // GetView report error when array contains only empty string.
        // so use ugly GetValue with an additional dummy length.
        [[maybe_unused]] ArrowOffsetType dummy_length;
        const uint8_t* array_data = array->GetValue(array_start_idx, &dummy_length);
        column->resize(column->size() + num_elements);
        auto& bytes = column->get_bytes();
        auto& offsets = column->get_offset();
        ArrowOffsetType copy_size =
                array->value_offset(array_start_idx + num_elements) - array->value_offset(array_start_idx);
        bytes.resize(bytes.size() + copy_size);
        const auto base_offset = offsets[column_start_idx];
        strings::memcpy_inlined(bytes.data() + base_offset, array_data, copy_size);
        auto* offsets_data = &offsets[column_start_idx + 1];
        auto* arrow_offsets_data = array->raw_value_offsets() + array_start_idx + 1;
        const auto arrow_base_offset = array->value_offset(array_start_idx);
        offsets_copy<ArrowOffsetType>(arrow_offsets_data, arrow_base_offset, num_elements, offsets_data, base_offset);
    }

    // Fill num_elements# empty string into column, started at position column_start_idx
    static void fill_empty_string(ColumnType* column, size_t column_start_idx, size_t num_elements) {
        column->resize(column->size() + num_elements);
        auto& offsets = column->get_offset();
        const auto base_offset = offsets[column_start_idx];
        auto* offsets_data = &offsets[column_start_idx + 1];
        std::fill_n(offsets_data, num_elements, base_offset);
    }

    static Status length_exceeds_limit_error(int length, int limit) {
        std::string s = (LT == TYPE_VARCHAR) ? "varchar" : ((LT == TYPE_CHAR) ? "char" : "binary");
        return Status::InternalError(strings::Substitute("Length($0) exceeds limit($1) of $2", length, limit, s));
    }

    static Status apply(const arrow::Array* array, size_t array_start_idx, size_t num_elements, Column* column,
                        size_t column_start_idx, [[maybe_unused]] uint8_t* null_data, Filter* chunk_filter,
                        ArrowConvertContext* ctx, [[maybe_unused]] ConvertFuncTree* conv_func) {
        auto concrete_array = down_cast<const ArrowArrayType*>(array);
        auto concrete_column = down_cast<ColumnType*>(column);
        auto* filter_data = (&chunk_filter->front()) + column_start_idx;
        size_t max_length = binary_max_length<LT>;
        if (ctx != nullptr) {
            size_t type_len = ctx->current_slot->type().len;
            if (type_len > 0) {
                max_length = type_len;
            }
        }

        if constexpr (AT == ArrowTypeId::FIXED_SIZE_BINARY) {
            uint32_t width = concrete_array->byte_width();
            // FixedSizeBinary's length exceeds maximum length of varchar/var
            if (width > max_length) {
                fill_empty_string(concrete_column, column_start_idx, num_elements);
                // Invalid data are regarded as nulls if target Column is nullable and is_strict is
                // false; a not-nullable column can not accept nulls, so discards invalid data;
                // Strict-mode(is_strict=true) loading also discards invalid data.
                if constexpr (is_nullable && !is_strict) {
                    //convert NULL
                    std::fill_n(null_data, num_elements, DATUM_NULL);
                } else {
                    //filter all
                    std::fill_n(filter_data, num_elements, 0);

                    if (ctx != nullptr) {
                        std::string raw_data = "arrow data is fixed size binary type";
                        std::string reason = strings::Substitute("type length $0 exceeds max length $1", width,
                                                                 binary_max_length<LT>);
                        ctx->report_error_message(reason, raw_data);
                    }
                }
                return Status::OK();
            }
            if constexpr (is_nullable) {
                optimize_nullable_fixed_size_binary(concrete_array, array_start_idx, num_elements, concrete_column,
                                                    column_start_idx);
            } else {
                optimize_not_nullable_fixed_size_binary(concrete_array, array_start_idx, num_elements, concrete_column,
                                                        column_start_idx);
            }
            return Status::OK();
        } else {
            using ArrowOffsetType = typename ArrowArrayType::offset_type;
            bool exceed_max_length = false;
            bool repeated = false;

            for (auto i = array_start_idx; i < array_start_idx + num_elements; ++i) {
                // Binary length exceeds maximum length of varchar/char.
                if (concrete_array->value_length(i) > max_length) {
                    exceed_max_length = true;
                    if constexpr (is_nullable && !is_strict) {
                        null_data[i - array_start_idx] = DATUM_NULL;
                    } else {
                        filter_data[i - array_start_idx] = 0;

                        if (ctx != nullptr && !repeated) {
                            repeated = true;
                            ArrowOffsetType s_size = 0;
                            const char* s_data = reinterpret_cast<const char*>(concrete_array->GetValue(i, &s_size));
                            std::string raw_data = std::string(s_data, s_size);
                            std::string reason =
                                    strings::Substitute("string length $0 exceeds max length $1", s_size, max_length);
                            ctx->report_error_message(reason, raw_data);
                        }
                    }
                }
            }
            if (exceed_max_length) {
                Slice s;
                concrete_column->reserve(concrete_column->size() + num_elements);
                for (auto i = array_start_idx; i < array_start_idx + num_elements; ++i) {
                    ArrowOffsetType slice_size = 0;
                    s.data = (char*)concrete_array->GetValue(i, &slice_size);
                    s.size = slice_size;
                    if (s.size > max_length) {
                        concrete_column->append_default();
                    } else {
                        concrete_column->append(s);
                    }
                }
            } else {
                optimize_non_fixed_size_binary(concrete_array, array_start_idx, num_elements, concrete_column,
                                               column_start_idx);
            }
            return Status::OK();
        }
    }
};

template <typename T>
struct RectifyDecimalType {
    using type = T;
};
template <>
struct RectifyDecimalType<DecimalV2Value> {
    using type = int128_t;
};

template <typename T>
using rectify_decimal_type = typename RectifyDecimalType<T>::type;
VALUE_GUARD(LogicalType, ArrowDecimalOfAnyVersionLTGuard, arrow_lt_is_decimal_of_any_version, TYPE_DECIMAL,
            TYPE_DECIMALV2, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128)

template <LogicalType LT, bool is_nullable, bool is_strict>
struct ArrowConverter<ArrowTypeId::DECIMAL, LT, is_nullable, is_strict, guard::Guard,
                      ArrowDecimalOfAnyVersionLTGuard<LT>> {
    static constexpr ArrowTypeId AT = ArrowTypeId::DECIMAL;
    using ArrowType = ArrowTypeIdToType<AT>;
    using ArrowArrayType = ArrowTypeIdToArrayType<AT>;
    using ArrowCppType = ArrowTypeIdToCppType<AT>;
    using CppType = RunTimeCppType<LT>;
    using ColumnType = RunTimeColumnType<LT>;

    static void optimize_decimal128_with_same_scale(const arrow::Decimal128Array* array, size_t array_start_idx,
                                                    size_t num_elements, ColumnType* column, size_t column_start_idx) {
        column->resize(column->size() + num_elements);
        auto* data = &column->get_data().front() + column_start_idx;
        auto* arrow_data = array->raw_values() + sizeof(CppType) * array_start_idx;
        strings::memcpy_inlined(data, arrow_data, sizeof(CppType) * num_elements);
    }

    template <bool is_aligned>
    static void copy_int128_t(int128_t* dst, const uint8_t* src) {
        if constexpr (is_aligned) {
            *dst = *(int128_t*)src;
        } else {
#if defined(__SSE2__)
            _mm_store_si128((__m128i*)dst, _mm_loadu_si128((__m128i_u*)src));
#else
            strings::memcpy_inlined(dst, src, sizeof(int128_t));
#endif
        }
    }

    static Status cast_error(int bits, int dst_scale, int src_scale, int128_t value) {
        std::string s = DecimalV3Cast::to_string<int128_t>(value, decimal_precision_limit<int128_t>, src_scale);
        return Status::InternalError(strings::Substitute("Decimal$0(*,$1) cannot hold $2", bits, dst_scale, s));
    }

    template <bool is_aligned, typename T>
    static Status fill_column(T* data, const uint8_t* arrow_data, const size_t num_elements, int dst_scale,
                              int src_scale, [[maybe_unused]] uint8_t* null_data,
                              [[maye_unused]] uint8_t* filter_data) {
        int128_t datum;
        const uint8_t* arrow_p = arrow_data;
        int adjust_scale = dst_scale - src_scale;
        if (adjust_scale == 0) {
            for (auto i = 0; i < num_elements; ++i, arrow_p += sizeof(int128_t)) {
                if constexpr (is_nullable) {
                    if (null_data[i] == DATUM_NULL) continue;
                }
                copy_int128_t<is_aligned>(&datum, arrow_p);
                auto overflow = DecimalV3Cast::to_decimal_trivial<int128_t, T, true>(datum, data + i);
                if (UNLIKELY(overflow)) {
                    if constexpr (is_nullable && !is_strict) {
                        null_data[i] = DATUM_NULL;
                    } else {
                        filter_data[i] = 0;
                    }
                }
            }
        } else if (adjust_scale > 0) {
            const auto scale_factor = get_scale_factor<T>(adjust_scale);
            for (auto i = 0; i < num_elements; ++i, arrow_p += sizeof(int128_t)) {
                if constexpr (is_nullable) {
                    if (null_data[i] == DATUM_NULL) continue;
                }
                copy_int128_t<is_aligned>(&datum, arrow_p);
                auto overflow = DecimalV3Cast::to_decimal<int128_t, T, T, true, true>(datum, scale_factor, data + i);
                if (UNLIKELY(overflow)) {
                    if constexpr (is_nullable && !is_strict) {
                        null_data[i] = DATUM_NULL;
                    } else {
                        filter_data[i] = 0;
                    }
                }
            }
        } else {
            const auto scale_factor = get_scale_factor<int128_t>(-adjust_scale);
            for (auto i = 0; i < num_elements; ++i, arrow_p += sizeof(int128_t)) {
                if constexpr (is_nullable) {
                    if (null_data[i] == DATUM_NULL) continue;
                }
                copy_int128_t<is_aligned>(&datum, arrow_p);
                auto overflow =
                        DecimalV3Cast::to_decimal<int128_t, T, int128_t, false, true>(datum, scale_factor, data + i);
                if (UNLIKELY(overflow)) {
                    if constexpr (is_nullable && !is_strict) {
                        null_data[i] = DATUM_NULL;
                    } else {
                        filter_data[i] = 0;
                    }
                }
            }
        }
        return Status::OK();
    }

    static Status apply(const arrow::Array* array, size_t array_start_idx, size_t num_elements, Column* column,
                        size_t column_start_idx, [[maybe_unused]] uint8_t* null_data,
                        [[maybe_unused]] Filter* chunk_filter, ArrowConvertContext* ctx,
                        [[maybe_unused]] ConvertFuncTree* conv_func) {
        auto concrete_array = down_cast<const ArrowArrayType*>(array);
        auto concrete_type = std::static_pointer_cast<ArrowType>(array->type());
        auto concrete_column = down_cast<ColumnType*>(column);
        const auto src_scale = concrete_type->scale();

        auto dst_scale = 9;
        if constexpr (lt_is_decimal<LT>) {
            dst_scale = concrete_column->scale();
        }

        if constexpr (!is_nullable) {
            if constexpr (lt_is_decimal128<LT> || lt_is_decimalv2<LT>) {
                if (src_scale == dst_scale) {
                    optimize_decimal128_with_same_scale(concrete_array, array_start_idx, num_elements, concrete_column,
                                                        column_start_idx);
                    return Status::OK();
                }
            }
        }

        concrete_column->resize(column->size() + num_elements);
        using RectifiedCppType = rectify_decimal_type<CppType>;
        auto* data = (RectifiedCppType*)(&concrete_column->get_data().front() + column_start_idx);
        auto* arrow_data = concrete_array->raw_values() + array_start_idx * concrete_type->byte_width();
        bool is_aligned = ((uintptr_t)arrow_data & (concrete_type->byte_width() - 1)) == 0;
        auto* filter_data = (&chunk_filter->front()) + column_start_idx;
        if (is_aligned) {
            return fill_column<true, RectifiedCppType>(data, arrow_data, num_elements, dst_scale, src_scale, null_data,
                                                       filter_data);
        }
        return fill_column<false, RectifiedCppType>(data, arrow_data, num_elements, dst_scale, src_scale, null_data,
                                                    filter_data);
    }
};

VALUE_GUARD(ArrowTypeId, Date32ATGuard, at_is_date32, ArrowTypeId::DATE32)
VALUE_GUARD(ArrowTypeId, Date64ATGuard, at_is_date64, ArrowTypeId::DATE64)
VALUE_GUARD(ArrowTypeId, DateTimeATGuard, at_is_datetime, ArrowTypeId::TIMESTAMP)
VALUE_GUARD(ArrowTypeId, DateATGuard, at_is_date, ArrowTypeId::DATE32, ArrowTypeId::DATE64)

VALUE_GUARD(ArrowTypeId, DateOrDateTimeATGuard, at_is_date_or_datetime, ArrowTypeId::DATE32, ArrowTypeId::DATE64,
            ArrowTypeId::TIMESTAMP)

template <ArrowTypeId AT, LogicalType LT, bool is_nullable, bool is_strict>
struct ArrowConverter<AT, LT, is_nullable, is_strict, DateOrDateTimeATGuard<AT>, DateOrDateTimeLTGuard<LT>> {
    using ArrowType = ArrowTypeIdToType<AT>;
    using ArrowArrayType = ArrowTypeIdToArrayType<AT>;
    using ArrowCppType = ArrowTypeIdToCppType<AT>;
    using CppType = RunTimeCppType<LT>;
    using ColumnType = RunTimeColumnType<LT>;

    static void convert_date(CppType& datum, const ArrowCppType& arrow_datum) {
        int64_t timestamp;
        if constexpr (at_is_date32<AT>) {
            timestamp = (int64_t)arrow_datum * 24 * 60 * 60;
        } else if constexpr (at_is_date64<AT>) {
            timestamp = arrow_datum / 1000;
        } else {
            static_assert(at_is_date<AT>, "Invalid arrow type");
        }
        if constexpr (lt_is_date<LT>) {
            TimestampValue ts;
            ts.from_unix_second(timestamp);
            datum = (DateValue)ts;
        } else if constexpr (lt_is_datetime<LT>) {
            datum.from_unix_second(timestamp);
        } else {
            static_assert(lt_is_date_or_datetime<LT>, "Invalid starrocks type");
        }
    }

    static int64_t time_unit_divisor(arrow::TimeUnit::type unit) {
        // StarRocks only supports seconds
        switch (unit) {
        case arrow::TimeUnit::type::SECOND: {
            return 1L;
        }
        case arrow::TimeUnit::type::MILLI: {
            return 1000L;
        }
        case arrow::TimeUnit::type::MICRO: {
            return 1000000L;
        }
        case arrow::TimeUnit::type::NANO: {
            return 1000000000L;
        }
        default:
            return 0L;
        }
    }

    static bool convert_one_datetime(CppType& datum, int64_t timestamp, const cctz::time_zone& ctz) {
        static_assert(at_is_datetime<AT>, "Invalid arrow type");

        DateTimeValue dtv;
        if (UNLIKELY(!dtv.from_unixtime(timestamp, ctz))) {
            return true;
        }
        if constexpr (lt_is_date<LT>) {
            datum.from_date(dtv.year(), dtv.month(), dtv.day());
        } else if constexpr (lt_is_datetime<LT>) {
            datum.from_timestamp(dtv.year(), dtv.month(), dtv.day(), dtv.hour(), dtv.minute(), dtv.second(), 0);
        }
        return false;
    }

    template <bool no_divide>
    static Status convert_datetime(CppType* data, const ArrowCppType* arrow_data, int num_elements,
                                   const cctz::time_zone& ctz, [[maybe_unused]] const uint8_t* null_data,
                                   [[maybe_unused]] int divisor) {
        for (int i = 0; i < num_elements; ++i) {
            if constexpr (is_nullable) {
                if (null_data[i] == DATUM_NULL) {
                    // When performing aggregation, for nullable column, we will first compare the null flag,
                    // then compare the data. We must make sure the data is consistent even for null value,
                    // In stream/broker load, the data for null date/datetime is DefaultValueGenerator,
                    // here we also set it to be DefaultValueGenerator for spark load, otherwise it will raise
                    // a problem in issue #9496
                    if constexpr (lt_is_date<LT>) {
                        data[i] = DefaultValueGenerator<DateValue>::next_value();
                    } else if constexpr (lt_is_datetime<LT>) {
                        data[i] = DefaultValueGenerator<TimestampValue>::next_value();
                    }
                    continue;
                }
            }
            bool fail;
            if constexpr (no_divide) {
                fail = convert_one_datetime(data[i], arrow_data[i], ctz);
            } else {
                fail = convert_one_datetime(data[i], arrow_data[i] / divisor, ctz);
            }
            if (fail) {
                return Status::InternalError(strings::Substitute("Illegal timestamp value($0)", arrow_data[i]));
            }
        }
        return Status::OK();
    }
    static Status apply(const arrow::Array* array, size_t array_start_idx, size_t num_elements, Column* column,
                        size_t column_start_idx, [[maybe_unused]] uint8_t* null_data,
                        [[maybe_unused]] Filter* chunk_filter, ArrowConvertContext* ctx,
                        [[maybe_unused]] ConvertFuncTree* conv_func) {
        auto* concrete_array = down_cast<const ArrowArrayType*>(array);
        auto concrete_type = std::static_pointer_cast<ArrowType>(array->type());
        auto* concrete_column = down_cast<ColumnType*>(column);

        auto* arrow_data = concrete_array->raw_values() + array_start_idx;
        concrete_column->resize(column->size() + num_elements);
        auto* data = &concrete_column->get_data().front() + column_start_idx;
        if constexpr (at_is_date<AT>) {
            for (int i = 0; i < num_elements; ++i) {
                // When performing aggregation, for nullable column, we will first compare the null flag,
                // then compare the data. We must make sure the data is consistent even for null value,
                // In stream/broker load, the data for null date/datetime is DefaultValueGenerator,
                // here we also set it to be DefaultValueGenerator for spark load, otherwise it will raise
                // a problem in issue #9496
                if constexpr (is_nullable) {
                    if (null_data[i] == DATUM_NULL) {
                        if constexpr (lt_is_date<LT>) {
                            data[i] = DefaultValueGenerator<DateValue>::next_value();
                        } else if constexpr (lt_is_datetime<LT>) {
                            data[i] = DefaultValueGenerator<TimestampValue>::next_value();
                        }
                        continue;
                    }
                }
                convert_date(data[i], arrow_data[i]);
            }
        } else if constexpr (at_is_datetime<AT>) {
            cctz::time_zone ctz;
            int64_t divisor;
            if (!TimezoneUtils::find_cctz_time_zone(concrete_type->timezone(), ctz)) {
                return Status::InternalError(strings::Substitute("Not found TimeZone($0)", concrete_type->timezone()));
            }
            divisor = time_unit_divisor(concrete_type->unit());
            if (divisor == 0) {
                return Status::InternalError(strings::Substitute("Not support TimeUnit($0)", concrete_type->unit()));
            }
            if (divisor == 1) {
                return convert_datetime<true>(data, arrow_data, num_elements, ctz, null_data, 1);
            }
            return convert_datetime<false>(data, arrow_data, num_elements, ctz, null_data, divisor);
        }
        return Status::OK();
    }
};

// Convert nested arrow type(Map,List,Struct...) to Json
Status convert_arrow_to_json(const arrow::Array* array, JsonColumn* output, size_t array_start_idx,
                             size_t num_elements);

template <ArrowTypeId AT, LogicalType LT, bool is_nullable, bool is_strict>
struct ArrowConverter<AT, LT, is_nullable, is_strict, JsonGuard<LT>> {
    static Status apply(const arrow::Array* array, size_t array_start_idx, size_t num_elements, Column* column,
                        size_t column_start_idx, [[maybe_unused]] uint8_t* null_data,
                        [[maybe_unused]] Filter* chunk_filter, ArrowConvertContext* ctx,
                        [[maybe_unused]] ConvertFuncTree* conv_func) {
        auto* json_column = down_cast<JsonColumn*>(column);
        json_column->reserve(column->size() + num_elements);

        return convert_arrow_to_json(array, json_column, array_start_idx, num_elements);
    }
};

template <typename T>
static void list_map_offsets_copy(const arrow::Array* layer, const size_t array_start_idx, const size_t num_elements,
                                  UInt32Column* col_offsets) {
    using ArrowArrayType = typename arrow::TypeTraits<T>::ArrayType;
    using OffsetsType = typename T::offset_type;
    auto* concrete_array = down_cast<const ArrowArrayType*>(layer);
    auto* arrow_offsets_data = concrete_array->raw_value_offsets() + array_start_idx;
    auto arrow_base_offset = arrow_offsets_data[0];
    arrow_offsets_data += 1;
    auto start_idx = col_offsets->size() - 1;
    col_offsets->resize(col_offsets->size() + num_elements);
    auto* offsets_data = &col_offsets->get_data().front() + start_idx;
    auto base_offset = offsets_data[0];
    offsets_data += 1;
    offsets_copy<OffsetsType>(arrow_offsets_data, arrow_base_offset, num_elements, offsets_data, base_offset);
}

template <typename T>
static arrow::Array* get_list_map_array_child(const arrow::Array* array, size_t array_start_idx = 0,
                                              size_t num_elements = 0, size_t* child_array_start_idx = nullptr,
                                              size_t* child_array_num_elements = nullptr, int8_t child_id = 0) {
    using ArrowArrayType = typename arrow::TypeTraits<T>::ArrayType;
    using OffsetsType = typename T::offset_type;
    if (child_array_start_idx && child_array_num_elements) {
        auto child_array = down_cast<const ArrowArrayType*>(array);
        *child_array_start_idx = child_array->value_offset(array_start_idx),
        *child_array_num_elements = child_array->value_offset(array_start_idx + num_elements) - *child_array_start_idx;
    }
    if constexpr (std::is_same<T, arrow::MapType>::value) {
        if (child_id == 0) {
            return down_cast<const ArrowArrayType*>(array)->keys().get();
        } else {
            return down_cast<const ArrowArrayType*>(array)->items().get();
        }
    } else {
        return down_cast<const ArrowArrayType*>(array)->values().get();
    }
}

template <ArrowTypeId AT, LogicalType LT, bool is_nullable, bool is_strict>
struct ArrowConverter<AT, LT, is_nullable, is_strict, ArrayGuard<LT>> {
    using UInt32ColumnPtr = UInt32Column::Ptr;
    static bool is_list(ArrowTypeId t) { return t == ArrowTypeId::LIST; }
    static bool is_large_list(ArrowTypeId t) { return t == ArrowTypeId::LARGE_LIST; }
    static bool is_fixed_size_list(ArrowTypeId t) { return t == ArrowTypeId::FIXED_SIZE_LIST; }
    static bool is_any_list(ArrowTypeId t) { return is_list(t) || is_large_list(t) || is_fixed_size_list(t); }

    static void fixed_size_list_map_offsets_copy(const arrow::Array* layer, const size_t array_start_idx,
                                                 const size_t num_elements, UInt32Column* col_offsets) {
        using ArrowArrayType = typename arrow::TypeTraits<arrow::FixedSizeListType>::ArrayType;
        using OffsetsType = typename arrow::FixedSizeListType::offset_type;
        auto* concrete_array = down_cast<const ArrowArrayType*>(layer);
        auto arrow_base_offset = concrete_array->value_offset(array_start_idx);
        auto start_idx = col_offsets->size() - 1;
        col_offsets->resize(col_offsets->size() + num_elements);
        auto* offsets_data = &col_offsets->get_data().front() + start_idx;
        auto base_offset = offsets_data[0];
        offsets_data += 1;

        for (auto i = 0; i < num_elements; ++i) {
            // never change following code to
            // base_offsets - arrow_base_offset + arrow_offsets_data[i],
            // that would cause underflow for unsigned int;
            offsets_data[i] = base_offset + (concrete_array->value_offset(array_start_idx + i + 1) - arrow_base_offset);
        }
    }

    static arrow::Array* get_child_array_position(const arrow::Array* array, size_t array_start_idx,
                                                  size_t num_elements, size_t* child_array_start_idx,
                                                  size_t* child_array_num_elements) {
        auto type_id = array->type_id();
        if (is_list(type_id)) {
            return get_list_map_array_child<arrow::ListType>(array, array_start_idx, num_elements,
                                                             child_array_start_idx, child_array_num_elements);
        } else if (is_large_list(type_id)) {
            return get_list_map_array_child<arrow::LargeListType>(array, array_start_idx, num_elements,
                                                                  child_array_start_idx, child_array_num_elements);
        } else if (is_fixed_size_list(type_id)) {
            return get_list_map_array_child<arrow::FixedSizeListType>(array, array_start_idx, num_elements,
                                                                      child_array_start_idx, child_array_num_elements);
        } else {
            return nullptr;
        }
    }

    static Status apply(const arrow::Array* array, size_t array_start_idx, size_t num_elements, Column* column,
                        size_t chunk_start_idx, [[maybe_unused]] uint8_t* null_data, Filter* chunk_filter,
                        ArrowConvertContext* ctx, ConvertFuncTree* conv_func) {
        auto* col_array = down_cast<ArrayColumn*>(column);
        UInt32Column* col_offsets = col_array->offsets_column().get();

        auto type_id = array->type_id();
        if (is_list(type_id)) {
            list_map_offsets_copy<arrow::ListType>(array, array_start_idx, num_elements, col_offsets);
        } else if (is_large_list(type_id)) {
            list_map_offsets_copy<arrow::LargeListType>(array, array_start_idx, num_elements, col_offsets);
        } else if (is_fixed_size_list(type_id)) {
            fixed_size_list_map_offsets_copy(array, array_start_idx, num_elements, col_offsets);
        } else {
            return Status::InternalError(strings::Substitute("Invalid arrow list type($0)", array->type()->name()));
        }

        size_t child_array_start_idx;
        size_t child_array_num_elements;
        const auto* child_array = get_child_array_position(array, array_start_idx, num_elements, &child_array_start_idx,
                                                           &child_array_num_elements);
        if (!child_array) {
            return Status::InternalError(fmt::format("Unnest arrow list type({}) fail", array->type()->name()));
        }

        Filter child_chunk_filter;
        child_chunk_filter.resize(col_array->elements_column()->size() + child_array_num_elements, 1);
        return ParquetScanner::convert_array_to_column(conv_func->children[0].get(), child_array_num_elements,
                                                       child_array, col_array->elements_column(), child_array_start_idx,
                                                       col_array->elements_column()->size(), &child_chunk_filter, ctx);
    }
};

template <ArrowTypeId AT, LogicalType LT, bool is_nullable, bool is_strict>
struct ArrowConverter<AT, LT, is_nullable, is_strict, MapGuard<LT>> {
    static Status apply(const arrow::Array* array, size_t array_start_idx, size_t num_elements, Column* column,
                        size_t chunk_start_idx, [[maybe_unused]] uint8_t* null_data, Filter* chunk_filter,
                        ArrowConvertContext* ctx, ConvertFuncTree* conv_func) {
        // offset
        auto* col_map = down_cast<MapColumn*>(column);
        UInt32Column* col_offsets = col_map->offsets_column().get();
        list_map_offsets_copy<arrow::MapType>(array, array_start_idx, num_elements, col_offsets);
        // keys, values
        size_t kv_size[] = {col_map->keys().size(), col_map->values().size()};
        ColumnPtr kv_columns[] = {col_map->keys_column(), col_map->values_column()};
        for (auto i = 0; i < 2; ++i) {
            size_t child_array_start_idx;
            size_t child_array_num_elements;
            const auto* child_array = get_list_map_array_child<arrow::MapType>(
                    array, array_start_idx, num_elements, &child_array_start_idx, &child_array_num_elements, i);
            if (!child_array) {
                return Status::InternalError(fmt::format("Unnest arrow array type({}) fail", array->type()->name()));
            }

            Filter child_chunk_filter;
            child_chunk_filter.resize(kv_size[i] + child_array_num_elements, 1);
            RETURN_IF_ERROR(ParquetScanner::convert_array_to_column(
                    conv_func->children[i].get(), child_array_num_elements, child_array, kv_columns[i],
                    child_array_start_idx, kv_size[i], &child_chunk_filter, ctx));
        }
        return Status::OK();
    }
};

template <ArrowTypeId AT, LogicalType LT, bool is_nullable, bool is_strict>
struct ArrowConverter<AT, LT, is_nullable, is_strict, StructGurad<LT>> {
    static Status apply(const arrow::Array* array, size_t array_start_idx, size_t num_elements, Column* column,
                        size_t chunk_start_idx, [[maybe_unused]] uint8_t* null_data, Filter* chunk_filter,
                        ArrowConvertContext* ctx, ConvertFuncTree* conv_func) {
        auto* struct_col = down_cast<StructColumn*>(column);
        auto* struct_array = down_cast<const arrow::StructArray*>(array);

        DCHECK_EQ(conv_func->field_names.size(), conv_func->children.size());
        for (size_t i = 0; i < conv_func->field_names.size(); i++) {
            const auto& child_name = conv_func->field_names[i];

            auto child_col = struct_col->field_column(child_name);
            auto child_array = struct_array->GetFieldByName(child_name);

            if (child_array == nullptr) {
                // default null
                DCHECK(child_col->is_nullable());
                child_col->append_nulls(num_elements);
                continue;
            }

            RETURN_IF_ERROR(ParquetScanner::convert_array_to_column(conv_func->children[i].get(), num_elements,
                                                                    child_array.get(), child_col, array_start_idx,
                                                                    chunk_start_idx, chunk_filter, ctx));
        }

        return Status::OK();
    }
};

// Convert Arrow null to any types
Status null_converter(const arrow::Array* array, size_t array_start_idx, size_t num_elements, Column* column,
                      size_t column_start_idx, uint8_t* null_data, [[maybe_unused]] Filter* chunk_filter,
                      ArrowConvertContext* ctx, [[maybe_unused]] ConvertFuncTree* conv_func) {
    if (null_data == nullptr) {
        return Status::InvalidArgument(fmt::format("The column ({}) must be nullable", ctx->current_slot->col_name()));
    }
    for (size_t i = 0; i < num_elements; i++) {
        null_data[column_start_idx + i] = 1;
    }
    column->append_default(num_elements);
    return {};
}

constexpr int32_t convert_idx(ArrowTypeId at, LogicalType lt, bool is_nullable, bool is_strict) {
    return (at << 17) | (lt << 2) | (is_nullable ? 2 : 0) | (is_strict ? 1 : 0);
}

#define ARROW_CONV_SINGLE_ENTRY_CTOR(a, b, t0, t1) \
    { convert_idx(a, b, t0, t1), &ArrowConverter<a, b, t0, t1>::apply }

#define ARROW_CONV_ENTRY_CTOR(a, b)                                                                    \
    ARROW_CONV_SINGLE_ENTRY_CTOR(a, b, false, false), ARROW_CONV_SINGLE_ENTRY_CTOR(a, b, false, true), \
            ARROW_CONV_SINGLE_ENTRY_CTOR(a, b, true, false), ARROW_CONV_SINGLE_ENTRY_CTOR(a, b, true, true)

#define ARROW_CONV_ENTRY(a, ...) DEF_BINARY_RELATION_ENTRY_SEP_COMMA(ARROW_CONV_ENTRY_CTOR, a, ##__VA_ARGS__)

#define STRICT_ARROW_CONV_ENTRY_CTOR(a, b) \
    { a, b }
#define STRICT_ARROW_CONV_ENTRY_R(a, ...) \
    DEF_BINARY_RELATION_ENTRY_SEP_COMMA_R(STRICT_ARROW_CONV_ENTRY_CTOR, a, ##__VA_ARGS__)

static const std::unordered_map<ArrowTypeId, LogicalType> global_strict_arrow_conv_table{
        STRICT_ARROW_CONV_ENTRY_R(TYPE_BOOLEAN, ArrowTypeId::BOOL),
        STRICT_ARROW_CONV_ENTRY_R(TYPE_TINYINT, ArrowTypeId::INT8, ArrowTypeId::UINT8),
        STRICT_ARROW_CONV_ENTRY_R(TYPE_SMALLINT, ArrowTypeId::INT16, ArrowTypeId::UINT16),
        STRICT_ARROW_CONV_ENTRY_R(TYPE_INT, ArrowTypeId::INT32, ArrowTypeId::UINT32),
        STRICT_ARROW_CONV_ENTRY_R(TYPE_BIGINT, ArrowTypeId::INT64, ArrowTypeId::UINT64),
        STRICT_ARROW_CONV_ENTRY_R(TYPE_FLOAT, ArrowTypeId::HALF_FLOAT, ArrowTypeId::FLOAT),
        STRICT_ARROW_CONV_ENTRY_R(TYPE_DOUBLE, ArrowTypeId::DOUBLE),
        STRICT_ARROW_CONV_ENTRY_R(TYPE_VARCHAR, ArrowTypeId::STRING, ArrowTypeId::LARGE_STRING, ArrowTypeId::BINARY,
                                  ArrowTypeId::LARGE_BINARY, ArrowTypeId::FIXED_SIZE_BINARY),
        STRICT_ARROW_CONV_ENTRY_R(TYPE_VARBINARY, ArrowTypeId::STRING, ArrowTypeId::LARGE_STRING, ArrowTypeId::BINARY,
                                  ArrowTypeId::LARGE_BINARY, ArrowTypeId::FIXED_SIZE_BINARY),
        STRICT_ARROW_CONV_ENTRY_R(TYPE_DATE, ArrowTypeId::DATE32),
        STRICT_ARROW_CONV_ENTRY_R(TYPE_DATETIME, ArrowTypeId::DATE64, ArrowTypeId::TIMESTAMP),
        STRICT_ARROW_CONV_ENTRY_R(TYPE_DECIMAL128, ArrowTypeId::DECIMAL),
        STRICT_ARROW_CONV_ENTRY_R(TYPE_JSON, ArrowTypeId::STRUCT, ArrowTypeId::MAP, ArrowTypeId::LIST),
        STRICT_ARROW_CONV_ENTRY_R(TYPE_ARRAY, ArrowTypeId::LIST, ArrowTypeId::LARGE_LIST, ArrowTypeId::FIXED_SIZE_LIST),
        STRICT_ARROW_CONV_ENTRY_R(TYPE_MAP, ArrowTypeId::MAP),
        STRICT_ARROW_CONV_ENTRY_R(TYPE_STRUCT, ArrowTypeId::STRUCT),
};

static const std::unordered_map<int32_t, ConvertFunc> global_optimized_arrow_conv_table{
        ARROW_CONV_ENTRY(ArrowTypeId::BOOL, TYPE_BOOLEAN, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::INT8, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT,
                         TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::INT8, TYPE_FLOAT, TYPE_DOUBLE, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::UINT8, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT,
                         TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::INT16, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::INT16, TYPE_FLOAT, TYPE_DOUBLE, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::UINT16, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::UINT16, TYPE_FLOAT, TYPE_DOUBLE, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::INT32, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT, TYPE_DOUBLE, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::UINT32, TYPE_INT, TYPE_BIGINT, TYPE_LARGEINT, TYPE_DOUBLE, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::INT64, TYPE_BIGINT, TYPE_LARGEINT, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::UINT64, TYPE_BIGINT, TYPE_LARGEINT, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::INT64, TYPE_BIGINT, TYPE_LARGEINT, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::UINT64, TYPE_BIGINT, TYPE_LARGEINT, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::HALF_FLOAT, TYPE_FLOAT, TYPE_DOUBLE),
        ARROW_CONV_ENTRY(ArrowTypeId::FLOAT, TYPE_FLOAT, TYPE_DOUBLE, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::DOUBLE, TYPE_DOUBLE, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::STRING, TYPE_CHAR, TYPE_VARCHAR, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::LARGE_STRING, TYPE_CHAR, TYPE_VARCHAR),
        ARROW_CONV_ENTRY(ArrowTypeId::BINARY, TYPE_VARBINARY, TYPE_CHAR, TYPE_VARCHAR),
        ARROW_CONV_ENTRY(ArrowTypeId::FIXED_SIZE_BINARY, TYPE_VARBINARY, TYPE_CHAR, TYPE_VARCHAR),
        ARROW_CONV_ENTRY(ArrowTypeId::LARGE_BINARY, TYPE_VARBINARY, TYPE_CHAR, TYPE_VARCHAR),
        ARROW_CONV_ENTRY(ArrowTypeId::DATE32, TYPE_DATE, TYPE_DATETIME, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::DATE64, TYPE_DATE, TYPE_DATETIME, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::TIMESTAMP, TYPE_DATE, TYPE_DATETIME, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::DECIMAL, TYPE_DECIMALV2, TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128),

        // JSON converters
        ARROW_CONV_ENTRY(ArrowTypeId::MAP, TYPE_MAP, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::LIST, TYPE_ARRAY, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::LARGE_LIST, TYPE_ARRAY, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::FIXED_SIZE_LIST, TYPE_ARRAY, TYPE_JSON),
        ARROW_CONV_ENTRY(ArrowTypeId::STRUCT, TYPE_STRUCT, TYPE_JSON),
};

ConvertFunc get_arrow_converter(ArrowTypeId at, LogicalType lt, bool is_nullable, bool is_strict) {
    if (at == ArrowTypeId::NA) {
        return null_converter;
    }
    auto optimized_idx = convert_idx(at, lt, is_nullable, is_strict);
    auto it = global_optimized_arrow_conv_table.find(optimized_idx);
    if (it != global_optimized_arrow_conv_table.end()) {
        return it->second;
    }
    return nullptr;
}

LogicalType get_strict_type(ArrowTypeId at) {
    auto lt_it = global_strict_arrow_conv_table.find(at);
    if (lt_it != global_strict_arrow_conv_table.end()) {
        return lt_it->second;
    }
    return TYPE_UNKNOWN;
}

static const int MAX_ERROR_MESSAGE_COUNTER = 100;

void ArrowConvertContext::report_error_message(const std::string& reason, const std::string& raw_data) {
    if (state == nullptr) return;
    if (error_message_counter > MAX_ERROR_MESSAGE_COUNTER) return;
    error_message_counter += 1;
    std::string error_msg =
            strings::Substitute("file = $0, column = $1, raw data = $2", current_file,
                                (current_slot == nullptr) ? "null" : current_slot->col_name(), raw_data);
    state->append_error_msg_to_file(error_msg, reason);
}

} // namespace starrocks
