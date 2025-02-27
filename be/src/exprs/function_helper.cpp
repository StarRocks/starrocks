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

#include "function_helper.h"

#include <util/raw_container.h>

#include "simd/multi_version.h"

namespace starrocks {

NullColumn::MutablePtr FunctionHelper::union_nullable_column(const ColumnPtr& v1, const ColumnPtr& v2) {
    // union nullable column
    MutableColumnPtr result;
    if (v1->is_nullable() && v2->is_nullable()) {
        const auto& n1 = ColumnHelper::as_raw_column<NullableColumn>(v1)->null_column();
        const auto& n2 = ColumnHelper::as_raw_column<NullableColumn>(v2)->null_column();
        if (!v1->has_null()) {
            result = (std::move(*n2)).mutate();
        }
        if (!v2->has_null()) {
            result = (std::move(*n1)).mutate();
        }
        return union_null_column(n1, n2);
    } else if (v1->is_nullable()) {
        auto& v1_null = ColumnHelper::as_raw_column<NullableColumn>(v1)->null_column();
        result = (std::move(*v1_null)).mutate();
    } else if (v2->is_nullable()) {
        auto& v2_null = ColumnHelper::as_raw_column<NullableColumn>(v2)->null_column();
        result = (std::move(*v2_null)).mutate();
    } else {
        return nullptr;
    }

    return NullColumn::static_pointer_cast(std::move(result));
}

void FunctionHelper::union_produce_nullable_column(const ColumnPtr& v1, const ColumnPtr& v2,
                                                   NullColumnPtr* produce_null_column) {
    auto* result = (*produce_null_column)->get_data().data();

    if (v1->has_null()) {
        auto* null1 = down_cast<const NullableColumn*>(v1.get())->null_column()->get_data().data();

        int size = v1->size();
        for (int i = 0; i < size; ++i) {
            result[i] = result[i] | null1[i];
        }
    }

    if (v2->has_null()) {
        auto* null2 = down_cast<const NullableColumn*>(v2.get())->null_column()->get_data().data();

        int size = v2->size();
        for (int i = 0; i < size; ++i) {
            result[i] = result[i] | null2[i];
        }
    }
}

void FunctionHelper::union_produce_nullable_column(const ColumnPtr& v1, NullColumnPtr* produce_null_column) {
    auto* result = (*produce_null_column)->get_data().data();

    if (v1->has_null()) {
        auto* null1 = down_cast<const NullableColumn*>(v1.get())->null_column()->get_data().data();

        int size = v1->size();
        for (int i = 0; i < size; ++i) {
            result[i] = result[i] | null1[i];
        }
    }
}

MFV_AVX512(void union_null_column_impl(uint8_t* dest, const uint8_t* v1, const uint8_t* v2, const size_t bytes) {
    constexpr auto SIMD_SIZE = sizeof(__m512i);
    const auto null1_end = v1 + bytes;
    const auto null1_simd_end = v1 + (bytes & ~(SIMD_SIZE - 1));

    for (; v1 < null1_simd_end; v1 += SIMD_SIZE, v2 += SIMD_SIZE, dest += SIMD_SIZE) {
        _mm512_storeu_si512((__m512i*)dest,
                            _mm512_or_si512(_mm512_loadu_si512((__m512i*)v1), _mm512_loadu_si512((__m512i*)v2)));
    }
    for (; v1 < null1_end; ++v1, ++v2, ++dest) {
        *dest = *v1 | *v2;
    }
})

MFV_AVX2(void union_null_column_impl(uint8_t* dest, const uint8_t* v1, const uint8_t* v2, const size_t bytes) {
    constexpr auto SIMD_SIZE = sizeof(__m256i);
    const auto null1_end = v1 + bytes;
    const auto null1_simd_end = v1 + (bytes & ~(SIMD_SIZE - 1));

    for (; v1 < null1_simd_end; v1 += SIMD_SIZE, v2 += SIMD_SIZE, dest += SIMD_SIZE) {
        _mm256_storeu_si256((__m256i*)dest,
                            _mm256_or_si256(_mm256_loadu_si256((__m256i*)v1), _mm256_loadu_si256((__m256i*)v2)));
    }
    for (; v1 < null1_end; ++v1, ++v2, ++dest) {
        *dest = *v1 | *v2;
    }
})

MFV_SSE42(void union_null_column_impl(uint8_t* dest, const uint8_t* v1, const uint8_t* v2, const size_t bytes) {
    constexpr auto SIMD_SIZE = sizeof(__m128i);
    const auto null1_end = v1 + bytes;
    const auto null1_simd_end = v1 + (bytes & ~(SIMD_SIZE - 1));

    for (; v1 < null1_simd_end; v1 += SIMD_SIZE, v2 += SIMD_SIZE, dest += SIMD_SIZE) {
        _mm_storeu_si128((__m128i*)dest, _mm_or_si128(_mm_loadu_si128((__m128i*)v1), _mm_loadu_si128((__m128i*)v2)));
    }
    for (; v1 < null1_end; ++v1, ++v2, ++dest) {
        *dest = *v1 | *v2;
    }
})

MFV_DEFAULT(void union_null_column_impl(uint8_t* dest, const uint8_t* v1, const uint8_t* v2, const size_t bytes) {
    for (const auto null1_end = v1 + bytes; v1 < null1_end; ++v1, ++v2, ++dest) {
        *dest = *v1 | *v2;
    }
})

NullColumn::MutablePtr FunctionHelper::union_null_column(const NullColumnPtr& v1, const NullColumnPtr& v2) {
    // union null column
    auto null1_begin = (uint8_t*)v1->get_data().data();
    auto null2_begin = (uint8_t*)v2->get_data().data();

    const size_t row_num = v1->size();
    NullColumn::MutablePtr null_result = NullColumn::create();

    auto& result_data = null_result->get_data();
    raw::make_room(&result_data, row_num);
    auto result_begin = (uint8_t*)result_data.data();
    const size_t bytes_size = sizeof(NullColumn::ValueType) * row_num;

    union_null_column_impl(result_begin, null1_begin, null2_begin, bytes_size);
    return null_result;
}

ColumnPtr FunctionHelper::merge_column_and_null_column(ColumnPtr&& column, NullColumnPtr&& null_column) {
    if (column->only_null()) {
        return std::move(column);
    } else if (column->is_constant()) {
        auto* const_column = down_cast<ConstColumn*>(column.get());
        const auto& data_column = const_column->data_column();
        auto new_data_column = (std::move(*data_column)).mutate();
        new_data_column->assign(null_column->size(), 0);
        return NullableColumn::create(std::move(new_data_column), std::move(null_column));
    } else if (column->is_nullable()) {
        DCHECK_EQ(column->size(), null_column->size());
        auto* nullable_column = down_cast<NullableColumn*>(column.get());
        auto new_null_column = union_null_column(nullable_column->null_column(), null_column);
        return NullableColumn::create(std::move(nullable_column->data_column()), std::move(new_null_column));
    } else {
        return NullableColumn::create(std::move(column), std::move(null_column));
    }
}

} // namespace starrocks
