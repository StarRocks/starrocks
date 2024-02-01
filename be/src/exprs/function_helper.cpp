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

#ifdef __x86_64__
#include <immintrin.h>
#elif defined(__aarch__64)
#include "avx2ki.h"
#endif

#include <util/raw_container.h>

namespace starrocks {

NullColumnPtr FunctionHelper::union_nullable_column(const ColumnPtr& v1, const ColumnPtr& v2) {
    // union nullable column
    ColumnPtr result;
    if (v1->is_nullable() && v2->is_nullable()) {
        const auto& n1 = ColumnHelper::as_raw_column<NullableColumn>(v1)->null_column();
        const auto& n2 = ColumnHelper::as_raw_column<NullableColumn>(v2)->null_column();
        if (!v1->has_null()) {
            result = n2->clone();
        }
        if (!v2->has_null()) {
            result = n1->clone();
        }
        return union_null_column(n1, n2);
    } else if (v1->is_nullable()) {
        result = ColumnHelper::as_raw_column<NullableColumn>(v1)->null_column()->clone();
    } else if (v2->is_nullable()) {
        result = ColumnHelper::as_raw_column<NullableColumn>(v2)->null_column()->clone();
    } else {
        DCHECK(false);
        return nullptr;
    }

    return ColumnHelper::cast_to<TYPE_NULL>(result);
}

void FunctionHelper::union_produce_nullable_column(const ColumnPtr& v1, const ColumnPtr& v2,
                                                   NullColumnPtr* produce_null_column) {
    auto* result = (*produce_null_column)->get_data().data();

    if (v1->has_null()) {
        auto* null1 = down_cast<NullableColumn*>(v1.get())->null_column()->get_data().data();

        int size = v1->size();
        for (int i = 0; i < size; ++i) {
            result[i] = result[i] | null1[i];
        }
    }

    if (v2->has_null()) {
        auto* null2 = down_cast<NullableColumn*>(v2.get())->null_column()->get_data().data();

        int size = v2->size();
        for (int i = 0; i < size; ++i) {
            result[i] = result[i] | null2[i];
        }
    }
}

void FunctionHelper::union_produce_nullable_column(const ColumnPtr& v1, NullColumnPtr* produce_null_column) {
    auto* result = (*produce_null_column)->get_data().data();

    if (v1->has_null()) {
        auto* null1 = down_cast<NullableColumn*>(v1.get())->null_column()->get_data().data();

        int size = v1->size();
        for (int i = 0; i < size; ++i) {
            result[i] = result[i] | null1[i];
        }
    }
}

NullColumnPtr FunctionHelper::union_null_column(const NullColumnPtr& v1, const NullColumnPtr& v2) {
    // union null column
    auto null1_begin = (uint8_t*)v1->get_data().data();
    auto null2_begin = (uint8_t*)v2->get_data().data();

    const size_t row_num = v1->size();
    NullColumnPtr null_result = NullColumn::create();

    auto& result_data = null_result->get_data();
    raw::make_room(&result_data, row_num);
    auto result_begin = (uint8_t*)result_data.data();
    const size_t bytes_size = sizeof(NullColumn::ValueType) * row_num;
    const auto null1_end = null1_begin + bytes_size;

    auto null1_curr = null1_begin;
    auto null2_curr = null2_begin;
    auto result_curr = result_begin;
#if defined(__AVX2__) || defined(__aarch64__)
    constexpr auto AVX2_SIZE = sizeof(__m256i);
    const auto null1_avx2_end = null1_begin + (bytes_size & ~(AVX2_SIZE - 1));
    for (; null1_curr < null1_avx2_end; null1_curr += AVX2_SIZE, null2_curr += AVX2_SIZE, result_curr += AVX2_SIZE) {
        _mm256_storeu_si256((__m256i*)result_curr, _mm256_or_si256(_mm256_loadu_si256((__m256i*)null1_curr),
                                                                   _mm256_loadu_si256((__m256i*)null2_curr)));
    }
#elif defined(__SSE2__)
    constexpr auto SSE2_SIZE = sizeof(__m128i);
    const auto null1_sse2_end = null1_begin + (bytes_size & ~(SSE2_SIZE - 1));
    for (; null1_curr < null1_sse2_end; null1_curr += SSE2_SIZE, null2_curr += SSE2_SIZE, result_curr += SSE2_SIZE) {
        _mm_storeu_si128((__m128i*)result_curr,
                         _mm_or_si128(_mm_loadu_si128((__m128i*)null1_curr), _mm_loadu_si128((__m128i*)null2_curr)));
    }
#endif
    for (; null1_curr < null1_end; ++null1_curr, ++null2_curr, ++result_curr) {
        *result_curr = *null1_curr | *null2_curr;
    }
    return null_result;
}

ColumnPtr FunctionHelper::merge_column_and_null_column(ColumnPtr&& column, NullColumnPtr&& null_column) {
    if (column->only_null()) {
        return std::move(column);
    } else if (column->is_constant()) {
        auto* const_column = down_cast<ConstColumn*>(column.get());
        const auto& data_column = const_column->data_column();
        auto new_data_column = data_column->clone();
        new_data_column->assign(null_column->size(), 0);
        return NullableColumn::create(std::move(new_data_column), std::move(null_column));
    } else if (column->is_nullable()) {
        DCHECK_EQ(column->size(), null_column->size());
        auto* nullable_column = down_cast<NullableColumn*>(column.get());
        auto new_null_column = union_null_column(nullable_column->null_column(), null_column);
        return NullableColumn::create(std::move(nullable_column->data_column()), new_null_column);
    } else {
        return NullableColumn::create(std::move(column), std::move(null_column));
    }
}

} // namespace starrocks
