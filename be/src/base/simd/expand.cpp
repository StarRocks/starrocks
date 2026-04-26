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

#include "base/simd/expand.h"

#include "base/simd/multi_version.h"

namespace SIMD::Expand {

MFV_DEFAULT(void expand_load_selection_i32(int32_t* dst_data, const int32_t* src_data, const uint8_t* nulls,
                                           size_t count) { expand_load_branchless(dst_data, src_data, nulls, count); });

MFV_AVX512VLBW(void expand_load_selection_i32(int32_t* dst_data, const int32_t* src_data, const uint8_t* nulls,
                                              size_t count) {
    size_t cnt = 0;
    size_t i = 0;
    for (; i + 16 <= count; i += 16) {
        __mmask16 null_mask;
        __m128i mask = _mm_loadu_epi8(&nulls[i]);
        null_mask = _mm_cmp_epi8_mask(mask, _mm_setzero_si128(), _MM_CMPINT_EQ);
        __m512i loaded = _mm512_maskz_expandloadu_epi32(null_mask, &src_data[cnt]);
        cnt += _mm_popcnt_u32(null_mask);
        _mm512_storeu_epi32(&dst_data[i], loaded);
    }
    for (; i < count; ++i) {
        dst_data[i] = src_data[cnt];
        cnt += !nulls[i];
    }
});

MFV_DEFAULT(void expand_load_selection_i64(int64_t* dst_data, const int64_t* src_data, const uint8_t* nulls,
                                           size_t count) { expand_load_branchless(dst_data, src_data, nulls, count); });

MFV_AVX512VLBW(void expand_load_selection_i64(int64_t* dst_data, const int64_t* src_data, const uint8_t* nulls,
                                              size_t count) {
    size_t cnt = 0;
    size_t i = 0;
    for (; i + 16 <= count; i += 16) {
        __m128i mask = _mm_loadu_epi8(&nulls[i]);
        __mmask16 null_mask = _mm_cmp_epi8_mask(mask, _mm_setzero_si128(), _MM_CMPINT_EQ);

        __m512i loaded;

        // process low mask
        __mmask8 mask_lo = null_mask;
        loaded = _mm512_maskz_expandloadu_epi64(mask_lo, &src_data[cnt]);
        _mm512_storeu_epi64(&dst_data[i], loaded);
        cnt += _mm_popcnt_u32(mask_lo);

        __mmask8 mask_hi = null_mask >> 8;
        loaded = _mm512_maskz_expandloadu_epi64(mask_hi, &src_data[cnt]);
        _mm512_storeu_epi64(&dst_data[i + 8], loaded);
        cnt += _mm_popcnt_u32(mask_hi);
    }
    for (; i < count; ++i) {
        dst_data[i] = src_data[cnt];
        cnt += !nulls[i];
    }
})

void expand_load_simd(int32_t* dst_data, const int32_t* src_data, const uint8_t* nulls, size_t count) {
    expand_load_selection_i32(dst_data, src_data, nulls, count);
}

void expand_load_simd(int64_t* dst_data, const int64_t* src_data, const uint8_t* nulls, size_t count) {
    expand_load_selection_i64(dst_data, src_data, nulls, count);
}

} // namespace SIMD::Expand
