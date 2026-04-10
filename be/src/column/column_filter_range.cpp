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

#include "column/column_filter_range.h"

#include <cstring>

#ifdef __x86_64__
#include <immintrin.h>
#endif

#if defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_acle.h>
#include <arm_neon.h>
#endif

#include "base/phmap/phmap.h"
#include "base/simd/simd.h"
#include "base/types/decimal12.h"
#include "base/types/int256.h"
#include "base/types/int96.h"
#include "base/types/uint24.h"
#include "types/date_value.h"
#include "types/decimalv2_value.h"
#include "types/timestamp_value.h"

namespace starrocks::column_filter_range {

template <typename T, bool avx512f>
size_t t_filter_range(const Filter& filter, T* dst_data, const T* src_data, size_t from, size_t to) {
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
            const T* src = src_data + start_offset + SHIFT;       \
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

// Explicit instantiation for t_filter_range
#define INSTANTIATE_T_FILTER_RANGE(T)                                                     \
    template size_t t_filter_range<T, true>(const Filter&, T*, const T*, size_t, size_t); \
    template size_t t_filter_range<T, false>(const Filter&, T*, const T*, size_t, size_t);

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

} // namespace starrocks::column_filter_range
