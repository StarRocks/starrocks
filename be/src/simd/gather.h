// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <type_traits>
#ifdef __AVX2__
#include <emmintrin.h>
#include <immintrin.h>
#endif

#include <cstdint>

namespace starrocks::vectorized {

struct SIMDGather {
    // https://johnysswlab.com/when-vectorization-hits-the-memory-wall-investigating-the-avx2-memory-gather-instruction
    // 512K
    static constexpr const int max_process_size = 512 * 1024;
    // b[i] = a[c[i]];
    // T was int32_t or uint32_t
    template <class TB, class TC>
    static void gather(TB* b, const int16_t* a, const TC* c, size_t buckets, int num_rows) {
        static_assert(sizeof(TB) == 4);
        static_assert(std::is_integral_v<TB>);
        static_assert(sizeof(TC) == 4);
        static_assert(std::is_integral_v<TC>);
        int i = 0;
#ifdef __AVX2__
        if (buckets < max_process_size) {
            __m256i mask = _mm256_set1_epi32(0xFF);
            for (; i + 8 <= num_rows; i += 8) {
                __m256i loaded = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(c));
                __m256i gathered = _mm256_i32gather_epi32((int32_t*)a, loaded, 2);
                gathered = _mm256_and_si256(gathered, mask);
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(b), gathered);
                c += 8;
                b += 8;
            }
            _mm256_zeroupper();
        }
#endif
        for (; i < num_rows; i++) {
            *b = a[*c];
            b++;
            c++;
        }
    }
};
} // namespace starrocks::vectorized