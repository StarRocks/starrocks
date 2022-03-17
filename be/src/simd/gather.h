// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

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
    // b, c should be aligned
    static void aligned_gather(const uint32_t* a, const uint32_t* c, uint32_t* b, size_t buckets, int num_rows) {
        int i = 0;
#ifdef __AVX2__
        if (buckets < max_process_size) {
            for (; i + 8 <= num_rows; i += 8) {
                __m256i loaded = _mm256_load_si256(reinterpret_cast<const __m256i*>(c));
                __m256i gathered = _mm256_i32gather_epi32((int32_t*)a, loaded, 4);
                _mm256_store_si256(reinterpret_cast<__m256i*>(b), gathered);
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