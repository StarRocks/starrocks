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

#include <type_traits>
#ifdef __AVX2__
#include <emmintrin.h>
#include <immintrin.h>
#endif

#include <cstdint>

namespace starrocks {

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
            // gather will collect data of size sizeof(int32)
            // we only need the lower 16 bits
            // eg:
            // a = [0x12 0x34 0x56 0x78 0x9a 0x...]
            // gather (a, [0,1,2,3], 2) will be:
            // [0x12 0x32 0x56 0x78] [0x56 0x78 0x9a..0.] [....]
            // use will use mask to get lower 16 bits
            // [0x12 0x32 0x00 0x00] [0x56 0x78 0x00 0x00] [....]
            __m256i mask = _mm256_set1_epi32(0xFFFF);
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
} // namespace starrocks
