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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>

#ifdef __AVX2__
#include <emmintrin.h>
#include <immintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

namespace starrocks {

class SIMDUtils {
public:
#ifdef __AVX2__
    template <class T>
    static __m256i set_data(T data) {
        // Bit-cast via memcpy to avoid strict-aliasing UB when T is float/double:
        // reinterpret_cast<int32_t*>(&float_value) and reading through it is UB
        // under -O2/-O3 -fstrict-aliasing and can miscompile.
        if constexpr (sizeof(T) == 1) {
            return _mm256_set1_epi8(data);
        } else if constexpr (sizeof(T) == 2) {
            return _mm256_set1_epi16(data);
        } else if constexpr (sizeof(T) == 4) {
            int32_t bits;
            std::memcpy(&bits, &data, sizeof(bits));
            return _mm256_set1_epi32(bits);
        } else if constexpr (sizeof(T) == 8) {
            int64_t bits;
            std::memcpy(&bits, &data, sizeof(bits));
            return _mm256_set1_epi64x(bits);
        } else {
            static_assert(sizeof(T) == 0, "set_data only supports sizeof(T) == 1, 2, 4, or 8");
        }
    }
#endif

    template <class T>
    static void simd_fill(T* dest, T value, size_t count) {
        std::fill_n(dest, count, value);
    }
};

} // namespace starrocks
