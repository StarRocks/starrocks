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
        if constexpr (sizeof(T) == 1) {
            return _mm256_set1_epi8(data);
        } else if constexpr (sizeof(T) == 2) {
            return _mm256_set1_epi16(data);
        } else if constexpr (sizeof(T) == 4) {
            return _mm256_set1_epi32(*reinterpret_cast<int32_t*>(&data));
        } else if constexpr (sizeof(T) == 8) {
            return _mm256_set1_epi64x(*reinterpret_cast<int64_t*>(&data));
        } else {
            static_assert(sizeof(T) == 0, "set_data only supports sizeof(T) == 1, 2, 4, or 8");
        }
    }
#endif

    // SIMD-optimized fill: fills array with repeated value
    // Uses AVX2 when available, falls back to NEON on aarch64, else scalar loop.
    template <class T>
    static void simd_fill(T* dest, T value, size_t count) {
        if (count == 0) return;

#ifdef __AVX2__
        constexpr size_t elements_per_vector = 32 / sizeof(T);

        if constexpr (sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8) {
            __m256i v_value = set_data(value);

            size_t i = 0;
            // Process 32 bytes at a time (unrolled 2x)
            for (; i + elements_per_vector * 2 <= count; i += elements_per_vector * 2) {
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), v_value);
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i + elements_per_vector), v_value);
            }
            // Process remaining full vectors
            for (; i + elements_per_vector <= count; i += elements_per_vector) {
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), v_value);
            }
            // Scalar tail
            for (; i < count; ++i) {
                dest[i] = value;
            }
            return;
        }
#elif defined(__ARM_NEON) && defined(__aarch64__)
        if constexpr (sizeof(T) == 1) {
            const uint8x16_t v_value = vdupq_n_u8(static_cast<uint8_t>(value));
            size_t i = 0;
            for (; i + 16 <= count; i += 16) {
                vst1q_u8(reinterpret_cast<uint8_t*>(dest + i), v_value);
            }
            for (; i < count; ++i) {
                dest[i] = value;
            }
            return;
        } else if constexpr (sizeof(T) == 2) {
            const uint16x8_t v_value = vdupq_n_u16(static_cast<uint16_t>(value));
            size_t i = 0;
            for (; i + 8 <= count; i += 8) {
                vst1q_u16(reinterpret_cast<uint16_t*>(dest + i), v_value);
            }
            for (; i < count; ++i) {
                dest[i] = value;
            }
            return;
        } else if constexpr (sizeof(T) == 4) {
            uint32_t v_bits = 0;
            memcpy(&v_bits, &value, sizeof(v_bits));
            const uint32x4_t v_value = vdupq_n_u32(v_bits);
            size_t i = 0;
            for (; i + 4 <= count; i += 4) {
                vst1q_u32(reinterpret_cast<uint32_t*>(dest + i), v_value);
            }
            for (; i < count; ++i) {
                dest[i] = value;
            }
            return;
        } else if constexpr (sizeof(T) == 8) {
            uint64_t v_bits = 0;
            memcpy(&v_bits, &value, sizeof(v_bits));
            const uint64x2_t v_value = vdupq_n_u64(v_bits);
            size_t i = 0;
            for (; i + 2 <= count; i += 2) {
                vst1q_u64(reinterpret_cast<uint64_t*>(dest + i), v_value);
            }
            for (; i < count; ++i) {
                dest[i] = value;
            }
            return;
        }
#endif

        // Fallback: scalar fill
        for (size_t i = 0; i < count; ++i) {
            dest[i] = value;
        }
    }
};

} // namespace starrocks
