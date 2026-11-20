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
#include <utility>

#ifdef __AVX2__
#include <emmintrin.h>
#include <immintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

#include "base/simd/simd.h"

namespace starrocks {

class SIMDUtils {
public:
#ifdef __AVX2__
    template <class T>
    static __m256i set_data(T data) {
        // Bit-cast via memcpy to avoid strict-aliasing UB when T is float/double/DateValue/TimestampValue:
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

    // Loads 32 selector bytes, normalizes any non-zero truthy byte (0x01..0xFF) to 0xFF (0x00 -> 0x00),
    // writes the 32-bit lane bitmask to *out_mask, and returns the 256-bit byte mask vector.
    static inline __m256i load_selector_mask(const uint8_t* selector, uint32_t* out_mask) {
        __m256i loaded_mask = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(selector));
        loaded_mask = ~_mm256_cmpeq_epi8(loaded_mask, _mm256_setzero_si256());
        *out_mask = static_cast<uint32_t>(_mm256_movemask_epi8(loaded_mask));
        return loaded_mask;
    }

    // Expands a sub-chunk bitmask (16 bits for 2B, 8 bits for 4B, 4 bits for 8B) into a full 256-bit blend mask.
    template <int DataSize>
    static inline __m256i expand_sub_mask(uint32_t sub_mask) {
        static_assert(DataSize == 2 || DataSize == 4 || DataSize == 8);
        if constexpr (DataSize == 2) {
            const __m256i data_mask =
                    _mm256_setr_epi16(0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x100, 0x200, 0x400, 0x800,
                                      0x1000, 0x2000, 0x4000, static_cast<int16_t>(0x8000u));
            __m256i select_vector = _mm256_set1_epi16(static_cast<int16_t>(sub_mask));
            return _mm256_cmpeq_epi16(select_vector & data_mask, data_mask);
        } else if constexpr (DataSize == 4) {
            const __m256i data_mask = _mm256_setr_epi32(0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80);
            __m256i select_vector = _mm256_set1_epi32(static_cast<int32_t>(sub_mask));
            return _mm256_cmpeq_epi32(select_vector & data_mask, data_mask);
        } else {
            const __m256i data_mask = _mm256_setr_epi64x(0x01, 0x02, 0x04, 0x08);
            __m256i select_vector = _mm256_set1_epi64x(static_cast<int64_t>(sub_mask));
            return _mm256_cmpeq_epi64(select_vector & data_mask, data_mask);
        }
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    template <class T>
    static auto set_data(T data) {
        // Bit-cast via memcpy to avoid strict-aliasing UB when T is float/double/DateValue/TimestampValue.
        if constexpr (sizeof(T) == 1) {
            uint8_t bits = 0;
            std::memcpy(&bits, &data, sizeof(bits));
            return vdupq_n_u8(bits);
        } else if constexpr (sizeof(T) == 2) {
            uint16_t bits = 0;
            std::memcpy(&bits, &data, sizeof(bits));
            return vdupq_n_u16(bits);
        } else if constexpr (sizeof(T) == 4) {
            uint32_t bits = 0;
            std::memcpy(&bits, &data, sizeof(bits));
            return vdupq_n_u32(bits);
        } else if constexpr (sizeof(T) == 8) {
            uint64_t bits = 0;
            std::memcpy(&bits, &data, sizeof(bits));
            return vdupq_n_u64(bits);
        } else {
            static_assert(sizeof(T) == 0, "set_data only supports sizeof(T) == 1, 2, 4, or 8");
        }
    }

    template <class T>
    using NeonVecType = decltype(set_data(std::declval<T>()));

    // Loads 16 selector bytes, normalizes any non-zero truthy byte (0x01..0xFF) to 0xFF (0x00 -> 0x00),
    // writes the 64-bit nibble mask (4 bits per row) to *out_nibble_mask, and returns the 128-bit byte mask.
    static inline uint8x16_t load_selector_mask(const uint8_t* selector, uint64_t* out_nibble_mask) {
        uint8x16_t mask = vld1q_u8(selector);
        mask = vtstq_u8(mask, mask);
        *out_nibble_mask = SIMD::get_nibble_mask(mask);
        return mask;
    }

    template <class T>
    static inline NeonVecType<T> load_unaligned(const T* ptr) {
        if constexpr (sizeof(T) == 1) {
            return vld1q_u8(reinterpret_cast<const uint8_t*>(ptr));
        } else if constexpr (sizeof(T) == 2) {
            return vld1q_u16(reinterpret_cast<const uint16_t*>(ptr));
        } else if constexpr (sizeof(T) == 4) {
            return vld1q_u32(reinterpret_cast<const uint32_t*>(ptr));
        } else if constexpr (sizeof(T) == 8) {
            return vld1q_u64(reinterpret_cast<const uint64_t*>(ptr));
        } else {
            static_assert(sizeof(T) == 0, "load_unaligned only supports sizeof(T) == 1, 2, 4, or 8");
        }
    }

    template <class T>
    static inline void store_unaligned(T* ptr, NeonVecType<T> vec) {
        if constexpr (sizeof(T) == 1) {
            vst1q_u8(reinterpret_cast<uint8_t*>(ptr), vec);
        } else if constexpr (sizeof(T) == 2) {
            vst1q_u16(reinterpret_cast<uint16_t*>(ptr), vec);
        } else if constexpr (sizeof(T) == 4) {
            vst1q_u32(reinterpret_cast<uint32_t*>(ptr), vec);
        } else if constexpr (sizeof(T) == 8) {
            vst1q_u64(reinterpret_cast<uint64_t*>(ptr), vec);
        } else {
            static_assert(sizeof(T) == 0, "store_unaligned only supports sizeof(T) == 1, 2, 4, or 8");
        }
    }

    template <class T>
    static inline NeonVecType<T> blend_bytes(uint8x16_t mask_bytes, NeonVecType<T> if_true, NeonVecType<T> if_false) {
        if constexpr (sizeof(T) == 1) {
            return vbslq_u8(mask_bytes, if_true, if_false);
        } else if constexpr (sizeof(T) == 2) {
            return vbslq_u16(vreinterpretq_u16_u8(mask_bytes), if_true, if_false);
        } else if constexpr (sizeof(T) == 4) {
            return vbslq_u32(vreinterpretq_u32_u8(mask_bytes), if_true, if_false);
        } else if constexpr (sizeof(T) == 8) {
            return vbslq_u64(vreinterpretq_u64_u8(mask_bytes), if_true, if_false);
        } else {
            static_assert(sizeof(T) == 0, "blend_bytes only supports sizeof(T) == 1, 2, 4, or 8");
        }
    }

    // Expands sub-chunk sub_idx (0 <= sub_idx < DataSize) of a 16-byte 0x00/0xFF selector mask
    // into a 16-byte mask for DataSize-byte lanes via vqtbl1q_u8.
    template <int DataSize>
    static inline uint8x16_t expand_sub_mask(uint8x16_t mask16, int sub_idx) {
        static_assert(DataSize == 2 || DataSize == 4 || DataSize == 8);
        constexpr int kLanes = 16 / DataSize;
        const uint8_t base = static_cast<uint8_t>(sub_idx * kLanes);
        if constexpr (DataSize == 2) {
            const uint8x16_t index = {base,
                                      base,
                                      static_cast<uint8_t>(base + 1),
                                      static_cast<uint8_t>(base + 1),
                                      static_cast<uint8_t>(base + 2),
                                      static_cast<uint8_t>(base + 2),
                                      static_cast<uint8_t>(base + 3),
                                      static_cast<uint8_t>(base + 3),
                                      static_cast<uint8_t>(base + 4),
                                      static_cast<uint8_t>(base + 4),
                                      static_cast<uint8_t>(base + 5),
                                      static_cast<uint8_t>(base + 5),
                                      static_cast<uint8_t>(base + 6),
                                      static_cast<uint8_t>(base + 6),
                                      static_cast<uint8_t>(base + 7),
                                      static_cast<uint8_t>(base + 7)};
            return vqtbl1q_u8(mask16, index);
        } else if constexpr (DataSize == 4) {
            const uint8x16_t index = {base,
                                      base,
                                      base,
                                      base,
                                      static_cast<uint8_t>(base + 1),
                                      static_cast<uint8_t>(base + 1),
                                      static_cast<uint8_t>(base + 1),
                                      static_cast<uint8_t>(base + 1),
                                      static_cast<uint8_t>(base + 2),
                                      static_cast<uint8_t>(base + 2),
                                      static_cast<uint8_t>(base + 2),
                                      static_cast<uint8_t>(base + 2),
                                      static_cast<uint8_t>(base + 3),
                                      static_cast<uint8_t>(base + 3),
                                      static_cast<uint8_t>(base + 3),
                                      static_cast<uint8_t>(base + 3)};
            return vqtbl1q_u8(mask16, index);
        } else {
            const uint8x16_t index = {base,
                                      base,
                                      base,
                                      base,
                                      base,
                                      base,
                                      base,
                                      base,
                                      static_cast<uint8_t>(base + 1),
                                      static_cast<uint8_t>(base + 1),
                                      static_cast<uint8_t>(base + 1),
                                      static_cast<uint8_t>(base + 1),
                                      static_cast<uint8_t>(base + 1),
                                      static_cast<uint8_t>(base + 1),
                                      static_cast<uint8_t>(base + 1),
                                      static_cast<uint8_t>(base + 1)};
            return vqtbl1q_u8(mask16, index);
        }
    }
#endif

    template <class T>
    static void simd_fill(T* dest, T value, size_t count) {
        std::fill_n(dest, count, value);
    }
};

} // namespace starrocks
