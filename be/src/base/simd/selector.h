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

#ifdef __AVX2__
#include <emmintrin.h>
#include <immintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_acle.h>
#include <arm_neon.h>
#endif

#include <cstdint>
#include <cstring>
#include <type_traits>

#include "base/simd/simd.h"
#include "base/simd/simd_utils.h"
#include "gutil/port.h"
#include "gutil/strings/fastmem.h"

namespace starrocks {

#ifdef __AVX2__
template <typename T, bool left_const = false, bool right_const = false, std::enable_if_t<sizeof(T) == 1, int> = 1>
inline void avx2_select_if(uint8_t*& selector, T*& dst, const T*& a, const T*& b, int size) {
    constexpr int batch_width = 32;
    constexpr int data_size = sizeof(T);

    const T* dst_end = dst + size;
    __m256i const_vec_a = _mm256_setzero_si256();
    __m256i const_vec_b = _mm256_setzero_si256();
    if (dst + batch_width <= dst_end) {
        if constexpr (left_const) {
            const_vec_a = SIMDUtils::set_data(*a);
        }
        if constexpr (right_const) {
            const_vec_b = SIMDUtils::set_data(*b);
        }
    }

    while (dst + batch_width <= dst_end) {
        uint32_t mask = 0;
        const __m256i loaded_mask = SIMDUtils::load_selector_mask(selector, &mask);
        if (mask == 0) { // Select all the rhs.
            if constexpr (right_const) {
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst), const_vec_b);
            } else {
                strings::memcpy_inlined(dst, b, batch_width * data_size);
            }
        } else if (mask == 0xFFFF'FFFF) { // Select all the lhs.
            if constexpr (left_const) {
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst), const_vec_a);
            } else {
                strings::memcpy_inlined(dst, a, batch_width * data_size);
            }
        } else {
            __m256i vec_a;
            if constexpr (left_const) {
                vec_a = const_vec_a;
            } else {
                vec_a = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a));
            }
            __m256i vec_b;
            if constexpr (right_const) {
                vec_b = const_vec_b;
            } else {
                vec_b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b));
            }

            __m256i res = _mm256_blendv_epi8(vec_b, vec_a, loaded_mask);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst), res);
        }

        dst += batch_width;
        selector += batch_width;
        if constexpr (!left_const) {
            a += batch_width;
        }
        if constexpr (!right_const) {
            b += batch_width;
        }
    }
}

template <class T>
constexpr bool could_use_common_select_if() {
    return sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8;
}

// Implement 2-byte, 4-byte, and 8-byte SIMD select_if
template <typename T, bool left_const = false, bool right_const = false>
inline void avx2_select_if_common_implement(uint8_t*& selector, T*& dst, const T*& a, const T*& b, int size) {
    constexpr int batch_width = 32;
    constexpr int data_size = sizeof(T);
    static_assert(data_size == 2 || data_size == 4 || data_size == 8);

    const T* dst_end = dst + size;
    __m256i const_vec_a = _mm256_setzero_si256();
    __m256i const_vec_b = _mm256_setzero_si256();
    if (dst + batch_width <= dst_end) {
        if constexpr (left_const) {
            const_vec_a = SIMDUtils::set_data(*a);
        }
        if constexpr (right_const) {
            const_vec_b = SIMDUtils::set_data(*b);
        }
    }

    auto load_sub_a = [&](int i) -> __m256i {
        if constexpr (left_const) {
            return const_vec_a;
        } else {
            return _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a) + i);
        }
    };
    auto load_sub_b = [&](int i) -> __m256i {
        if constexpr (right_const) {
            return const_vec_b;
        } else {
            return _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b) + i);
        }
    };

    while (dst + batch_width <= dst_end) {
        uint32_t mask = 0;
        SIMDUtils::load_selector_mask(selector, &mask);

        if (mask == 0) { // Select all the rhs.
            if constexpr (right_const) {
                for (int i = 0; i < data_size; ++i) {
                    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst) + i, const_vec_b);
                }
            } else {
                strings::memcpy_inlined(dst, b, batch_width * data_size);
            }
        } else if (mask == 0xFFFF'FFFF) { // Select all the lhs.
            if constexpr (left_const) {
                for (int i = 0; i < data_size; ++i) {
                    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst) + i, const_vec_a);
                }
            } else {
                strings::memcpy_inlined(dst, a, batch_width * data_size);
            }
        } else {
            // Process one 256-bit sub-chunk at a time to avoid spilling 16 YMM registers
            // and skip loading the unselected side when a sub-chunk is homogeneous.
            constexpr uint8_t each_loop_handle_sz = 32 / data_size;
            constexpr uint32_t sub_mask_max = (1u << each_loop_handle_sz) - 1u;
            for (int i = 0; i < data_size; ++i) {
                uint32_t select_mask = (mask >> (i * each_loop_handle_sz)) & sub_mask_max;
                if (select_mask == 0) {
                    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst) + i, load_sub_b(i));
                    continue;
                }
                if (select_mask == sub_mask_max) {
                    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst) + i, load_sub_a(i));
                    continue;
                }

                const __m256i vec_a = load_sub_a(i);
                const __m256i vec_b = load_sub_b(i);
                const __m256i select_vector = SIMDUtils::expand_sub_mask<data_size>(select_mask);
                const __m256i vec_dst = _mm256_blendv_epi8(vec_b, vec_a, select_vector);
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst) + i, vec_dst);
            }
        }

        dst += batch_width;
        selector += batch_width;
        if constexpr (!left_const) {
            a += batch_width;
        }
        if constexpr (!right_const) {
            b += batch_width;
        }
    }
}

#elif defined(__ARM_NEON) && defined(__aarch64__)
template <class T>
constexpr bool neon_could_use_common_select_if() {
    return sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8;
}

template <typename T, bool left_const = false, bool right_const = false>
inline void neon_select_if_common_implement(uint8_t*& selector, T*& dst, const T*& a, const T*& b, int size) {
    const T* dst_end = dst + size;
    constexpr int data_size = sizeof(T);
    static_assert(data_size == 1 || data_size == 2 || data_size == 4 || data_size == 8);
    constexpr int neon_width = 16; // Process 16 elements per iteration
    constexpr int kLanes = neon_width / data_size;

    using VecType = SIMDUtils::NeonVecType<T>;
    VecType const_vec_a{};
    VecType const_vec_b{};
    if (dst + neon_width <= dst_end) {
        if constexpr (left_const) {
            const_vec_a = SIMDUtils::set_data(*a);
        }
        if constexpr (right_const) {
            const_vec_b = SIMDUtils::set_data(*b);
        }
    }

    auto load_sub_a = [&](int sub_idx) -> VecType {
        if constexpr (left_const) {
            return const_vec_a;
        } else {
            return SIMDUtils::load_unaligned(a + sub_idx * kLanes);
        }
    };
    auto load_sub_b = [&](int sub_idx) -> VecType {
        if constexpr (right_const) {
            return const_vec_b;
        } else {
            return SIMDUtils::load_unaligned(b + sub_idx * kLanes);
        }
    };

    // Process 16 elements of data at a time
    while (dst + neon_width <= dst_end) {
        uint64_t nibble_mask = 0;
        const uint8x16_t select_lhs_mask = SIMDUtils::load_selector_mask(selector, &nibble_mask);

        if (nibble_mask == 0) { // Select all the rhs.
            if constexpr (right_const) {
                for (int i = 0; i < data_size; ++i) {
                    SIMDUtils::store_unaligned(dst + i * kLanes, const_vec_b);
                }
            } else {
                strings::memcpy_inlined(dst, b, neon_width * data_size);
            }
        } else if (nibble_mask == 0xffff'ffff'ffff'ffffull) { // Select all the lhs.
            if constexpr (left_const) {
                for (int i = 0; i < data_size; ++i) {
                    SIMDUtils::store_unaligned(dst + i * kLanes, const_vec_a);
                }
            } else {
                strings::memcpy_inlined(dst, a, neon_width * data_size);
            }
        } else if constexpr (data_size == 1) {
            SIMDUtils::store_unaligned(dst, SIMDUtils::blend_bytes<T>(select_lhs_mask, load_sub_a(0), load_sub_b(0)));
        } else {
            constexpr uint32_t kSubBits = 4 * kLanes;
            constexpr uint64_t kSubMask = (kSubBits == 64) ? ~uint64_t(0) : ((uint64_t(1) << kSubBits) - 1ull);
            for (int i = 0; i < data_size; ++i) {
                const uint64_t sub_mask = (nibble_mask >> (i * kSubBits)) & kSubMask;
                if (sub_mask == 0) {
                    SIMDUtils::store_unaligned(dst + i * kLanes, load_sub_b(i));
                    continue;
                }
                if (sub_mask == kSubMask) {
                    SIMDUtils::store_unaligned(dst + i * kLanes, load_sub_a(i));
                    continue;
                }

                const uint8x16_t mask = SIMDUtils::expand_sub_mask<data_size>(select_lhs_mask, i);
                SIMDUtils::store_unaligned(dst + i * kLanes,
                                           SIMDUtils::blend_bytes<T>(mask, load_sub_a(i), load_sub_b(i)));
            }
        }

        dst += neon_width;
        selector += neon_width;
        if constexpr (!left_const) {
            a += neon_width;
        }
        if constexpr (!right_const) {
            b += neon_width;
        }
    }
}

#endif

} // namespace starrocks
