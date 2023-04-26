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
#endif

#include <cstdint>
#include <type_traits>

#include "column/type_traits.h"
#include "gutil/port.h"
#include "simd/simd_utils.h"
#include "types/logical_type.h"

namespace starrocks {

#ifdef __AVX2__
template <typename T, bool left_const = false, bool right_const = false, std::enable_if_t<sizeof(T) == 1, int> = 1>
inline void avx2_select_if(uint8_t*& selector, T*& dst, const T*& a, const T*& b, int size) {
    const T* dst_end = dst + size;
    while (dst + 32 < dst_end) {
        __m256i loaded_mask = _mm256_loadu_si256(reinterpret_cast<__m256i*>(selector));
        loaded_mask = _mm256_cmpeq_epi8(loaded_mask, _mm256_setzero_si256());
        loaded_mask = ~loaded_mask;
        __m256i vec_a;
        __m256i vec_b;
        if constexpr (!left_const) {
            vec_a = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a));
        } else {
            vec_a = _mm256_set1_epi8(*a);
        }
        if constexpr (!right_const) {
            vec_b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b));
        } else {
            vec_b = _mm256_set1_epi8(*b);
        }
        __m256i res = _mm256_blendv_epi8(vec_b, vec_a, loaded_mask);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst), res);
        dst += 32;
        selector += 32;
        if (!left_const) {
            a += 32;
        }
        if (!right_const) {
            b += 32;
        }
    }
}

// _mm256_blend_epi32
template <typename T, std::enable_if_t<sizeof(T) == 4, int> = 4>
inline void avx2_select_if(uint8_t*& selector, T*& dst, const T*& a, const T*& b, int size) {
    const T* dst_end = dst + size;

    while (dst + 8 < dst_end) {
        uint64_t value = UNALIGNED_LOAD64(selector);
        __m128i v = _mm_set1_epi64x(value);
        __m256i loaded_mask = _mm256_cvtepi8_epi32(v);
        __m256i cond = _mm256_cmpeq_epi8(loaded_mask, _mm256_setzero_si256());
        cond = ~cond;

        // Mask Shuffle
        // convert 0x 10 00 00 00 14 00 00 00
        // to      0x 00 00 00 10 00 00 00 14
        __m256i mask = _mm256_set_epi8(0x0c, 0xff, 0xff, 0xff, 0x08, 0xff, 0xff, 0xff, 0x04, 0xff, 0xff, 0xff, 0x00,
                                       0xff, 0xff, 0xff, 0x0c, 0xff, 0xff, 0xff, 0x08, 0xff, 0xff, 0xff, 0x04, 0xff,
                                       0xff, 0xff, 0x00, 0xff, 0xff, 0xff);
        cond = _mm256_shuffle_epi8(cond, mask);

        __m256i vec_a = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a));
        __m256i vec_b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b));
        __m256 res =
                _mm256_blendv_ps(_mm256_castsi256_ps(vec_b), _mm256_castsi256_ps(vec_a), _mm256_castsi256_ps(cond));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst), _mm256_castps_si256(res));

        dst += 8;
        selector += 8;
        a += 8;
        b += 8;
    }
}

template <class T>
constexpr bool could_use_common_select_if() {
    return sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8;
}

// implentment int16/int32/float/int64/double SIMD select_if
template <typename T, bool left_const = false, bool right_const = false>
inline void avx2_select_if_common_implement(uint8_t*& selector, T*& dst, const T*& a, const T*& b, int size) {
    const T* dst_end = dst + size;
    constexpr int data_size = sizeof(T);

    while (dst + 32 < dst_end) {
        // load selector mask from selector
        __m256i loaded_mask = _mm256_loadu_si256(reinterpret_cast<__m256i*>(selector));
        loaded_mask = _mm256_cmpeq_epi8(loaded_mask, _mm256_setzero_si256());
        loaded_mask = ~loaded_mask;
        uint32_t mask = _mm256_movemask_epi8(loaded_mask);

        __m256i vec_a[data_size];
        __m256i vec_b[data_size];
        __m256i vec_dst[data_size];

        // load data from data vector
        for (int i = 0; i < data_size; ++i) {
            if constexpr (!left_const) {
                vec_a[i] = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a) + i);
            } else {
                vec_a[i] = SIMDUtils::set_data(*a);
            }
            if constexpr (!right_const) {
                vec_b[i] = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b) + i);
            } else {
                vec_b[i] = SIMDUtils::set_data(*b);
            }
        }

        // Each loop can handle the size of the selection vector,
        // using int16 as an example,
        // because each mask outer loop has to handle 32 int16, but since m256 can only handle 16 int16,
        // we need to use two loops to handle 16 each, so each loop needs to use 0xFFFF to get 16 bits to process.

        // In addition, since data_size is constexpr, the loop here will be expanded by the compiler
        constexpr uint32_t mask_table[] = {0, 0xFFFFFFFF, 0xFFFF, 0, 0xFF, 0, 0, 0, 0x0F, 0, 0, 0, 0, 0, 0, 0, 0x03};
        constexpr uint8_t each_loop_handle_sz = 32 / data_size;
        for (int i = 0; i < data_size; ++i) {
            uint32_t select_mask = mask & mask_table[data_size];
            // how to get mask from load mask
            __m256i select_vector;
            if constexpr (data_size == 2) {
                // First expand the 16 bits into m256
                // each bit expand to 4 bytes
                // 0b0110 -> 0x00 0x00 0x00 0x01 0x00 0x01 0x00 0x00
                select_vector = _mm256_set1_epi16(select_mask);
                // set is usually a more costly operation, but since it is a constant,
                // there is no need to worry about the performance impact
                // data_mask could help acquire bit at x in locate x
                const __m256i data_mask = _mm256_setr_epi16(0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x100,
                                                            0x200, 0x400, 0x800, 0x1000, 0x2000, 0x4000, 0x8000);
                // eg: input vec is (0xabcd), (0xabcd)...x16
                // data_mask bitwise and input_vec was
                // (0xabcd and 0x01),(0xabcd and 0x02)...x16
                // then select_vector was the expended of 16 bits mask
                select_vector &= data_mask;
                // AVX/AVX2 couldn't provide a compare not equal
                // we use compare and NOT instead of it
                select_vector = _mm256_cmpeq_epi16(select_vector, _mm256_setzero_si256());
                select_vector = ~select_vector;
            } else if constexpr (data_size == 4) {
                select_vector = _mm256_set1_epi8(select_mask);
                // the same method as int16
                // expand 8 bits into m256
                // each bit expand to 4 bytes
                // clang-format off
                const __m256i data_mask = _mm256_setr_epi8(
                        0x00, 0x00, 0x00, 0x01, 
                        0x00, 0x00, 0x00, 0x02, 
                        0x00, 0x00, 0x00, 0x04, 
                        0x00, 0x00, 0x00, 0x08,
                        0x00, 0x00, 0x00, 0x10, 
                        0x00, 0x00, 0x00, 0x20, 
                        0x00, 0x00, 0x00, 0x40, 
                        0x00, 0x00, 0x00, 0x80);
                // clang-format on
                select_vector &= data_mask;
                select_vector = _mm256_cmpeq_epi32(select_vector, _mm256_setzero_si256());
                select_vector = ~select_vector;
            } else if constexpr (data_size == 8) {
                select_vector = _mm256_set1_epi8(select_mask);
                // the same method as int16
                // each bit expand to 8 bytes
                // clang-format off
                const __m256i data_mask = _mm256_setr_epi8(
                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 
                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 
                        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08);
                // clang-format on
                select_vector &= data_mask;
                select_vector = _mm256_cmpeq_epi64(select_vector, _mm256_setzero_si256());
                select_vector = ~select_vector;
            }
            // use blendv
            vec_dst[i] = _mm256_blendv_epi8(vec_b[i], vec_a[i], select_vector);

            mask >>= each_loop_handle_sz;
        }

        for (int i = 0; i < data_size; ++i) {
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst) + i, vec_dst[i]);
        }

        dst += 32;
        selector += 32;
        if (!left_const) {
            a += 32;
        }
        if (!right_const) {
            b += 32;
        }
    }
}
#endif

// SIMD selector
// only support PrimaryType Arithmetic and DATE
template <LogicalType TYPE>
class SIMD_selector {
public:
    using Container = typename RunTimeColumnType<TYPE>::Container;
    using CppType = RunTimeCppType<TYPE>;
    using SelectVec = uint8_t*;

    // select if var var
    // dst[i] = select_vec[i] ? a[i] : b[i]
    static void select_if(SelectVec select_vec, Container& dst, const Container& a, const Container& b) {
        int size = dst.size();
        auto* start_dst = dst.data();
        auto* end_dst = dst.data() + size;

        auto* start_a = a.data();
        auto* start_b = b.data();

#ifdef __AVX2__
        if constexpr (sizeof(CppType) == 1) {
            avx2_select_if(select_vec, start_dst, start_a, start_b, size);
        } else if constexpr (sizeof(CppType) == 4) {
            avx2_select_if(select_vec, start_dst, start_a, start_b, size);
        } else if constexpr (could_use_common_select_if<CppType>()) {
            avx2_select_if_common_implement(select_vec, start_dst, start_a, start_b, size);
        }
#endif

        while (start_dst < end_dst) {
            *start_dst = *select_vec ? *start_a : *start_b;
            select_vec++;
            start_dst++;
            start_a++;
            start_b++;
        }
    }

    // select if const var
    // dst[i] = select_vec[i] ? a : b[i]
    static void select_if(SelectVec select_vec, Container& dst, CppType a, const Container& b) {
        int size = dst.size();
        auto* start_dst = dst.data();
        auto* end_dst = dst.data() + size;

        [[maybe_unused]] const CppType* start_a = &a;
        auto* start_b = b.data();

#ifdef __AVX2__
        if constexpr (sizeof(RunTimeCppType<TYPE>) == 1) {
            avx2_select_if<CppType, true, false>(select_vec, start_dst, start_a, start_b, size);
        } else if constexpr (could_use_common_select_if<CppType>()) {
            avx2_select_if_common_implement<CppType, true, false>(select_vec, start_dst, start_a, start_b, size);
        }
#endif

        while (start_dst < end_dst) {
            *start_dst = *select_vec ? a : *start_b;
            select_vec++;
            start_dst++;
            start_b++;
        }
    }

    // select if var const
    // dst[i] = select_vec[i] ? a[i] : b
    static void select_if(SelectVec select_vec, Container& dst, const Container& a, const CppType b) {
        int size = dst.size();
        auto* start_dst = dst.data();
        auto* end_dst = dst.data() + size;

        auto* start_a = a.data();
        [[maybe_unused]] const CppType* start_b = &b;

#ifdef __AVX2__
        if constexpr (sizeof(RunTimeCppType<TYPE>) == 1) {
            avx2_select_if<CppType, false, true>(select_vec, start_dst, start_a, start_b, size);
        } else if constexpr (could_use_common_select_if<CppType>()) {
            avx2_select_if_common_implement<CppType, false, true>(select_vec, start_dst, start_a, start_b, size);
        }
#endif

        while (start_dst < end_dst) {
            *start_dst = *select_vec ? *start_a : b;
            select_vec++;
            start_dst++;
            start_a++;
        }
    }

    // select if const const
    // dst[i] = select_vec[i] ? a : b
    static void select_if(SelectVec select_vec, Container& dst, CppType a, CppType b) {
        int size = dst.size();
        auto* start_dst = dst.data();
        auto* end_dst = dst.data() + size;

        [[maybe_unused]] const CppType* start_a = &a;
        [[maybe_unused]] const CppType* start_b = &b;

#ifdef __AVX2__
        if constexpr (sizeof(RunTimeCppType<TYPE>) == 1) {
            avx2_select_if<CppType, true, true>(select_vec, start_dst, start_a, start_b, size);
        } else if constexpr (could_use_common_select_if<CppType>()) {
            avx2_select_if_common_implement<CppType, true, true>(select_vec, start_dst, start_a, start_b, size);
        }
#endif
        while (start_dst < end_dst) {
            *start_dst = *select_vec ? a : b;
            select_vec++;
            start_dst++;
        }
    }
};

} // namespace starrocks
