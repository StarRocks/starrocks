// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#ifdef __AVX2__
#include <emmintrin.h>
#include <immintrin.h>
#endif

#include <cstdint>
#include <type_traits>

#include "column/type_traits.h"
#include "gutil/port.h"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {

#ifdef __AVX2__
template <typename T, std::enable_if_t<sizeof(T) == 1, int> = 1>
inline void avx2_select_if(uint8_t*& selector, T*& dst, const T*& a, const T*& b, int size) {
    const T* dst_end = dst + size;
    while (dst + 32 < dst_end) {
        __m256i loaded_mask = _mm256_loadu_si256(reinterpret_cast<__m256i*>(selector));
        loaded_mask = _mm256_cmpgt_epi8(loaded_mask, _mm256_setzero_si256());
        __m256i vec_a = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a));
        __m256i vec_b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b));
        __m256i res = _mm256_blendv_epi8(vec_b, vec_a, loaded_mask);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst), res);
        dst += 32;
        selector += 32;
        a += 32;
        b += 32;
    }
}

template <typename T, std::enable_if_t<sizeof(T) == 1, int> = 1>
inline void avx2_select_if_const_var(uint8_t*& selector, T*& dst, const T a, T*& b, int size) {
    __m256i vec_a = _mm256_set1_epi8(a);
    const T* dst_end = dst + size;
    while (dst + 32 < dst_end) {
        __m256i loaded_mask = _mm256_loadu_si256(reinterpret_cast<__m256i*>(selector));
        loaded_mask = _mm256_cmpgt_epi8(loaded_mask, _mm256_setzero_si256());
        __m256i vec_b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b));
        __m256i res = _mm256_blendv_epi8(vec_b, vec_a, loaded_mask);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst), res);
        dst += 32;
        selector += 32;
        b += 32;
    }
}

template <typename T, std::enable_if_t<sizeof(T) == 1, int> = 1>
inline void avx2_select_if_var_const(uint8_t*& selector, T*& dst, T*& a, const T b, int size) {
    __m256i vec_b = _mm256_set1_epi8(b);
    const T* dst_end = dst + size;
    while (dst + 32 < dst_end) {
        __m256i loaded_mask = _mm256_loadu_si256(reinterpret_cast<__m256i*>(selector));
        loaded_mask = _mm256_cmpgt_epi8(loaded_mask, _mm256_setzero_si256());
        __m256i vec_a = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a));
        __m256i res = _mm256_blendv_epi8(vec_b, vec_a, loaded_mask);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst), res);
        dst += 32;
        selector += 32;
        a += 32;
    }
}

template <typename T, std::enable_if_t<sizeof(T) == 1, int> = 1>
inline void avx2_select_if_const_const(uint8_t*& selector, T*& dst, const T a, T b, int size) {
    __m256i vec_a = _mm256_set1_epi8(a);
    __m256i vec_b = _mm256_set1_epi8(b);
    const T* dst_end = dst + size;
    while (dst + 32 < dst_end) {
        __m256i loaded_mask = _mm256_loadu_si256(reinterpret_cast<__m256i*>(selector));
        loaded_mask = _mm256_cmpgt_epi8(loaded_mask, _mm256_setzero_si256());
        __m256i res = _mm256_blendv_epi8(vec_b, vec_a, loaded_mask);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst), res);
        dst += 32;
        selector += 32;
        b += 32;
    }
}

// _mm256_blend_epi not work as expect
// TODO (stdpain): implement select_if for int16 int64 int128
template <typename T, std::enable_if_t<sizeof(T) == 2, int> = 2>
inline void avx2_select_if(uint8_t*& selector, T*& dst, const T*& a, const T*& b, int size) {}

// _mm256_blend_epi32
template <typename T, std::enable_if_t<sizeof(T) == 4, int> = 4>
inline void avx2_select_if(uint8_t*& selector, T*& dst, const T*& a, const T*& b, int size) {
    const T* dst_end = dst + size;

    while (dst + 8 < dst_end) {
        uint64_t value = UNALIGNED_LOAD64(selector);
        __m128i v = _mm_set1_epi64x(value);
        __m256i loaded_mask = _mm256_cvtepi8_epi32(v);
        __m256i cond = _mm256_cmpgt_epi8(loaded_mask, _mm256_setzero_si256());

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
#endif

// SIMD selector
// only support PrimaryType Arithmetic and DATE
template <PrimitiveType TYPE>
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
        if constexpr (sizeof(RunTimeCppType<TYPE>) == 1) {
            avx2_select_if(select_vec, start_dst, start_a, start_b, size);
        } else if constexpr (sizeof(RunTimeCppType<TYPE>) == 4) {
            avx2_select_if(select_vec, start_dst, start_a, start_b, size);
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
    static void select_if(SelectVec select_vec, Container& dst, const CppType a, Container& b) {
        int size = dst.size();
        auto* start_dst = dst.data();
        auto* end_dst = dst.data() + size;

        auto* start_b = b.data();

#ifdef __AVX2__
        if constexpr (sizeof(RunTimeCppType<TYPE>) == 1) {
            avx2_select_if_const_var(select_vec, start_dst, a, start_b, size);
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
    static void select_if(SelectVec select_vec, Container& dst, Container& a, const CppType b) {
        int size = dst.size();
        auto* start_dst = dst.data();
        auto* end_dst = dst.data() + size;

        auto* start_a = a.data();

#ifdef __AVX2__
        if constexpr (sizeof(RunTimeCppType<TYPE>) == 1) {
            avx2_select_if_var_const(select_vec, start_dst, start_a, b, size);
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
    static void select_if(SelectVec select_vec, Container& dst, const CppType a, const CppType b) {
        int size = dst.size();
        auto* start_dst = dst.data();
        auto* end_dst = dst.data() + size;

#ifdef __AVX2__
        if constexpr (sizeof(RunTimeCppType<TYPE>) == 1) {
            avx2_select_if_const_const(select_vec, start_dst, a, b, size);
        }
#endif
        while (start_dst < end_dst) {
            *start_dst = *select_vec ? a : b;
            select_vec++;
            start_dst++;
        }
    }
};

} // namespace starrocks::vectorized