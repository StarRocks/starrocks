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

#include <cstdint>
#include <cstring>
#include <type_traits>
#include <vector>

#include "column/column.h"
#ifdef __SSE2__
#include <emmintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_acle.h>
#include <arm_neon.h>
#endif

namespace SIMD {

// Count the number of zeros of 8-bit integers.
template <typename T>
inline typename std::enable_if<std::is_same<T, int8_t>::value || std::is_same<T, uint8_t>::value, size_t>::type
count_zero(const T* data, size_t size) {
    size_t count = 0;
    const T* end = data + size;

#if defined(__SSE2__) && defined(__POPCNT__)
    const __m128i zero16 = _mm_setzero_si128();
    const T* end64 = data + (size / 64 * 64);

    for (; data < end64; data += 64) {
        count += __builtin_popcountll(static_cast<uint64_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                                              _mm_loadu_si128(reinterpret_cast<const __m128i*>(data)), zero16))) |
                                      (static_cast<uint64_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                                               _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + 16)), zero16)))
                                       << 16u) |
                                      (static_cast<uint64_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                                               _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + 32)), zero16)))
                                       << 32u) |
                                      (static_cast<uint64_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(
                                               _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + 48)), zero16)))
                                       << 48u));
    }
#elif defined(__ARM_NEON) && defined(__aarch64__) && defined(__POPCNT__)
    const T* end16 = data + (size / 16 * 16);
    for (; data < end16; data += 16) {
        uint8x16_t vdata = vld1q_u8(data);
        // result[i] = vdata[i] == 0 ? 0xFF : 0x00
        uint8x16_t result = vceqq_u8(vdata, vdupq_n_u8(0));
        // result[i] = result[i] & 0x1
        result = vandq_u8(result, vdupq_n_u8(1));
        count += vaddvq_u8(result);
    }
#endif

    for (; data < end; ++data) {
        count += (*data == 0);
    }
    return count;
}

// Count the number of zeros of 32-bit integers.
template <typename T>
inline typename std::enable_if<std::is_same<T, int32_t>::value || std::is_same<T, uint32_t>::value, size_t>::type
count_zero(const T* data, size_t size) {
    size_t count = 0;
    const T* end = data + size;

#if defined(__SSE2__) && defined(__POPCNT__)
    // count per 16 int
    const __m128i zero16 = _mm_setzero_si128();
    const T* end16 = data + (size / 16 * 16);

    for (; data < end16; data += 16) {
        count += __builtin_popcountll(static_cast<uint64_t>(_mm_movemask_ps(_mm_cvtepi32_ps(_mm_cmpeq_epi32(
                                              _mm_loadu_si128(reinterpret_cast<const __m128i*>(data)), zero16)))) |
                                      (static_cast<uint64_t>(_mm_movemask_ps(_mm_cvtepi32_ps(_mm_cmpeq_epi32(
                                               _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + 4)), zero16))))
                                       << 4u) |
                                      (static_cast<uint64_t>(_mm_movemask_ps(_mm_cvtepi32_ps(_mm_cmpeq_epi32(
                                               _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + 8)), zero16))))
                                       << 8u) |
                                      (static_cast<uint64_t>(_mm_movemask_ps(_mm_cvtepi32_ps(_mm_cmpeq_epi32(
                                               _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + 12)), zero16))))
                                       << 12u));
    }
#elif defined(__ARM_NEON) && defined(__aarch64__) && defined(__POPCNT__)
    const T* end4 = data + (size / 4 * 4);
    for (; data < end4; data += 4) {
        uint32x4_t vdata = vld1q_u32(data);
        // result[i] = vdata[i] == 0 ? 0xFF : 0x00
        uint32x4_t result = vceqq_u32(vdata, vdupq_n_u32(0));
        // result[i] = result[i] & 0x1
        result = vandq_u32(result, vdupq_n_u32(1));
        count += vaddvq_u32(result);
    }
#endif

    for (; data < end; ++data) {
        count += (*data == 0);
    }
    return count;
}

template <typename Container, typename T = typename Container::value_type>
inline size_t count_zero(const Container& nums) {
    static_assert(std::is_integral<T>::value && (sizeof(T) == sizeof(uint8_t) || sizeof(T) == sizeof(uint32_t)),
                  "only 8-bit or 32-bit integral types are supported");
    return count_zero(nums.data(), nums.size());
}

template <typename Container, typename T = typename Container::value_type>
inline size_t count_zero(const Container& nums, size_t size) {
    static_assert(std::is_integral<T>::value && (sizeof(T) == sizeof(uint8_t) || sizeof(T) == sizeof(uint32_t)),
                  "only 8-bit or 32-bit integral types are supported");
    return count_zero(nums.data(), size);
}

template <typename T>
inline size_t count_nonzero(const T* data, size_t size) {
    static_assert(std::is_integral<T>::value && (sizeof(T) == sizeof(uint8_t) || sizeof(T) == sizeof(uint32_t)),
                  "only 8-bit or 32-bit integral types are supported");
    return size - count_zero(data, size);
}

template <typename Container, typename T = typename Container::value_type>
inline size_t count_nonzero(const Container& nums) {
    static_assert(std::is_integral<T>::value && (sizeof(T) == sizeof(uint8_t) || sizeof(T) == sizeof(uint32_t)),
                  "only 8-bit or 32-bit integral types are supported");
    return count_nonzero(nums.data(), nums.size());
}

// NOTE: memchr is much faster than a plain SIMD implementation
template <typename Container, typename T = typename Container::value_type>
inline static size_t find_byte(const Container& list, size_t start, size_t count, T byte) {
    static_assert(std::is_integral<T>::value && (sizeof(T) == sizeof(uint8_t)),
                  "only 8-bit integral types are supported");

    if (start >= list.size()) {
        return start;
    }
    count = std::min(count, list.size() - start);
    const void* p = std::memchr((const void*)(list.data() + start), byte, count);
    if (p == nullptr) {
        return start + count;
    }
    return (T*)p - list.data();
}

template <typename Container, typename T = typename Container::value_type>
inline static size_t find_byte(const Container& list, size_t start, T byte) {
    static_assert(std::is_integral<T>::value && (sizeof(T) == sizeof(uint8_t)),
                  "only 8-bit integral types are supported");

    if (start >= list.size()) {
        return start;
    }
    const void* p = std::memchr((const void*)(list.data() + start), byte, list.size() - start);
    if (p == nullptr) {
        return list.size();
    }
    return (T*)p - list.data();
}

// Find position for zero byte, return size of list if not found
template <typename Container, typename T = typename Container::value_type>
inline size_t find_zero(const Container& list, size_t start) {
    static_assert(std::is_integral<T>::value && (sizeof(T) == sizeof(uint8_t)),
                  "only 8-bit integral types are supported");
    return find_byte(list, start, static_cast<T>(0));
}

template <typename Container, typename T = typename Container::value_type>
inline size_t find_zero(const Container& list, size_t start, size_t count) {
    static_assert(std::is_integral<T>::value && (sizeof(T) == sizeof(uint8_t)),
                  "only 8-bit integral types are supported");
    return find_byte(list, start, count, static_cast<T>(0));
}

template <typename Container, typename T = typename Container::value_type>
inline size_t find_nonzero(const Container& list, size_t start) {
    static_assert(std::is_integral<T>::value && (sizeof(T) == sizeof(uint8_t)),
                  "only 8-bit integral types are supported");
    return find_byte(list, start, static_cast<T>(1));
}

template <typename Container, typename T = typename Container::value_type>
inline size_t find_nonzero(const Container& list, size_t start, size_t count) {
    static_assert(std::is_integral<T>::value && (sizeof(T) == sizeof(uint8_t)),
                  "only 8-bit integral types are supported");
    return find_byte(list, start, count, static_cast<T>(1));
}

template <typename Container, typename T = typename Container::value_type>
inline bool contain_zero(const Container& list) {
    static_assert(std::is_integral<T>::value && (sizeof(T) == sizeof(uint8_t)),
                  "only 8-bit integral types are supported");
    return find_zero(list, 0) < list.size();
}

template <typename Container, typename T = typename Container::value_type>
inline bool contain_nonzero(const Container& list) {
    static_assert(std::is_integral<T>::value && (sizeof(T) == sizeof(uint8_t)),
                  "only 8-bit integral types are supported");
    return find_nonzero(list, 0) < list.size();
}

template <typename Container, typename T = typename Container::value_type>
inline bool contain_nonzero(const Container& list, size_t start) {
    static_assert(std::is_integral<T>::value && (sizeof(T) == sizeof(uint8_t)),
                  "only 8-bit integral types are supported");
    return find_nonzero(list, start) < list.size();
}

template <typename Container, typename T = typename Container::value_type>
inline bool contain_nonzero(const Container& list, size_t start, size_t count) {
    static_assert(std::is_integral<T>::value && (sizeof(T) == sizeof(uint8_t)),
                  "only 8-bit integral types are supported");
    size_t pos = find_nonzero(list, start, count);
    return pos < list.size() && pos < start + count;
}

#if defined(__ARM_NEON) && defined(__aarch64__)

/// Returns a 64-bit mask, each 4-bit represents a byte of the input.
/// The input containes 16 bytes and is expected to either 0x00 or 0xff for each byte.
/// The returned 4-bit is 0x if the corresponding byte of the input is 0x00, otherwise it is 0xf.
inline uint64_t get_nibble_mask(uint8x16_t values) {
    // vshrn_n_u16(values, 4) operates on each 16 bits. It right shifts 4 bits and then keeps the low 8 bits.
    // Therefore, 2 bytes of value can be compressed into 1 byte.
    // For example, 0x00'00 -> 0x00, 0xff'00 -> 0xf0, 0x00'ff -> 0x0f, 0xff'ff -> 0xff,
    return vget_lane_u64(vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(values), 4)), 0);
}

#endif

} // namespace SIMD
