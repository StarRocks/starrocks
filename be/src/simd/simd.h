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
#include <vector>
#ifdef __SSE2__
#include <emmintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_acle.h>
#include <arm_neon.h>
#endif

namespace SIMD {

// Count the number of zeros of 8-bit signed integers.
inline size_t count_zero(const int8_t* data, size_t size) {
    size_t count = 0;
    const int8_t* end = data + size;

#if defined(__SSE2__) && defined(__POPCNT__)
    const __m128i zero16 = _mm_setzero_si128();
    const int8_t* end64 = data + (size / 64 * 64);

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
#endif

    for (; data < end; ++data) {
        count += (*data == 0);
    }
    return count;
}

inline size_t count_zero(const uint32_t* data, size_t size) {
    size_t count = 0;
    const uint32_t* end = data + size;

#if defined(__SSE2__) && defined(__POPCNT__)
    // count per 16 int
    const __m128i zero16 = _mm_setzero_si128();
    const uint32_t* end16 = data + (size / 16 * 16);

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
#endif

    for (; data < end; ++data) {
        count += (*data == 0);
    }
    return count;
}

inline size_t count_zero(const std::vector<uint32_t>& nums) {
    return count_zero(nums.data(), nums.size());
}

inline size_t count_nonzero(const std::vector<uint32_t>& nums) {
    return nums.size() - count_zero(nums.data(), nums.size());
}

// Count the number of zeros of 8-bit unsigned integers.
inline size_t count_zero(const uint8_t* data, size_t size) {
    return count_zero(reinterpret_cast<const int8_t*>(data), size);
}

inline size_t count_zero(const std::vector<int8_t>& nums) {
    return count_zero(nums.data(), nums.size());
}

inline size_t count_zero(const std::vector<uint8_t>& nums, size_t size) {
    return count_zero(nums.data(), size);
}

inline size_t count_zero(const std::vector<uint8_t>& nums) {
    return count_zero(nums.data(), nums.size());
}

// Count the number of nonzeros of 8-bit signed integers.
inline size_t count_nonzero(const int8_t* data, size_t size) {
    return size - count_zero(data, size);
}

// Count the number of nonzeros of 8-bit unsigned integers.
inline size_t count_nonzero(const uint8_t* data, size_t size) {
    return size - count_zero(data, size);
}

inline size_t count_nonzero(const std::vector<uint8_t>& list) {
    return count_nonzero(list.data(), list.size());
}

inline size_t count_nonzero(const std::vector<int8_t>& list) {
    return count_nonzero(list.data(), list.size());
}

// NOTE: memchr is much faster than a plain SIMD implementation
template <class T>
inline static size_t find_byte(const std::vector<T>& list, size_t start, size_t count, T byte) {
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

template <class T>
inline static size_t find_byte(const std::vector<T>& list, size_t start, T byte) {
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
inline size_t find_zero(const std::vector<uint8_t>& list, size_t start) {
    return find_byte<uint8_t>(list, start, 0);
}

inline size_t find_nonzero(const std::vector<uint8_t>& list, size_t start) {
    return find_byte<uint8_t>(list, start, 1);
}

inline size_t find_nonzero(const std::vector<uint8_t>& list, size_t start, size_t count) {
    return find_byte<uint8_t>(list, start, count, 1);
}

inline size_t find_zero(const std::vector<int8_t>& list, size_t start) {
    return find_byte<int8_t>(list, start, 0);
}

inline size_t find_zero(const std::vector<uint8_t>& list, size_t start, size_t count) {
    return find_byte<uint8_t>(list, start, count, 0);
}

inline bool contain_zero(const std::vector<uint8_t>& list) {
    return find_zero(list, 0) < list.size();
}

inline bool contain_nonzero(const std::vector<uint8_t>& list) {
    return find_nonzero(list, 0) < list.size();
}

inline bool contain_nonzero(const std::vector<uint8_t>& list, size_t start) {
    return find_nonzero(list, start) < list.size();
}

inline bool contain_nonzero(const std::vector<uint8_t>& list, size_t start, size_t count) {
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
