// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/simd/simd.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <cstdint>
#include <cstring>
#include <vector>
#ifdef __SSE2__
#include <emmintrin.h>
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

inline size_t find_zero(const std::vector<int8_t>& list, size_t start) {
    return find_byte<int8_t>(list, start, 0);
}
} // namespace SIMD
