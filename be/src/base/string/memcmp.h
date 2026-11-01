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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/base/string/memcmp.h

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

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <type_traits>

#include "base/hash/unaligned_access.h"
#include "gutil/strings/fastmem.h"

// Must include headers out of namespace
#if defined(__SSE4_1__)
#include <smmintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

namespace starrocks {

template <typename T>
inline int compare(T lhs, T rhs) {
    if (lhs < rhs) {
        return -1;
    } else if (lhs > rhs) {
        return 1;
    } else {
        return 0;
    }
}

// memequal is used to optimize the comparison between two strings.
//  1. If size >= 16, compare 16-byte chunks via SSE4.1 or ARM64 NEON and an
//     overlapping 16-byte tail load at (end1 - 16, end2 - 16).
//  2. If size < 16, compare via overlapping 8/4/2/1-byte scalar loads.
// Every load is strictly within [p1, p1 + size1) and [p2, p2 + size1), so it
// is safe under AddressSanitizer and UndefinedBehaviorSanitizer without
// requiring 16-byte buffer padding.
inline bool memequal(const char* p1, size_t size1, const char* p2, size_t size2) {
    if (size1 != size2) {
        return false;
    }

    if (size1 == 0) {
        return true;
    }

    if (size1 >= 16) {
        const char* end1 = p1 + size1;
        const char* end2 = p2 + size1;
#if defined(__SSE4_1__)
        __m128i zero16 = _mm_setzero_si128();
        while (p1 + 16 < end1) {
            if (!_mm_testc_si128(zero16, _mm_xor_si128(_mm_loadu_si128(reinterpret_cast<const __m128i*>(p1)),
                                                       _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2))))) {
                return false;
            }
            p1 += 16;
            p2 += 16;
        }
        return _mm_testc_si128(zero16, _mm_xor_si128(_mm_loadu_si128(reinterpret_cast<const __m128i*>(end1 - 16)),
                                                     _mm_loadu_si128(reinterpret_cast<const __m128i*>(end2 - 16))));
#elif defined(__ARM_NEON) && defined(__aarch64__)
        while (p1 + 16 < end1) {
            uint8x16_t v1 = vld1q_u8(reinterpret_cast<const uint8_t*>(p1));
            uint8x16_t v2 = vld1q_u8(reinterpret_cast<const uint8_t*>(p2));
            uint64x2_t diff = vreinterpretq_u64_u8(veorq_u8(v1, v2));
            if ((vgetq_lane_u64(diff, 0) | vgetq_lane_u64(diff, 1)) != 0) {
                return false;
            }
            p1 += 16;
            p2 += 16;
        }
        uint8x16_t t1 = vld1q_u8(reinterpret_cast<const uint8_t*>(end1 - 16));
        uint8x16_t t2 = vld1q_u8(reinterpret_cast<const uint8_t*>(end2 - 16));
        uint64x2_t tdiff = vreinterpretq_u64_u8(veorq_u8(t1, t2));
        return (vgetq_lane_u64(tdiff, 0) | vgetq_lane_u64(tdiff, 1)) == 0;
#else
        return memcmp(p1, p2, size1) == 0;
#endif
    }

    if (size1 >= 8) {
        return ((unaligned_load<uint64_t>(p1) ^ unaligned_load<uint64_t>(p2)) |
                (unaligned_load<uint64_t>(p1 + size1 - 8) ^ unaligned_load<uint64_t>(p2 + size1 - 8))) == 0;
    }
    if (size1 >= 4) {
        return ((unaligned_load<uint32_t>(p1) ^ unaligned_load<uint32_t>(p2)) |
                (unaligned_load<uint32_t>(p1 + size1 - 4) ^ unaligned_load<uint32_t>(p2 + size1 - 4))) == 0;
    }
    if (size1 >= 2) {
        return ((unaligned_load<uint16_t>(p1) ^ unaligned_load<uint16_t>(p2)) |
                (unaligned_load<uint16_t>(p1 + size1 - 2) ^ unaligned_load<uint16_t>(p2 + size1 - 2))) == 0;
    }
    return p1[0] == p2[0];
}

// memcompare_short orders keys by loading 2/4/8 bytes and byte-swapping them, so that the integer comparison
// matches lexicographic byte order. That is only correct when native loads are little-endian.
static_assert(std::endian::native == std::endian::little, "memcompare_short requires a little-endian target");

inline int memcompare_short(const char* p1, size_t size1, const char* p2, size_t size2) {
    size_t min_size = std::min(size1, size2);
    if (min_size == 0) {
        return compare(size1, size2);
    }
    if (min_size <= 16) {
        if (min_size >= 8) {
            uint64_t a = __builtin_bswap64(unaligned_load<uint64_t>(p1));
            uint64_t b = __builtin_bswap64(unaligned_load<uint64_t>(p2));
            if (a != b) return a < b ? -1 : 1;
            uint64_t ta = __builtin_bswap64(unaligned_load<uint64_t>(p1 + min_size - 8));
            uint64_t tb = __builtin_bswap64(unaligned_load<uint64_t>(p2 + min_size - 8));
            if (ta != tb) return ta < tb ? -1 : 1;
            return compare(size1, size2);
        }
        if (min_size >= 4) {
            uint32_t a = __builtin_bswap32(unaligned_load<uint32_t>(p1));
            uint32_t b = __builtin_bswap32(unaligned_load<uint32_t>(p2));
            if (a != b) return a < b ? -1 : 1;
            uint32_t ta = __builtin_bswap32(unaligned_load<uint32_t>(p1 + min_size - 4));
            uint32_t tb = __builtin_bswap32(unaligned_load<uint32_t>(p2 + min_size - 4));
            if (ta != tb) return ta < tb ? -1 : 1;
            return compare(size1, size2);
        }
        if (min_size >= 2) {
            uint16_t a = __builtin_bswap16(unaligned_load<uint16_t>(p1));
            uint16_t b = __builtin_bswap16(unaligned_load<uint16_t>(p2));
            if (a != b) return a < b ? -1 : 1;
            uint16_t ta = __builtin_bswap16(unaligned_load<uint16_t>(p1 + min_size - 2));
            uint16_t tb = __builtin_bswap16(unaligned_load<uint16_t>(p2 + min_size - 2));
            if (ta != tb) return ta < tb ? -1 : 1;
            return compare(size1, size2);
        }
        uint8_t a = static_cast<uint8_t>(p1[0]);
        uint8_t b = static_cast<uint8_t>(p2[0]);
        if (a != b) return a < b ? -1 : 1;
        return compare(size1, size2);
    }

    auto res = memcmp(p1, p2, min_size);
    if (res != 0) {
        return res > 0 ? 1 : -1;
    }
    return compare(size1, size2);
}

inline int memcompare(const char* p1, size_t size1, const char* p2, size_t size2) {
    return memcompare_short(p1, size1, p2, size2);
}

#if defined(__SSE4_2__)
// Legacy 16-byte SIMD compare helper retained for direct unit tests; requires
// both p1 and p2 to have at least 16 readable bytes regardless of `size`.
inline int sse_memcmp2(const char* p1, const char* p2, size_t size) {
    __m128i left = _mm_lddqu_si128(reinterpret_cast<const __m128i*>(p1));
    __m128i right = _mm_lddqu_si128(reinterpret_cast<const __m128i*>(p2));
    __m128i nz = ~_mm_cmpeq_epi8(left, right);
    unsigned short mask = _mm_movemask_epi8(nz);
    if (mask == 0) return 0;
    int index = __builtin_ctz(mask);
    if (static_cast<size_t>(index) >= size) return 0;
    return static_cast<int>(static_cast<uint8_t>(p1[index])) - static_cast<int>(static_cast<uint8_t>(p2[index]));
}
#endif

constexpr size_t PADDED_SIZE = 16;
// All loads in memcompare_short are strictly within [0, min_size), so 16-byte
// overflow padding is no longer required; PADDED_SIZE is retained for callers
// (e.g. maxmin.h / retract_maxmin.h) that size state buffers with it.
inline int memcompare_padded(const char* p1, size_t size1, const char* p2, size_t size2) {
    return memcompare_short(p1, size1, p2, size2);
}

} // namespace starrocks
