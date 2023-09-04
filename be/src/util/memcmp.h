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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/memcmp.h

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
#include <type_traits>

// Must include headers out of namespace
#if defined(__SSE4_1__) && !defined(ADDRESS_SANITIZER)
#include <smmintrin.h>
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

// memequal is used to optimize the comparison between the two strings.
//  1. If the length is equal and larger than 16, use SSE4.1
//  2. If the length is small than 16, convert the address to int16/int32/int64
//     to comparison
// so it does not need to consider extra padding bytes for SIMD, which is required by memequal_padded().
// TODO: If know the size in advance, call the function by constant parameter
//       like memequal(p1, 10, p2, 10) is efficient

#if defined(__SSE4_1__) && !defined(ADDRESS_SANITIZER)

inline bool memequal(const char* p1, size_t size1, const char* p2, size_t size2) {
    if (size1 != size2) {
        return false;
    }

    if (size1 == 0) {
        return true;
    }

    // const char * p1_end = p1 + size1;
    const char* p1_end_16 = p1 + size1 / 16 * 16;

    __m128i zero16 = _mm_setzero_si128();

    while (p1 < p1_end_16) {
        if (!_mm_testc_si128(zero16, _mm_xor_si128(_mm_loadu_si128(reinterpret_cast<const __m128i*>(p1)),
                                                   _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2)))))
            return false;

        p1 += 16;
        p2 += 16;
    }

    switch (size1 % 16) {
    case 15:
        if (p1[14] != p2[14]) return false;
        [[fallthrough]];
    case 14:
        if (p1[13] != p2[13]) return false;
        [[fallthrough]];
    case 13:
        if (p1[12] != p2[12]) return false;
        [[fallthrough]];
    case 12:
        if (reinterpret_cast<const uint32_t*>(p1)[2] == reinterpret_cast<const uint32_t*>(p2)[2])
            goto l8;
        else
            return false;
    case 11:
        if (p1[10] != p2[10]) return false;
        [[fallthrough]];
    case 10:
        if (p1[9] != p2[9]) return false;
        [[fallthrough]];
    case 9:
        if (p1[8] != p2[8]) return false;
    l8:
        [[fallthrough]];
    case 8:
        return reinterpret_cast<const uint64_t*>(p1)[0] == reinterpret_cast<const uint64_t*>(p2)[0];
    case 7:
        if (p1[6] != p2[6]) return false;
        [[fallthrough]];
    case 6:
        if (p1[5] != p2[5]) return false;
        [[fallthrough]];
    case 5:
        if (p1[4] != p2[4]) return false;
        [[fallthrough]];
    case 4:
        return reinterpret_cast<const uint32_t*>(p1)[0] == reinterpret_cast<const uint32_t*>(p2)[0];
    case 3:
        if (p1[2] != p2[2]) return false;
        [[fallthrough]];
    case 2:
        return reinterpret_cast<const uint16_t*>(p1)[0] == reinterpret_cast<const uint16_t*>(p2)[0];
    case 1:
        if (p1[0] != p2[0]) return false;
        [[fallthrough]];
    case 0:
        break;
    }

    return true;
}

#else

inline bool memequal(const char* p1, size_t size1, const char* p2, size_t size2) {
    return (size1 == size2) && (memcmp(p1, p2, size1) == 0);
}

#endif

inline int memcompare(const char* p1, size_t size1, const char* p2, size_t size2) {
    size_t min_size = std::min(size1, size2);
    auto res = memcmp(p1, p2, min_size);
    if (res != 0) {
        return res;
    }
    return compare(size1, size2);
}

} // namespace starrocks
