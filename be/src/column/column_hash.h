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

#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

#ifdef __SSE2__
#include <emmintrin.h>
#include <xmmintrin.h>
#endif

#include "util/hash_util.hpp"
#include "util/slice.h"
#include "util/unaligned_access.h"

#if defined(__aarch64__)
#include "arm_acle.h"
#endif

namespace starrocks {

typedef unsigned __int128 uint128_t;
inline uint64_t umul128(uint64_t a, uint64_t b, uint64_t* high) {
    auto result = static_cast<uint128_t>(a) * static_cast<uint128_t>(b);
    *high = static_cast<uint64_t>(result >> 64u);
    return static_cast<uint64_t>(result);
}

template <int n>
struct phmap_mix {
    inline size_t operator()(size_t) const;
};

template <>
class phmap_mix<4> {
public:
    inline size_t operator()(size_t a) const {
        static constexpr uint64_t kmul = 0xcc9e2d51UL;
        uint64_t l = a * kmul;
        return static_cast<size_t>(l ^ (l >> 32u));
    }
};

template <>
class phmap_mix<8> {
public:
    // Very fast mixing (similar to Abseil)
    inline size_t operator()(size_t a) const {
        static constexpr uint64_t k = 0xde5fb9d2630458e9ULL;
        uint64_t h;
        uint64_t l = umul128(a, k, &h);
        return static_cast<size_t>(h + l);
    }
};

enum PhmapSeed { PhmapSeed1, PhmapSeed2 };

template <int n, PhmapSeed seed>
class phmap_mix_with_seed {
public:
    inline size_t operator()(size_t) const;
};

template <>
class phmap_mix_with_seed<4, PhmapSeed1> {
public:
    inline size_t operator()(size_t a) const {
        static constexpr uint64_t kmul = 0xcc9e2d51UL;
        uint64_t l = a * kmul;
        return static_cast<size_t>(l ^ (l >> 32u));
    }
};

template <>
class phmap_mix_with_seed<8, PhmapSeed1> {
public:
    inline size_t operator()(size_t a) const {
        static constexpr uint64_t k = 0xde5fb9d2630458e9ULL;
        uint64_t h;
        uint64_t l = umul128(a, k, &h);
        return static_cast<size_t>(h + l);
    }
};

template <>
class phmap_mix_with_seed<4, PhmapSeed2> {
public:
    inline size_t operator()(size_t a) const {
        static constexpr uint64_t kmul = 0xcc9e2d511d;
        uint64_t l = a * kmul;
        return static_cast<size_t>(l ^ (l >> 32u));
    }
};

template <>
class phmap_mix_with_seed<8, PhmapSeed2> {
public:
    inline size_t operator()(size_t a) const {
        static constexpr uint64_t k = 0xde5fb9d263046000ULL;
        uint64_t h;
        uint64_t l = umul128(a, k, &h);
        return static_cast<size_t>(h + l);
    }
};

inline uint32_t crc_hash_32(const void* data, int32_t bytes, uint32_t hash) {
#if defined(__x86_64__) && !defined(__SSE4_2__)
    return static_cast<uint32_t>(crc32(hash, (const unsigned char*)data, bytes));
#else
    uint32_t words = bytes / sizeof(uint32_t);
    bytes = bytes % 4 /*sizeof(uint32_t)*/;

    auto* p = reinterpret_cast<const uint8_t*>(data);

    while (words--) {
#if defined(__x86_64__)
        hash = _mm_crc32_u32(hash, unaligned_load<uint32_t>(p));
#elif defined(__aarch64__)
        hash = __crc32cw(hash, unaligned_load<uint32_t>(p));
#else
#error "Not supported architecture"
#endif
        p += sizeof(uint32_t);
    }

    while (bytes--) {
#if defined(__x86_64__)
        hash = _mm_crc32_u8(hash, *p);
#elif defined(__aarch64__)
        hash = __crc32cb(hash, *p);
#else
#error "Not supported architecture"
#endif
        ++p;
    }

    // The lower half of the CRC hash has has poor uniformity, so swap the halves
    // for anyone who only uses the first several bits of the hash.
    hash = (hash << 16u) | (hash >> 16u);
    return hash;
#endif
}

inline uint64_t crc_hash_64(const void* data, int32_t length, uint64_t hash) {
#if defined(__x86_64__) && !defined(__SSE4_2__)
    return crc32(hash, (const unsigned char*)data, length);
#else
    if (UNLIKELY(length < 8)) {
        return crc_hash_32(data, length, static_cast<uint32_t>(hash));
    }

    uint64_t words = length / sizeof(uint64_t);
    auto* p = reinterpret_cast<const uint8_t*>(data);
    auto* end = reinterpret_cast<const uint8_t*>(data) + length;
    while (words--) {
#if defined(__x86_64__) && defined(__SSE4_2__)
        hash = _mm_crc32_u64(hash, unaligned_load<uint64_t>(p));
#elif defined(__aarch64__)
        hash = __crc32cd(hash, unaligned_load<uint64_t>(p));
#else
#error "Not supported architecture"
#endif
        p += sizeof(uint64_t);
    }
    // Reduce the branch condition
    p = end - 8;
#if defined(__x86_64__)
    hash = _mm_crc32_u64(hash, unaligned_load<uint64_t>(p));
#elif defined(__aarch64__)
    hash = __crc32cd(hash, unaligned_load<uint64_t>(p));
#else
#error "Not supported architecture"
#endif
    p += sizeof(uint64_t);
    return hash;
#endif
}

// TODO: 0x811C9DC5 is not prime number
static const uint32_t CRC_HASH_SEED1 = 0x811C9DC5;
static const uint32_t CRC_HASH_SEED2 = 0x811C9DD7;

class SliceHash {
public:
    std::size_t operator()(const Slice& slice) const {
        return crc_hash_64(slice.data, static_cast<int32_t>(slice.size), CRC_HASH_SEED1);
    }
};

template <PhmapSeed>
class SliceHashWithSeed {
public:
    std::size_t operator()(const Slice& slice) const;
};

template <>
class SliceHashWithSeed<PhmapSeed1> {
public:
    std::size_t operator()(const Slice& slice) const {
        return crc_hash_64(slice.data, static_cast<int32_t>(slice.size), CRC_HASH_SEED1);
    }
};

template <>
class SliceHashWithSeed<PhmapSeed2> {
public:
    std::size_t operator()(const Slice& slice) const {
        return crc_hash_64(slice.data, static_cast<int32_t>(slice.size), CRC_HASH_SEED2);
    }
};

#if defined(__SSE2__) && !defined(ADDRESS_SANITIZER)

// NOTE: This function will access 15 excessive bytes after p1 and p2.
// NOTE: typename T must be uint8_t or int8_t
template <typename T>
typename std::enable_if<sizeof(T) == 1, bool>::type memequal(const T* p1, size_t size1, const T* p2, size_t size2) {
    if (size1 != size2) {
        return false;
    }
    for (size_t offset = 0; offset < size1; offset += 16) {
        uint16_t mask =
                _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(p1 + offset)),
                                                 _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2 + offset))));
        mask = ~mask;
        if (mask) {
            offset += __builtin_ctz(mask);
            return offset >= size1;
        }
    }
    return true;
}
#else

template <typename T>
typename std::enable_if<sizeof(T) == 1, bool>::type memequal(const T* p1, size_t size1, const T* p2, size_t size2) {
    return (size1 == size2) && (memcmp(p1, p2, size1) == 0);
}
#endif

static constexpr uint16_t SLICE_MEMEQUAL_OVERFLOW_PADDING = 15;
class SliceEqual {
public:
    bool operator()(const Slice& x, const Slice& y) const { return memequal(x.data, x.size, y.data, y.size); }
};

class SliceNormalEqual {
public:
    bool operator()(const Slice& x, const Slice& y) const {
        return (x.size == y.size) && (memcmp(x.data, y.data, x.size) == 0);
    }
};

template <class T>
class StdHash {
public:
    std::size_t operator()(T value) const { return phmap_mix<sizeof(size_t)>()(std::hash<T>()(value)); }
};

template <class T, PhmapSeed seed>
class StdHashWithSeed {
public:
    std::size_t operator()(T value) const { return phmap_mix_with_seed<sizeof(size_t), seed>()(std::hash<T>()(value)); }
};

inline uint64_t crc_hash_uint64(uint64_t value, uint64_t seed) {
#if defined(__x86_64__) && defined(__SSE4_2__)
    return _mm_crc32_u64(seed, value);
#elif defined(__x86_64__)
    return crc32(seed, (const unsigned char*)&value, sizeof(uint64_t));
#elif defined(__aarch64__)
    return __crc32cd(seed, value);
#else
#error "Not supported architecture"
#endif
}

inline uint64_t crc_hash_uint128(uint64_t value0, uint64_t value1, uint64_t seed) {
#if defined(__x86_64__) && defined(__SSE4_2__)
    uint64_t hash = _mm_crc32_u64(seed, value0);
    hash = _mm_crc32_u64(hash, value1);
#elif defined(__x86_64__)
    uint64_t hash = crc32(seed, (const unsigned char*)&value0, sizeof(uint64_t));
    hash = crc32(hash, (const unsigned char*)&value1, sizeof(uint64_t));
#elif defined(__aarch64__)
    uint64_t hash = __crc32cd(seed, value0);
    hash = __crc32cd(hash, value1);
#else
#error "Not supported architecture"
#endif
    return hash;
}

// https://github.com/HowardHinnant/hash_append/issues/7
template <typename T>
inline void hash_combine(uint64_t& seed, const T& val) {
    seed ^= std::hash<T>{}(val) + 0x9e3779b97f4a7c15LLU + (seed << 12) + (seed >> 4);
}

inline uint64_t hash_128(uint64_t seed, int128_t val) {
    auto low = static_cast<size_t>(val);
    auto high = static_cast<size_t>(val >> 64);
    hash_combine(seed, low);
    hash_combine(seed, high);
    return seed;
}

template <PhmapSeed seed>
struct Hash128WithSeed {
    std::size_t operator()(int128_t value) const {
        return phmap_mix_with_seed<sizeof(size_t), seed>()(hash_128(seed, value));
    }
};

} // namespace starrocks
