#pragma once
#include <cstddef>
#include <cstdint>

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

struct CRC_HASH_SEEDS {
    // TODO: 0x811C9DC5 is not prime number
    static const uint32_t CRC_HASH_SEED1 = 0x811C9DC5;
    static const uint32_t CRC_HASH_SEED2 = 0x811C9DD7;
};

class SliceHash {
public:
    std::size_t operator()(const Slice& slice) const {
        return crc_hash_64(slice.data, static_cast<int32_t>(slice.size), CRC_HASH_SEEDS::CRC_HASH_SEED1);
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
        return crc_hash_64(slice.data, static_cast<int32_t>(slice.size), CRC_HASH_SEEDS::CRC_HASH_SEED1);
    }
};

template <>
class SliceHashWithSeed<PhmapSeed2> {
public:
    std::size_t operator()(const Slice& slice) const {
        return crc_hash_64(slice.data, static_cast<int32_t>(slice.size), CRC_HASH_SEEDS::CRC_HASH_SEED2);
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
} // namespace starrocks