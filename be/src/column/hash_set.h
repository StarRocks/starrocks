// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/column_hash.h"
#include "util/phmap/phmap.h"
#include "util/phmap/phmap_dump.h"

namespace starrocks::vectorized {

template <typename T>
using HashSet = phmap::flat_hash_set<T, StdHash<T>>;

// By storing hash value in slice, we can save the cost of
// 1. re-calculate hash value of the slice
// 2. touch slice memory area which may cause high latency of memory access.
// and the tradeoff is we allocate 8-bytes hash value in slice.
// But now we allocate all slice data on a single memory pool(4K per allocation)
// the internal fragmentation can offset these 8-bytes hash value.

class SliceWithHash : public Slice {
public:
    size_t hash;
    SliceWithHash(const Slice& src) : Slice(src.data, src.size) { hash = SliceHash()(src); }
    SliceWithHash(const uint8_t* p, size_t s, size_t h) : Slice(p, s), hash(h) {}
};

class HashOnSliceWithHash {
public:
    std::size_t operator()(const SliceWithHash& slice) const { return slice.hash; }
};

class EqualOnSliceWithHash {
public:
    bool operator()(const SliceWithHash& x, const SliceWithHash& y) const {
        // by comparing hash value first, we can avoid comparing real data
        // which may touch another memory area and has bad cache locality.
        return x.hash == y.hash && memequal(x.data, x.size, y.data, y.size);
    }
};

template <PhmapSeed seed>
class TSliceWithHash : public Slice {
public:
    size_t hash;
    TSliceWithHash(const Slice& src) : Slice(src.data, src.size) { hash = SliceHashWithSeed<seed>()(src); }
    TSliceWithHash(const uint8_t* p, size_t s, size_t h) : Slice(p, s), hash(h) {}
};

template <PhmapSeed seed>
class THashOnSliceWithHash {
public:
    std::size_t operator()(const TSliceWithHash<seed>& slice) const { return slice.hash; }
};

template <PhmapSeed seed>
class TEqualOnSliceWithHash {
public:
    bool operator()(const TSliceWithHash<seed>& x, const TSliceWithHash<seed>& y) const {
        // by comparing hash value first, we can avoid comparing real data
        // which may touch another memory area and has bad cache locality.
        return x.hash == y.hash && memequal(x.data, x.size, y.data, y.size);
    }
};

using SliceHashSet = phmap::flat_hash_set<SliceWithHash, HashOnSliceWithHash, EqualOnSliceWithHash>;

using SliceNormalHashSet = phmap::flat_hash_set<Slice, SliceHash, SliceNormalEqual>;

struct SliceKey8 {
    union U {
        struct {
            char data[7];
            uint8_t size;
        } __attribute__((packed));
        int64_t value;
    } u;
    static_assert(sizeof(u) == sizeof(u.value));
    bool operator==(const SliceKey8& k) const { return u.value == k.u.value; }
    SliceKey8() {}
    SliceKey8(const SliceKey8& x) { u.value = x.u.value; }
    SliceKey8& operator=(const SliceKey8& x) {
        u.value = x.u.value;
        return *this;
    }
    SliceKey8(SliceKey8&& x) { u.value = x.u.value; }
};

struct SliceKey16 {
    union U {
        struct {
            char data[15];
            uint8_t size;
        } __attribute__((packed));
        struct {
            uint64_t ui64[2];
        } __attribute__((packed));
        int128_t value;
    } u;
    static_assert(sizeof(u) == sizeof(u.value));
    bool operator==(const SliceKey16& k) const { return u.value == k.u.value; }
    SliceKey16() {}
    SliceKey16(const SliceKey16& x) { u.value = x.u.value; }
    SliceKey16& operator=(const SliceKey16& x) {
        u.value = x.u.value;
        return *this;
    }
    SliceKey16(SliceKey16&& x) { u.value = x.u.value; }
};

template <typename SliceKey, PhmapSeed seed>
class FixedSizeSliceKeyHash {
public:
    std::size_t operator()(const SliceKey& s) const {
        if constexpr (sizeof(SliceKey) == 8) {
            if constexpr (seed == PhmapSeed1) {
                return crc_hash_uint64(s.u.value, CRC_HASH_SEED1);
            } else {
                return crc_hash_uint64(s.u.value, CRC_HASH_SEED2);
            }
        } else {
            static_assert(sizeof(SliceKey) == 16);
            if constexpr (seed == PhmapSeed1) {
                return crc_hash_uint128(s.u.ui64[0], s.u.ui64[1], CRC_HASH_SEED1);
            } else {
                return crc_hash_uint128(s.u.ui64[0], s.u.ui64[1], CRC_HASH_SEED2);
            }
        }
    }
};

} // namespace starrocks::vectorized
