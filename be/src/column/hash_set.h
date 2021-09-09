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

using SliceHashSet = phmap::flat_hash_set<SliceWithHash, HashOnSliceWithHash, EqualOnSliceWithHash>;

using SliceNormalHashSet = phmap::flat_hash_set<Slice, SliceHash, SliceNormalEqual>;

template <typename T>
struct PhSetTraits {
    using SetType = HashSet<T>;
};

template <>
struct PhSetTraits<Slice> {
    using SetType = SliceNormalHashSet;
};

template <typename T>
using PhSet = typename PhSetTraits<T>::SetType;

} // namespace starrocks::vectorized
