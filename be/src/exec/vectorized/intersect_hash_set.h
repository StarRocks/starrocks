// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/chunk.h"
#include "column/column_hash.h"
#include "exprs/expr_context.h"
#include "runtime/mem_pool.h"
#include "util/phmap/phmap.h"
#include "util/slice.h"

namespace starrocks::vectorized {

class IntersectSliceFlag;
struct IntersectSliceFlagEqual;

class IntersectSliceFlag {
public:
    IntersectSliceFlag(const uint8_t* d, size_t n) : slice(d, n), hit_times(0) {}

    Slice slice;
    mutable uint16_t hit_times;
};

struct IntersectSliceFlagEqual {
    bool operator()(const IntersectSliceFlag& x, const IntersectSliceFlag& y) const {
        return memequal(x.slice.data, x.slice.size, y.slice.data, y.slice.size);
    }
};

struct IntersectSliceFlagHash {
    static const uint32_t CRC_SEED = 0x811C9DC5;
    std::size_t operator()(const IntersectSliceFlag& sliceMayUnneed) const {
        const Slice& slice = sliceMayUnneed.slice;
        return crc_hash_64(slice.data, slice.size, CRC_SEED);
    }
};

template <typename HashSet>
class IntersectHashSet {
public:
    using Iterator = typename HashSet::iterator;
    using KeyVector = typename std::vector<Slice>;

    IntersectHashSet() = default;

    Status init() {
        _hash_set = std::make_unique<HashSet>();
        _mem_pool = std::make_unique<MemPool>();
        _buffer = _mem_pool->allocate(_max_one_row_size * config::vector_chunk_size);
        RETURN_IF_UNLIKELY_NULL(_buffer, Status::MemoryAllocFailed("alloc mem for intersect hash set failed"));
        return Status::OK();
    }

    Iterator begin() { return _hash_set->begin(); }

    Iterator end() { return _hash_set->end(); }

    bool empty() { return _hash_set->empty(); }

    void build_set(RuntimeState* state, const ChunkPtr& chunkPtr, const std::vector<ExprContext*>& exprs,
                   MemPool* pool);

    Status refine_intersect_row(RuntimeState* state, const ChunkPtr& chunkPtr, const std::vector<ExprContext*>& exprs,
                                int hit_times);

    void deserialize_to_columns(KeyVector& keys, const Columns& key_columns, size_t batch_size);

    int64_t mem_usage() const;

private:
    void _serialize_columns(const ChunkPtr& chunkPtr, const std::vector<ExprContext*>& exprs, size_t chunk_size);

    size_t _get_max_serialize_size(const ChunkPtr& chunkPtr, const std::vector<ExprContext*>& exprs);

    std::unique_ptr<HashSet> _hash_set;

    Buffer<uint32_t> _slice_sizes;
    size_t _max_one_row_size = 8;
    std::unique_ptr<MemPool> _mem_pool;
    uint8_t* _buffer;
};

template class IntersectHashSet<
        phmap::flat_hash_set<IntersectSliceFlag, IntersectSliceFlagHash, IntersectSliceFlagEqual>>;
using IntersectHashSerializeSet =
        IntersectHashSet<phmap::flat_hash_set<IntersectSliceFlag, IntersectSliceFlagHash, IntersectSliceFlagEqual>>;
} // namespace starrocks::vectorized
