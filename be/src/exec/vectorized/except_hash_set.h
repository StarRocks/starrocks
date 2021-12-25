// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "column/chunk.h"
#include "column/column_hash.h"
#include "exprs/expr_context.h"
#include "runtime/mem_pool.h"
#include "util/phmap/phmap.h"
#include "util/phmap/phmap_dump.h"
#include "util/slice.h"

namespace starrocks::vectorized {

class ExceptSliceFlag;
struct ExceptSliceFlagEqual;
struct ExceptSliceFlagHash;

class ExceptSliceFlag {
public:
    ExceptSliceFlag(const uint8_t* d, size_t n) : slice(d, n), deleted(false) {}

    Slice slice;
    mutable bool deleted;
};

struct ExceptSliceFlagEqual {
    bool operator()(const ExceptSliceFlag& x, const ExceptSliceFlag& y) const {
        return memequal(x.slice.data, x.slice.size, y.slice.data, y.slice.size);
    }
};

struct ExceptSliceFlagHash {
    static const uint32_t CRC_SEED = 0x811C9DC5;
    std::size_t operator()(const ExceptSliceFlag& sliceMayUnneed) const {
        const Slice& slice = sliceMayUnneed.slice;
        return crc_hash_64(slice.data, slice.size, CRC_SEED);
    }
};

template <typename HashSet>
class ExceptHashSet {
public:
    using Iterator = typename HashSet::iterator;
    using KeyVector = std::vector<Slice>;

    ExceptHashSet() = default;

    Status init() {
        _hash_set = std::make_unique<HashSet>();
        _mem_pool = std::make_unique<MemPool>();
        _buffer = _mem_pool->allocate(_max_one_row_size * config::vector_chunk_size);
        RETURN_IF_UNLIKELY_NULL(_buffer, Status::MemoryAllocFailed("alloc mem of except hash set failed"));
        return Status::OK();
    }

    Iterator begin() { return _hash_set->begin(); }

    Iterator end() { return _hash_set->end(); }

    bool empty() { return _hash_set->empty(); }

    size_t size() { return _hash_set->size(); }

    void build_set(RuntimeState* state, const ChunkPtr& chunk, const std::vector<ExprContext*>& exprs, MemPool* pool);

    Status erase_duplicate_row(RuntimeState* state, const ChunkPtr& chunk, const std::vector<ExprContext*>& exprs);

    void deserialize_to_columns(KeyVector& keys, const Columns& key_columns, size_t batch_size);

    int64_t mem_usage() { return _hash_set->dump_bound() + _mem_pool->total_reserved_bytes(); }

private:
    size_t _get_max_serialize_size(const ChunkPtr& chunk, const std::vector<ExprContext*>& exprs);

    void _serialize_columns(const ChunkPtr& chunk, const std::vector<ExprContext*>& exprs, size_t chunk_size);

    size_t _max_one_row_size = 8;
    Buffer<uint32_t> _slice_sizes;

    std::unique_ptr<HashSet> _hash_set;

    // Used to allocate memory for serializing columns to the key.
    std::unique_ptr<MemPool> _mem_pool;
    uint8_t* _buffer;
};

using ExceptHashSerializeSet =
        ExceptHashSet<phmap::flat_hash_set<ExceptSliceFlag, ExceptSliceFlagHash, ExceptSliceFlagEqual>>;

} // namespace starrocks::vectorized
