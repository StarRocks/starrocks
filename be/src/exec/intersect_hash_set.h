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

#include "column/chunk.h"
#include "column/column_hash.h"
#include "exprs/expr_context.h"
#include "runtime/mem_pool.h"
#include "util/phmap/phmap.h"
#include "util/slice.h"

namespace starrocks {

class IntersectSliceFlag;
struct IntersectSliceFlagEqual;

class IntersectSliceFlag {
public:
    IntersectSliceFlag(const uint8_t* d, size_t n) : slice(d, n) {}

    Slice slice;
    mutable uint16_t hit_times{0};
};

struct IntersectSliceFlagEqual {
    bool operator()(const IntersectSliceFlag& x, const IntersectSliceFlag& y) const {
        return memequal_padded(x.slice.data, x.slice.size, y.slice.data, y.slice.size);
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
    using KeyVector = Buffer<Slice>;

    IntersectHashSet() = default;

    Status init(RuntimeState* state);

    Iterator begin() { return _hash_set->begin(); }

    Iterator end() { return _hash_set->end(); }

    bool empty() { return _hash_set->empty(); }

    void build_set(RuntimeState* state, const ChunkPtr& chunkPtr, const std::vector<ExprContext*>& exprs,
                   MemPool* pool);

    Status refine_intersect_row(RuntimeState* state, const ChunkPtr& chunkPtr, const std::vector<ExprContext*>& exprs,
                                int hit_times);

    void deserialize_to_columns(KeyVector& keys, Columns& key_columns, size_t chunk_size);

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

using IntersectHashSerializeSet =
        IntersectHashSet<phmap::flat_hash_set<IntersectSliceFlag, IntersectSliceFlagHash, IntersectSliceFlagEqual>>;

} // namespace starrocks
