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

#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exec/join_hash_map.h"
#include "runtime/runtime_state.h"

namespace starrocks {
class HashJoiner;
class HashJoinProber {
public:
    HashJoinProber(HashJoiner& hash_joiner) : _hash_joiner(hash_joiner) {}

    bool probe_chunk_empty() const { return _probe_chunk == nullptr; }

    [[nodiscard]] Status push_probe_chunk(RuntimeState* state, ChunkPtr&& chunk);

    // probe hash table
    [[nodiscard]] StatusOr<ChunkPtr> probe_chunk(RuntimeState* state, JoinHashTable* hash_table);

    [[nodiscard]] StatusOr<ChunkPtr> probe_remain(RuntimeState* state, JoinHashTable* hash_table, bool* has_remain);

    void reset();

    HashJoinProber* clone_empty(ObjectPool* pool) { return pool->add(new HashJoinProber(_hash_joiner)); }

private:
    HashJoiner& _hash_joiner;
    ChunkPtr _probe_chunk;
    Columns _key_columns;
    bool _current_probe_has_remain = false;
};

class HashJoinBuilder {
public:
    static constexpr size_t max_hash_table_element_size = UINT32_MAX;

    HashJoinBuilder(HashJoiner& hash_joiner) : _hash_joiner(hash_joiner) {}

    void create(const HashTableParam& param);

    JoinHashTable& hash_table() { return _ht; }

    void close();

    void reset(const HashTableParam& param);

    Status append_chunk(RuntimeState* state, const ChunkPtr& chunk);

    Status build(RuntimeState* state);

    size_t hash_table_row_count() { return _ht.get_row_count(); }

    void reset_probe(RuntimeState* state);

    HashJoinBuilder* clone_empty(ObjectPool* pool) { return pool->add(new HashJoinBuilder(_hash_joiner)); }

    bool ready() const { return _ready; }

    int64_t hash_table_mem_usage() const { return _ht.mem_usage(); }

private:
    HashJoiner& _hash_joiner;
    JoinHashTable _ht;
    Columns _key_columns;
    bool _ready = false;
};

} // namespace starrocks