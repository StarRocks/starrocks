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

#include "exec/hash_join_components.h"

#include "column/vectorized_fwd.h"
#include "exec/hash_joiner.h"

namespace starrocks {

void HashJoinProber::push_probe_chunk(RuntimeState* state, ChunkPtr&& chunk) {
    DCHECK(!_probe_chunk);
    _probe_chunk = std::move(chunk);
    _current_probe_has_remain = true;
    _hash_joiner.prepare_probe_key_columns(&_key_columns, _probe_chunk);
}

StatusOr<ChunkPtr> HashJoinProber::probe_chunk(RuntimeState* state, JoinHashTable* hash_table) {
    auto tmp_chunk = std::make_shared<Chunk>();
    auto chunk = std::make_shared<Chunk>();
    TRY_CATCH_ALLOC_SCOPE_START()
    DCHECK(_current_probe_has_remain && _probe_chunk);
    RETURN_IF_ERROR(hash_table->probe(state, _key_columns, &_probe_chunk, &tmp_chunk, &_current_probe_has_remain));
    RETURN_IF_ERROR(_hash_joiner.filter_probe_output_chunk(tmp_chunk, *hash_table));
    if (tmp_chunk && !tmp_chunk->is_empty()) {
        hash_table->lazy_materialize(&_probe_chunk, &tmp_chunk, &chunk);
    }
    if (!_current_probe_has_remain) {
        _probe_chunk = nullptr;
    }
    TRY_CATCH_ALLOC_SCOPE_END()
    return chunk;
}

StatusOr<ChunkPtr> HashJoinProber::probe_remain(RuntimeState* state, JoinHashTable* hash_table, bool* has_remain) {
    auto chunk = std::make_shared<Chunk>();
    TRY_CATCH_ALLOC_SCOPE_START()
    RETURN_IF_ERROR(hash_table->probe_remain(state, &chunk, &_current_probe_has_remain));
    *has_remain = _current_probe_has_remain;
    RETURN_IF_ERROR(_hash_joiner.filter_post_probe_output_chunk(chunk));
    TRY_CATCH_ALLOC_SCOPE_END()
    return chunk;
}

void HashJoinProber::reset() {
    _probe_chunk.reset();
    _current_probe_has_remain = false;
}

void HashJoinBuilder::create(const HashTableParam& param) {
    _ht.create(param);
}

void HashJoinBuilder::close() {
    _key_columns.clear();
    _ht.close();
}

void HashJoinBuilder::reset(const HashTableParam& param) {
    close();
    create(param);
}

void HashJoinBuilder::reset_probe(RuntimeState* state) {
    _key_columns.clear();
    _ht.reset_probe_state(state);
}

Status HashJoinBuilder::append_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (UNLIKELY(_ht.get_row_count() + chunk->num_rows() >= max_hash_table_element_size)) {
        return Status::NotSupported(strings::Substitute("row count of right table in hash join > $0", UINT32_MAX));
    }

    _hash_joiner.prepare_build_key_columns(&_key_columns, chunk);
    // copy chunk of right table
    SCOPED_TIMER(_hash_joiner.build_metrics().copy_right_table_chunk_timer);
    TRY_CATCH_BAD_ALLOC(_ht.append_chunk(state, chunk, _key_columns));
    return Status::OK();
}

Status HashJoinBuilder::build(RuntimeState* state) {
    SCOPED_TIMER(_hash_joiner.build_metrics().build_ht_timer);
    TRY_CATCH_BAD_ALLOC(RETURN_IF_ERROR(_ht.build(state)));
    _ready = true;
    return Status::OK();
}

} // namespace starrocks
