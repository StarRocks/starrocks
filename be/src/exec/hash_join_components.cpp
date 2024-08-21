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

#include <memory>

#include "column/vectorized_fwd.h"
#include "exec/hash_joiner.h"
#include "exec/join_hash_map.h"
#include "gutil/casts.h"

namespace starrocks {

class SingleHashJoinProberImpl final : public HashJoinProberImpl {
public:
    SingleHashJoinProberImpl(HashJoiner& hash_joiner) : HashJoinProberImpl(hash_joiner) {}
    ~SingleHashJoinProberImpl() override = default;
    bool probe_chunk_empty() const override { return _probe_chunk == nullptr; }
    Status push_probe_chunk(RuntimeState* state, ChunkPtr&& chunk) override;
    StatusOr<ChunkPtr> probe_chunk(RuntimeState* state) override;
    StatusOr<ChunkPtr> probe_remain(RuntimeState* state, bool* has_remain) override;
    void reset() override {
        _probe_chunk.reset();
        _current_probe_has_remain = false;
    }
    void set_ht(JoinHashTable* hash_table) { _hash_table = hash_table; }

private:
    JoinHashTable* _hash_table = nullptr;
    ChunkPtr _probe_chunk;
    Columns _key_columns;
    bool _current_probe_has_remain = false;
};

Status SingleHashJoinProberImpl::push_probe_chunk(RuntimeState* state, ChunkPtr&& chunk) {
    DCHECK(!_probe_chunk);
    _probe_chunk = std::move(chunk);
    _current_probe_has_remain = true;
    RETURN_IF_ERROR(_hash_joiner.prepare_probe_key_columns(&_key_columns, _probe_chunk));
    return Status::OK();
}

StatusOr<ChunkPtr> SingleHashJoinProberImpl::probe_chunk(RuntimeState* state) {
    auto chunk = std::make_shared<Chunk>();
    TRY_CATCH_ALLOC_SCOPE_START()
    DCHECK(_current_probe_has_remain && _probe_chunk);
    RETURN_IF_ERROR(_hash_table->probe(state, _key_columns, &_probe_chunk, &chunk, &_current_probe_has_remain));
    RETURN_IF_ERROR(_hash_joiner.filter_probe_output_chunk(chunk, *_hash_table));
    RETURN_IF_ERROR(_hash_joiner.lazy_output_chunk<false>(state, &_probe_chunk, &chunk, *_hash_table));
    if (!_current_probe_has_remain) {
        _probe_chunk = nullptr;
    }
    TRY_CATCH_ALLOC_SCOPE_END()
    return chunk;
}

StatusOr<ChunkPtr> SingleHashJoinProberImpl::probe_remain(RuntimeState* state, bool* has_remain) {
    auto chunk = std::make_shared<Chunk>();
    TRY_CATCH_ALLOC_SCOPE_START()
    RETURN_IF_ERROR(_hash_table->probe_remain(state, &chunk, &_current_probe_has_remain));
    *has_remain = _current_probe_has_remain;
    RETURN_IF_ERROR(_hash_joiner.filter_post_probe_output_chunk(chunk));
    RETURN_IF_ERROR(_hash_joiner.lazy_output_chunk<true>(state, nullptr, &chunk, *_hash_table));
    TRY_CATCH_ALLOC_SCOPE_END()
    return chunk;
}

void HashJoinProber::attach(HashJoinBuilder* builder, const HashJoinProbeMetrics& probe_metrics) {
    builder->visitHt([&](JoinHashTable* ht) {
        ht->set_probe_profile(probe_metrics.search_ht_timer, probe_metrics.output_probe_column_timer,
                              probe_metrics.output_build_column_timer, probe_metrics.probe_counter);
    });
    _impl = builder->create_prober();
}

void SingleHashJoinBuilder::create(const HashTableParam& param) {
    _ht.create(param);
}

void SingleHashJoinBuilder::close() {
    _key_columns.clear();
    _ht.close();
}

void SingleHashJoinBuilder::reset(const HashTableParam& param) {
    close();
    create(param);
}

void SingleHashJoinBuilder::reset_probe(RuntimeState* state) {
    _key_columns.clear();
    _ht.reset_probe_state(state);
}

bool SingleHashJoinBuilder::anti_join_key_column_has_null() const {
    if (_ht.get_key_columns().size() != 1) {
        return false;
    }
    auto& column = _ht.get_key_columns()[0];
    if (column->is_nullable()) {
        const auto& null_column = ColumnHelper::as_raw_column<NullableColumn>(column)->null_column();
        DCHECK_GT(null_column->size(), 0);
        return null_column->contain_value(1, null_column->size(), 1);
    }
    return false;
}

Status SingleHashJoinBuilder::do_append_chunk(const ChunkPtr& chunk) {
    if (UNLIKELY(_ht.get_row_count() + chunk->num_rows() >= max_hash_table_element_size)) {
        return Status::NotSupported(strings::Substitute("row count of right table in hash join > $0", UINT32_MAX));
    }

    RETURN_IF_ERROR(_hash_joiner.prepare_build_key_columns(&_key_columns, chunk));
    // copy chunk of right table
    SCOPED_TIMER(_hash_joiner.build_metrics().copy_right_table_chunk_timer);
    TRY_CATCH_BAD_ALLOC(_ht.append_chunk(chunk, _key_columns));
    return Status::OK();
}

Status SingleHashJoinBuilder::build(RuntimeState* state) {
    SCOPED_TIMER(_hash_joiner.build_metrics().build_ht_timer);
    TRY_CATCH_BAD_ALLOC(RETURN_IF_ERROR(_ht.build(state)));
    _ready = true;
    return Status::OK();
}

void SingleHashJoinBuilder::visitHt(const std::function<void(JoinHashTable*)>& visitor) {
    visitor(&_ht);
}

std::unique_ptr<HashJoinProberImpl> SingleHashJoinBuilder::create_prober() {
    auto res = std::make_unique<SingleHashJoinProberImpl>(_hash_joiner);
    res->set_ht(&_ht);
    return res;
}

void SingleHashJoinBuilder::clone_readable(HashJoinBuilder* builder) {
    auto* other = down_cast<SingleHashJoinBuilder*>(builder);
    other->_ht = _ht.clone_readable_table();
}

ChunkPtr SingleHashJoinBuilder::convert_to_spill_schema(const ChunkPtr& chunk) const {
    return _ht.convert_to_spill_schema(chunk);
}

} // namespace starrocks
