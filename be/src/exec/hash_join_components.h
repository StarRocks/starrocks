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

#include <functional>
#include <memory>

#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exec/join_hash_map.h"
#include "runtime/runtime_state.h"

namespace starrocks {
class HashJoiner;
struct HashJoinProbeMetrics;
class HashJoinBuilder;

class HashJoinProberImpl {
public:
    virtual ~HashJoinProberImpl() = default;
    virtual bool probe_chunk_empty() const = 0;
    virtual Status on_input_finished(RuntimeState* state) = 0;
    virtual Status push_probe_chunk(RuntimeState* state, ChunkPtr&& chunk) = 0;
    virtual StatusOr<ChunkPtr> probe_chunk(RuntimeState* state) = 0;
    virtual StatusOr<ChunkPtr> probe_remain(RuntimeState* state, bool* has_remain) = 0;
    virtual void reset(RuntimeState* runtime_state) = 0;

protected:
    HashJoinProberImpl(HashJoiner& hash_joiner) : _hash_joiner(hash_joiner) {}
    HashJoiner& _hash_joiner;
};

class HashJoinProber {
public:
    HashJoinProber(HashJoiner& hash_joiner) : _hash_joiner(hash_joiner) {}

    bool probe_chunk_empty() const { return _impl == nullptr || _impl->probe_chunk_empty(); }

    Status push_probe_chunk(RuntimeState* state, ChunkPtr&& chunk) {
        return _impl->push_probe_chunk(state, std::move(chunk));
    }

    Status on_input_finished(RuntimeState* state) {
        if (_impl == nullptr) {
            return Status::OK();
        }
        return _impl->on_input_finished(state);
    }

    // probe hash table
    StatusOr<ChunkPtr> probe_chunk(RuntimeState* state) { return _impl->probe_chunk(state); }

    StatusOr<ChunkPtr> probe_remain(RuntimeState* state, bool* has_remain) {
        return _impl->probe_remain(state, has_remain);
    }

    void reset(RuntimeState* runtime_state) { return _impl->reset(runtime_state); }

    HashJoinProber* clone_empty(ObjectPool* pool) { return pool->add(new HashJoinProber(_hash_joiner)); }

    void attach(HashJoinBuilder* builder, const HashJoinProbeMetrics& probe_metrics);

    bool has_attached() const { return _impl != nullptr; }

    bool need_input() const { return has_attached() && probe_chunk_empty(); }

private:
    HashJoiner& _hash_joiner;
    std::unique_ptr<HashJoinProberImpl> _impl;
};

// build hash table
class HashJoinBuilder {
public:
    static constexpr size_t max_hash_table_element_size = UINT32_MAX;

    HashJoinBuilder(HashJoiner& hash_joiner) : _hash_joiner(hash_joiner) {}
    virtual ~HashJoinBuilder() = default;

    virtual void create(const HashTableParam& param) = 0;

    // append chunk to hash table
    Status append_chunk(const ChunkPtr& chunk) {
        _inc_row_count(chunk->num_rows());
        return do_append_chunk(chunk);
    }
    virtual Status do_append_chunk(const ChunkPtr& chunk) = 0;

    virtual Status build(RuntimeState* state) = 0;

    virtual void close() = 0;

    virtual void reset(const HashTableParam& param) = 0;

    virtual int64_t ht_mem_usage() const = 0;

    // used for check NULL_AWARE_LEFT_ANTI_JOIN build side has null
    virtual bool anti_join_key_column_has_null() const = 0;

    bool ready() const { return _ready; }

    size_t hash_table_row_count() const { return _hash_table_row_count; }

    virtual size_t get_output_probe_column_count() const = 0;
    virtual size_t get_output_build_column_count() const = 0;

    virtual void get_build_info(size_t* bucket_size, float* avg_keys_per_bucket) = 0;

    virtual void visitHt(const std::function<void(JoinHashTable*)>& visitor) = 0;

    virtual std::unique_ptr<HashJoinProberImpl> create_prober() = 0;

    // clone readable to to builder
    virtual void clone_readable(HashJoinBuilder* builder) = 0;

    virtual ChunkPtr convert_to_spill_schema(const ChunkPtr& chunk) const = 0;

protected:
    HashJoiner& _hash_joiner;
    bool _ready = false;

private:
    size_t _hash_table_row_count = 0;
    void _inc_row_count(size_t num_rows) { _hash_table_row_count += num_rows; }
};

// HashJoinBuilder with single partition
class SingleHashJoinBuilder final : public HashJoinBuilder {
public:
    SingleHashJoinBuilder(HashJoiner& hash_joiner) : HashJoinBuilder(hash_joiner) {}

    void create(const HashTableParam& param) override;

    JoinHashTable& hash_table() { return _ht; }

    void close() override;

    void reset(const HashTableParam& param) override;

    Status do_append_chunk(const ChunkPtr& chunk) override;

    Status build(RuntimeState* state) override;

    bool anti_join_key_column_has_null() const override;

    int64_t ht_mem_usage() const override { return _ht.mem_usage(); }

    void get_build_info(size_t* bucket_size, float* avg_keys_per_bucket) override {
        *bucket_size = _ht.get_bucket_size();
        *avg_keys_per_bucket = _ht.get_keys_per_bucket();
    }

    size_t get_output_probe_column_count() const override { return _ht.get_output_probe_column_count(); }
    size_t get_output_build_column_count() const override { return _ht.get_output_build_column_count(); }

    void visitHt(const std::function<void(JoinHashTable*)>& visitor) override;

    std::unique_ptr<HashJoinProberImpl> create_prober() override;

    void clone_readable(HashJoinBuilder* builder) override;

    ChunkPtr convert_to_spill_schema(const ChunkPtr& chunk) const override;

private:
    JoinHashTable _ht;
    Columns _key_columns;
};

struct HashJoinBuildOptions {
    bool enable_partitioned_hash_join = false;
};

class HashJoinBuilderFactory {
public:
    static HashJoinBuilder* create(ObjectPool* pool, const HashJoinBuildOptions& options, HashJoiner& hash_joiner);
};
} // namespace starrocks