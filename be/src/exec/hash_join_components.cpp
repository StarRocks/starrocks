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

#include <deque>
#include <memory>
#include <numeric>

#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/object_pool.h"
#include "exec/hash_joiner.h"
#include "exec/join/join_hash_map.h"
#include "exprs/agg/distinct.h"
#include "exprs/expr_context.h"
#include "gutil/casts.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "util/cpu_info.h"
#include "util/runtime_profile.h"

namespace starrocks {

class SingleHashJoinProberImpl final : public HashJoinProberImpl {
public:
    SingleHashJoinProberImpl(HashJoiner& hash_joiner) : HashJoinProberImpl(hash_joiner) {}
    ~SingleHashJoinProberImpl() override = default;
    bool probe_chunk_empty() const override { return _probe_chunk == nullptr; }
    Status on_input_finished(RuntimeState* state) override { return Status::OK(); }
    Status push_probe_chunk(RuntimeState* state, ChunkPtr&& chunk) override;
    StatusOr<ChunkPtr> probe_chunk(RuntimeState* state) override;
    StatusOr<ChunkPtr> probe_remain(RuntimeState* state, bool* has_remain) override;
    void reset(RuntimeState* runtime_state) override {
        _probe_chunk.reset();
        _current_probe_has_remain = false;
        if (_hash_table != nullptr) {
            _hash_table->reset_probe_state(runtime_state);
        }
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

class PartitionChunkChannel {
public:
    PartitionChunkChannel(MemTracker* tracker) : _tracker(tracker) {}
    bool processing() const { return _processing; }
    void set_processing(bool processing) { _processing = processing; }

    ChunkPtr pull() {
        auto chunk = std::move(_chunks.front());
        _tracker->release(chunk->memory_usage());
        _chunks.pop_front();
        return chunk;
    }

    void push(ChunkPtr&& chunk) {
        _tracker->consume(chunk->memory_usage());
        _chunks.emplace_back(std::move(chunk));
    }

    const ChunkPtr& back() { return _chunks.back(); }

    void append_selective_to_back(const Chunk& src, const uint32_t* indexes, uint32_t from, uint32_t size) {
        auto& chunk = _chunks.back();
        const size_t prev_bytes = chunk->memory_usage();

        chunk->append_selective(src, indexes, from, size);
        const size_t new_bytes = chunk->memory_usage();

        _tracker->consume(new_bytes - prev_bytes);
    }

    bool is_full() const {
        return _chunks.size() >= 4 || _tracker->consumption() > config::partition_hash_join_probe_limit_size;
    }

    size_t size() const { return _chunks.size(); }

    bool is_empty() const { return _chunks.empty() || _chunks.front()->is_empty(); }

    bool not_empty() const { return !is_empty(); }

private:
    MemTracker* _tracker;
    std::deque<ChunkPtr> _chunks;
    bool _processing = false;
};

class PartitionedHashJoinProberImpl final : public HashJoinProberImpl {
public:
    PartitionedHashJoinProberImpl(HashJoiner& hash_joiner) : HashJoinProberImpl(hash_joiner) {}
    ~PartitionedHashJoinProberImpl() override = default;
    bool probe_chunk_empty() const override;
    Status on_input_finished(RuntimeState* state) override;
    Status push_probe_chunk(RuntimeState* state, ChunkPtr&& chunk) override;
    StatusOr<ChunkPtr> probe_chunk(RuntimeState* state) override;
    StatusOr<ChunkPtr> probe_remain(RuntimeState* state, bool* has_remain) override;
    void reset(RuntimeState* runtime_state) override;
    void set_probers(std::vector<std::unique_ptr<SingleHashJoinProberImpl>>&& probers) {
        _probers = std::move(probers);
        _partition_input_channels.resize(_probers.size(), PartitionChunkChannel(&_mem_tracker));
    }

private:
    MemTracker _mem_tracker;
    bool _all_input_finished = false;
    int32_t _remain_partition_idx = 0;
    std::vector<std::unique_ptr<SingleHashJoinProberImpl>> _probers;
    std::vector<PartitionChunkChannel> _partition_input_channels;
};

bool PartitionedHashJoinProberImpl::probe_chunk_empty() const {
    auto& probers = _probers;
    size_t num_partitions = probers.size();

    if (!_all_input_finished) {
        for (size_t i = 0; i < num_partitions; ++i) {
            if (!probers[i]->probe_chunk_empty() || _partition_input_channels[i].processing()) {
                return false;
            }
        }
    } else {
        for (size_t i = 0; i < num_partitions; ++i) {
            if (!probers[i]->probe_chunk_empty() || _partition_input_channels[i].not_empty()) {
                return false;
            }
        }
    }

    return true;
}

Status PartitionedHashJoinProberImpl::on_input_finished(RuntimeState* runtime_state) {
    SCOPED_TIMER(_hash_joiner.probe_metrics().partition_probe_overhead);
    _all_input_finished = true;
    auto& probers = _probers;
    size_t num_partitions = probers.size();

    for (size_t i = 0; i < num_partitions; ++i) {
        if (_partition_input_channels[i].is_empty()) {
            continue;
        }
        if (!probers[i]->probe_chunk_empty()) {
            continue;
        }
        RETURN_IF_ERROR(probers[i]->push_probe_chunk(runtime_state, _partition_input_channels[i].pull()));
    }
    return Status::OK();
}

Status PartitionedHashJoinProberImpl::push_probe_chunk(RuntimeState* state, ChunkPtr&& chunk) {
    SCOPED_TIMER(_hash_joiner.probe_metrics().partition_probe_overhead);
    auto& probers = _probers;
    auto& partition_keys = _hash_joiner.probe_expr_ctxs();

    size_t num_rows = chunk->num_rows();
    size_t num_partitions = probers.size();
    size_t num_partition_cols = partition_keys.size();

    Columns partition_columns(num_partition_cols);
    for (size_t i = 0; i < num_partition_cols; ++i) {
        ASSIGN_OR_RETURN(partition_columns[i], partition_keys[i]->evaluate(chunk.get()));
    }
    std::vector<uint32_t> hash_values;
    {
        hash_values.assign(num_rows, 0);

        for (const ColumnPtr& column : partition_columns) {
            column->crc32_hash(hash_values.data(), 0, num_rows);
        }
        // find partition id
        for (size_t i = 0; i < hash_values.size(); ++i) {
            hash_values[i] = HashUtil::fmix32(hash_values[i]) & (num_partitions - 1);
        }
    }

    const auto& partitions = hash_values;

    std::vector<uint32_t> selection;
    selection.resize(chunk->num_rows());

    std::vector<int32_t> channel_row_idx_start_points;
    channel_row_idx_start_points.assign(num_partitions + 1, 0);

    for (uint32_t i : partitions) {
        channel_row_idx_start_points[i]++;
    }

    for (int32_t i = 1; i <= channel_row_idx_start_points.size() - 1; ++i) {
        channel_row_idx_start_points[i] += channel_row_idx_start_points[i - 1];
    }

    for (int32_t i = chunk->num_rows() - 1; i >= 0; --i) {
        selection[channel_row_idx_start_points[partitions[i]] - 1] = i;
        channel_row_idx_start_points[partitions[i]]--;
    }
    _partition_input_channels.resize(num_partitions, PartitionChunkChannel(&_mem_tracker));

    for (size_t i = 0; i < num_partitions; ++i) {
        auto from = channel_row_idx_start_points[i];
        auto size = channel_row_idx_start_points[i + 1] - from;
        if (size == 0) {
            continue;
        }

        if (_partition_input_channels[i].is_empty()) {
            _partition_input_channels[i].push(chunk->clone_empty());
        }

        if (_partition_input_channels[i].back()->num_rows() + size <= state->chunk_size()) {
            _partition_input_channels[i].back()->append_selective(*chunk, selection.data(), from, size);
        } else {
            _partition_input_channels[i].push(chunk->clone_empty());
            _partition_input_channels[i].back()->append_selective(*chunk, selection.data(), from, size);
        }

        if (_partition_input_channels[i].is_full()) {
            _partition_input_channels[i].set_processing(true);
            RETURN_IF_ERROR(probers[i]->push_probe_chunk(state, _partition_input_channels[i].pull()));
        }
    }

    return Status::OK();
}

StatusOr<ChunkPtr> PartitionedHashJoinProberImpl::probe_chunk(RuntimeState* state) {
    auto& probers = _probers;
    size_t num_partitions = probers.size();
    if (_all_input_finished) {
        for (size_t i = 0; i < num_partitions; ++i) {
            if (probers[i]->probe_chunk_empty() && _partition_input_channels[i].is_empty()) {
                continue;
            }
            if (probers[i]->probe_chunk_empty()) {
                RETURN_IF_ERROR(probers[i]->push_probe_chunk(state, _partition_input_channels[i].pull()));
            }
            auto chunk = std::make_shared<Chunk>();
            ASSIGN_OR_RETURN(chunk, probers[i]->probe_chunk(state))
            return chunk;
        }
    } else {
        for (size_t i = 0; i < num_partitions; ++i) {
            if (probers[i]->probe_chunk_empty() && !_partition_input_channels[i].processing()) {
                continue;
            }
            if (probers[i]->probe_chunk_empty()) {
                RETURN_IF_ERROR(probers[i]->push_probe_chunk(state, _partition_input_channels[i].pull()));
            }
            _partition_input_channels[i].set_processing(_partition_input_channels[i].size() > 1);
            auto chunk = std::make_shared<Chunk>();
            ASSIGN_OR_RETURN(chunk, probers[i]->probe_chunk(state))
            return chunk;
        }
    }
    CHECK(false);

    return nullptr;
}

StatusOr<ChunkPtr> PartitionedHashJoinProberImpl::probe_remain(RuntimeState* state, bool* has_remain) {
    auto& probers = _probers;
    size_t num_partitions = probers.size();
    while (_remain_partition_idx < num_partitions) {
        auto chunk = std::make_shared<Chunk>();
        bool sub_map_has_remain = false;
        ASSIGN_OR_RETURN(chunk, probers[_remain_partition_idx]->probe_remain(state, &sub_map_has_remain));
        if (!sub_map_has_remain) {
            _remain_partition_idx++;
        }
        if (chunk->is_empty()) {
            continue;
        }
        *has_remain = true;
        return chunk;
    }

    *has_remain = false;
    return nullptr;
}

void PartitionedHashJoinProberImpl::reset(RuntimeState* runtime_state) {
    for (auto& prober : _probers) {
        prober->reset(runtime_state);
    }
    _partition_input_channels.clear();
    _all_input_finished = false;
    _remain_partition_idx = 0;
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

Status SingleHashJoinBuilder::do_append_chunk(RuntimeState* state, const ChunkPtr& chunk) {
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

enum class CacheLevel { L2, L3, MEMORY };

class AdaptivePartitionHashJoinBuilder final : public HashJoinBuilder {
public:
    explicit AdaptivePartitionHashJoinBuilder(HashJoiner& hash_joiner);
    ~AdaptivePartitionHashJoinBuilder() override = default;

    void create(const HashTableParam& param) override;

    void close() override;

    void reset(const HashTableParam& param) override;

    Status do_append_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    Status build(RuntimeState* state) override;

    bool anti_join_key_column_has_null() const override;

    int64_t ht_mem_usage() const override;

    void get_build_info(size_t* bucket_size, float* avg_keys_per_bucket, std::string* hash_map_type) override;

    size_t get_output_probe_column_count() const override;
    size_t get_output_build_column_count() const override;

    void visitHt(const std::function<void(JoinHashTable*)>& visitor) override;

    std::unique_ptr<HashJoinProberImpl> create_prober() override;

    void clone_readable(HashJoinBuilder* builder) override;

    Status prepare_for_spill_start(RuntimeState* state) override;
    ChunkPtr convert_to_spill_schema(const ChunkPtr& chunk) const override;

private:
    static double _calculate_cache_miss_factor(const HashJoiner& hash_joiner);

    size_t _estimate_hash_table_probing_bytes_per_row(const HashTableParam& param) const;
    size_t _estimate_probe_row_bytes(const HashTableParam& param) const;
    template <CacheLevel T>
    size_t _estimate_cost_by_bytes(size_t row_bytes) const;

    void _init_partition_nums(const HashTableParam& param);
    void _adjust_partition_rows(size_t hash_table_bytes_per_row, size_t hash_table_probing_bytes_per_row);

    Status _do_append_chunk(RuntimeState* state, const ChunkPtr& chunk);
    Status _append_chunk_to_partitions(RuntimeState* state, const ChunkPtr& chunk);
    Status _transfer_to_appending_stage(RuntimeState* state);
    Status _convert_to_single_partition(RuntimeState* state);
    Status _flush_buffer_chunks(RuntimeState* state);

    bool _need_partition_join_for_build(size_t ht_num_rows) const;
    bool _need_partition_join_for_append(size_t ht_num_rows) const;

private:
    std::vector<std::unique_ptr<SingleHashJoinBuilder>> _builders;

    // Split append chunk into two stages:
    // - BUFFERING: buffers chunks without partitioning until the number of rows exceeds _partition_join_l2_max_rows or _partition_join_l3_max_rows.
    // - APPENDING: partitions all incoming chunks.
    enum class Stage { BUFFERING, APPENDING };
    Stage _stage = Stage::BUFFERING;
    MemTracker _mem_tracker;
    std::vector<PartitionChunkChannel> _partition_input_channels;
    std::vector<ChunkPtr> _unpartition_chunks;

    size_t _partition_num = 0;

    size_t _hash_table_probing_bytes_per_row = 0;
    size_t _hash_table_bytes_per_row = 0;
    size_t _partition_join_l2_min_rows = 0;
    size_t _partition_join_l2_max_rows = 0;
    size_t _partition_join_l3_min_rows = 0;
    size_t _partition_join_l3_max_rows = 0;

    size_t _probe_row_shuffle_cost = 0;
    size_t _l2_benefit = 0;
    size_t _l3_benefit = 0;

    size_t _fit_L2_cache_max_rows = 0;
    size_t _fit_L3_cache_max_rows = 0;

    size_t _L2_cache_size = 0;
    size_t _L3_cache_size = 0;

    size_t _pushed_chunks = 0;

    // Shared read-only data accessed concurrently by threads can lead to better cache performance.
    // Therefore, for broadcast joins, this parameter is used to reduce benefit of partitioned hash joins as the number
    // of prober threads (DOP) increases.
    const double _cache_miss_factor;
};

AdaptivePartitionHashJoinBuilder::AdaptivePartitionHashJoinBuilder(HashJoiner& hash_joiner)
        : HashJoinBuilder(hash_joiner), _cache_miss_factor(_calculate_cache_miss_factor(hash_joiner)) {
    static constexpr size_t DEFAULT_L2_CACHE_SIZE = 1 * 1024 * 1024;
    static constexpr size_t DEFAULT_L3_CACHE_SIZE = 32 * 1024 * 1024;
    const auto& cache_sizes = CpuInfo::get_cache_sizes();
    _L2_cache_size = cache_sizes[CpuInfo::L2_CACHE];
    _L3_cache_size = cache_sizes[CpuInfo::L3_CACHE];
    _L2_cache_size = _L2_cache_size ? _L2_cache_size : DEFAULT_L2_CACHE_SIZE;
    _L3_cache_size = _L3_cache_size ? _L3_cache_size : DEFAULT_L3_CACHE_SIZE;
}

double AdaptivePartitionHashJoinBuilder::_calculate_cache_miss_factor(const HashJoiner& hash_joiner) {
    if (hash_joiner.distribution_mode() != TJoinDistributionMode::BROADCAST) {
        return 1.0; // No broadcast join, no cache reuse between different probers.
    }

    const size_t max_prober_dop = hash_joiner.max_dop();
    if (max_prober_dop <= 1) {
        return 1.0;
    }
    if (max_prober_dop > 8) {
        return 0.1;
    }
    return 1 - (max_prober_dop - 1) * 0.1;
}

size_t AdaptivePartitionHashJoinBuilder::_estimate_hash_table_probing_bytes_per_row(const HashTableParam& param) const {
    size_t estimated_each_row = 0;

    // Probing a row need
    // 1. touch `first` and `next` vectors,
    // 2 and compare join keys between builder and prober.
    // 3. output columns from the build side.

    // 1. `first` and `next` bytes
    estimated_each_row += 8;

    // 2. key bytes
    for (const auto& join_key : param.join_keys) {
        if (join_key.type != nullptr) {
            estimated_each_row += get_size_of_fixed_length_type(join_key.type->type);
            // The benefit from non-fixed key columns is less than those from fixed key columns, so the penalty (/4) is applied here.
            estimated_each_row += type_estimated_overhead_bytes(join_key.type->type) / 4;
        }
    }

    // 3. output bytes
    for (auto* tuple : param.build_row_desc->tuple_descriptors()) {
        for (const auto* slot : tuple->slots()) {
            if (param.build_output_slots.empty() || param.build_output_slots.contains(slot->id())) {
                estimated_each_row += get_size_of_fixed_length_type(slot->type().type);
                estimated_each_row += type_estimated_overhead_bytes(slot->type().type);
            }
        }
    }

    return std::max<size_t>(estimated_each_row * _cache_miss_factor, 1);
}

// We could use a better estimation model.
size_t AdaptivePartitionHashJoinBuilder::_estimate_probe_row_bytes(const HashTableParam& param) const {
    size_t size = 0;

    // shuffling probe bytes
    for (auto* tuple : param.probe_row_desc->tuple_descriptors()) {
        for (const auto* slot : tuple->slots()) {
            size += get_size_of_fixed_length_type(slot->type().type);
            size += type_estimated_overhead_bytes(slot->type().type);
        }
    }

    return std::max<size_t>(size, 1);
}

template <>
size_t AdaptivePartitionHashJoinBuilder::_estimate_cost_by_bytes<CacheLevel::L2>(size_t row_bytes) const {
    return row_bytes / 2;
}
template <>
size_t AdaptivePartitionHashJoinBuilder::_estimate_cost_by_bytes<CacheLevel::L3>(size_t row_bytes) const {
    return row_bytes;
}
template <>
size_t AdaptivePartitionHashJoinBuilder::_estimate_cost_by_bytes<CacheLevel::MEMORY>(size_t row_bytes) const {
    return row_bytes * 2;
}

bool AdaptivePartitionHashJoinBuilder::_need_partition_join_for_build(size_t ht_num_rows) const {
    return (_partition_join_l2_min_rows < ht_num_rows && ht_num_rows <= _partition_join_l2_max_rows) ||
           (_partition_join_l3_min_rows < ht_num_rows && ht_num_rows <= _partition_join_l3_max_rows);
}

bool AdaptivePartitionHashJoinBuilder::_need_partition_join_for_append(size_t ht_num_rows) const {
    return ht_num_rows <= _partition_join_l2_max_rows || ht_num_rows <= _partition_join_l3_max_rows;
}

void AdaptivePartitionHashJoinBuilder::_adjust_partition_rows(size_t hash_table_bytes_per_row,
                                                              size_t hash_table_probing_bytes_per_row) {
    if (hash_table_bytes_per_row == _hash_table_bytes_per_row &&
        hash_table_probing_bytes_per_row == _hash_table_probing_bytes_per_row) {
        return; // No need to adjust partition rows.
    }

    _hash_table_bytes_per_row = hash_table_bytes_per_row;
    _hash_table_probing_bytes_per_row = hash_table_probing_bytes_per_row;

    hash_table_bytes_per_row = std::max<size_t>(hash_table_bytes_per_row, 1);

    _fit_L2_cache_max_rows = _L2_cache_size / hash_table_bytes_per_row;
    _fit_L3_cache_max_rows = _L3_cache_size / hash_table_bytes_per_row;

    _partition_join_l2_min_rows = -1;
    _partition_join_l2_max_rows = 0;
    _partition_join_l3_min_rows = -1;
    _partition_join_l3_max_rows = 0;

    const auto l2_benefit = _estimate_cost_by_bytes<CacheLevel::L3>(hash_table_probing_bytes_per_row) -
                            _estimate_cost_by_bytes<CacheLevel::L2>(hash_table_probing_bytes_per_row);
    const auto l3_benefit = _estimate_cost_by_bytes<CacheLevel::MEMORY>(hash_table_probing_bytes_per_row) -
                            _estimate_cost_by_bytes<CacheLevel::L3>(hash_table_probing_bytes_per_row);

    if (_probe_row_shuffle_cost < l3_benefit) { // Partitioned joins benefit from L3 cache.
        // Partitioned joins benefit from L3 cache when probing a row has cache miss in non-partitioned join but not in partitioned join.
        // 1. min_rows > (l3_cache_size/hash_table_bytes_per_row)*(l3_benefit/(l3_benefit-_probe_row_shuffle_cost)), because:
        //   - l3_benefit * non_partition_cache_miss_rate > _probe_row_shuffle_cost
        //   - non_partition_cache_miss_rate = 1 - l3_cache_size/(min_rows*hash_table_bytes_per_row)
        // 2. max_rows < (l3_cache_size/hash_table_bytes_per_row)*(l3_benefit/_probe_row_shuffle_cost)*num_partitions, because:
        //   - l3_benefit * partition_cache_hit_rate > _probe_row_shuffle_cost
        //   - partition_cache_hit_rate = l3_cache_size/(max_rows_per_partition*hash_table_bytes_per_row)
        _partition_join_l3_min_rows = _fit_L3_cache_max_rows * l3_benefit / (l3_benefit - _probe_row_shuffle_cost);
        _partition_join_l3_max_rows = _fit_L3_cache_max_rows * _partition_num * l3_benefit / _probe_row_shuffle_cost;
        _partition_join_l3_max_rows *= 2; // relax the restriction

        if (_probe_row_shuffle_cost < l2_benefit) { // Partitioned joins benefit from L2 cache.
            _partition_join_l2_min_rows = _fit_L2_cache_max_rows * l2_benefit / (l2_benefit - _probe_row_shuffle_cost);
            _partition_join_l2_min_rows *= 2; // Make the restriction more stringent
            _partition_join_l2_max_rows =
                    (_fit_L2_cache_max_rows * _partition_num) * l2_benefit / _probe_row_shuffle_cost;
        }
    } else {
        // Partitioned joins don't have performance gains. Not using partition hash join.
        _partition_num = 1;
    }

    _l2_benefit = l2_benefit;
    _l3_benefit = l3_benefit;

    VLOG_OPERATOR << "TRACE: _adjust_partition_rows "
                  << "[partition_num=" << _partition_num << "] "
                  << "[partition_join_l2_min_rows=" << _partition_join_l2_min_rows << "] "
                  << "[partition_join_l2_max_rows=" << _partition_join_l2_max_rows << "] "
                  << "[partition_join_l3_min_rows=" << _partition_join_l3_min_rows << "] "
                  << "[partition_join_l3_max_rows=" << _partition_join_l3_max_rows << "] "
                  << "[hash_table_probing_bytes_per_row=" << hash_table_probing_bytes_per_row << "] "
                  << "[hash_table_bytes_per_row=" << hash_table_bytes_per_row << "] "
                  << "[l2_benefit=" << l2_benefit << "] "
                  << "[l3_benefit=" << l3_benefit << "] "
                  << "[probe_shuffle_cost=" << _probe_row_shuffle_cost << "] ";
}

void AdaptivePartitionHashJoinBuilder::_init_partition_nums(const HashTableParam& param) {
    _partition_num = 16;

    _probe_row_shuffle_cost =
            std::max<size_t>(_estimate_cost_by_bytes<CacheLevel::L3>(_estimate_probe_row_bytes(param)), 1);

    const size_t hash_table_probing_bytes_per_row = _estimate_hash_table_probing_bytes_per_row(param);
    _adjust_partition_rows(1, hash_table_probing_bytes_per_row);

    COUNTER_SET(_hash_joiner.build_metrics().partition_nums, static_cast<int64_t>(_partition_num));
}

void AdaptivePartitionHashJoinBuilder::create(const HashTableParam& param) {
    _init_partition_nums(param);

    if (_partition_num > 1) {
        _partition_input_channels.resize(_partition_num, PartitionChunkChannel(&_mem_tracker));
    }
    for (size_t i = 0; i < _partition_num; ++i) {
        _builders.emplace_back(std::make_unique<SingleHashJoinBuilder>(_hash_joiner));
        _builders.back()->create(param);
    }
}

void AdaptivePartitionHashJoinBuilder::close() {
    for (const auto& builder : _builders) {
        builder->close();
    }
    _builders.clear();
    _partition_input_channels.clear();
    _partition_num = 0;
    _partition_join_l2_min_rows = 0;
    _partition_join_l2_max_rows = 0;
    _partition_join_l3_min_rows = 0;
    _partition_join_l3_max_rows = 0;
    _probe_row_shuffle_cost = 0;
    _hash_table_probing_bytes_per_row = 0;
    _fit_L2_cache_max_rows = 0;
    _fit_L3_cache_max_rows = 0;
    _pushed_chunks = 0;
}

void AdaptivePartitionHashJoinBuilder::reset(const HashTableParam& param) {
    close();
    create(param);
}

bool AdaptivePartitionHashJoinBuilder::anti_join_key_column_has_null() const {
    return std::any_of(_builders.begin(), _builders.end(),
                       [](const auto& builder) { return builder->anti_join_key_column_has_null(); });
}

void AdaptivePartitionHashJoinBuilder::get_build_info(size_t* bucket_size, float* avg_keys_per_bucket,
                                                      std::string* hash_map_type) {
    size_t total_bucket_size = 0;
    float total_keys_per_bucket = 0;
    std::unordered_map<std::string, uint32_t> hash_map_types;
    for (const auto& builder : _builders) {
        size_t bucket_size = 0;
        float keys_per_bucket = 0;
        std::string cur_hash_map_type;
        builder->get_build_info(&bucket_size, &keys_per_bucket, &cur_hash_map_type);
        total_bucket_size += bucket_size;
        total_keys_per_bucket += keys_per_bucket;
        hash_map_types[cur_hash_map_type]++;
    }
    *bucket_size = total_bucket_size;
    *avg_keys_per_bucket = total_keys_per_bucket / _builders.size();
    std::string merged_hash_map_type;
    for (const auto& [type, count] : hash_map_types) {
        if (!merged_hash_map_type.empty()) {
            merged_hash_map_type += ", ";
        }
        merged_hash_map_type += type + ":" + std::to_string(count);
    }
    *hash_map_type = merged_hash_map_type;
}

size_t AdaptivePartitionHashJoinBuilder::get_output_probe_column_count() const {
    return _builders[0]->get_output_probe_column_count();
}

size_t AdaptivePartitionHashJoinBuilder::get_output_build_column_count() const {
    return _builders[0]->get_output_build_column_count();
}

int64_t AdaptivePartitionHashJoinBuilder::ht_mem_usage() const {
    return std::accumulate(_builders.begin(), _builders.end(), 0L,
                           [](int64_t sum, const auto& builder) { return sum + builder->ht_mem_usage(); });
}

Status AdaptivePartitionHashJoinBuilder::_convert_to_single_partition(RuntimeState* state) {
    VLOG_OPERATOR << "TRACE: convert_to_single_partition "
                  << "[partition_num=" << _partition_num << "] "
                  << "[partition_join_l2_min_rows=" << _partition_join_l2_min_rows << "] "
                  << "[partition_join_l2_max_rows=" << _partition_join_l2_max_rows << "] "
                  << "[partition_join_l3_min_rows=" << _partition_join_l3_min_rows << "] "
                  << "[partition_join_l3_max_rows=" << _partition_join_l3_max_rows << "] "
                  << "[hash_table_row_count=" << hash_table_row_count() << "] ";

    // merge all partition data to the first partition
    if (_stage == Stage::BUFFERING) {
        _mem_tracker.set(0);
        for (const auto& unpartition_chunk : _unpartition_chunks) {
            RETURN_IF_ERROR(_builders[0]->do_append_chunk(state, unpartition_chunk));
        }
        _unpartition_chunks.clear();
    } else {
        for (size_t i = 0; i < _builders.size(); ++i) {
            if (i != 0) {
                _builders[0]->hash_table().merge_ht(_builders[i]->hash_table());
            }
            auto& channel = _partition_input_channels[i];
            while (!channel.is_empty()) {
                RETURN_IF_ERROR(_builders[0]->do_append_chunk(state, channel.pull()));
            }
        }
        _partition_input_channels.clear();
    }
    _builders.resize(1);

    _partition_num = 1;
    COUNTER_SET(_hash_joiner.build_metrics().partition_nums, static_cast<int64_t>(1));

    return Status::OK();
}

Status AdaptivePartitionHashJoinBuilder::_transfer_to_appending_stage(RuntimeState* state) {
    _stage = Stage::APPENDING;
    _mem_tracker.set(0); // All the buffered chunks are moved to the partition builders, so clear the memory tracker.
    for (const auto& unpartition_chunk : _unpartition_chunks) {
        RETURN_IF_ERROR(_append_chunk_to_partitions(state, unpartition_chunk));
    }
    _unpartition_chunks.clear();

    return Status::OK();
}

Status AdaptivePartitionHashJoinBuilder::_do_append_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (_stage == Stage::BUFFERING) {
        _mem_tracker.consume(chunk->memory_usage());
        _unpartition_chunks.push_back(chunk);

        const size_t num_rows = hash_table_row_count();
        if (num_rows >= _partition_join_l2_min_rows || num_rows >= _partition_join_l3_min_rows) {
            RETURN_IF_ERROR(_transfer_to_appending_stage(state));
        }

        return Status::OK();
    } else {
        return _append_chunk_to_partitions(state, chunk);
    }
}

Status AdaptivePartitionHashJoinBuilder::_append_chunk_to_partitions(RuntimeState* state, const ChunkPtr& chunk) {
    const std::vector<ExprContext*>& build_partition_keys = _hash_joiner.build_expr_ctxs();

    size_t num_rows = chunk->num_rows();
    size_t num_partitions = _builders.size();
    size_t num_partition_cols = build_partition_keys.size();

    Columns partition_columns(num_partition_cols);
    for (size_t i = 0; i < num_partition_cols; ++i) {
        ASSIGN_OR_RETURN(partition_columns[i], build_partition_keys[i]->evaluate(chunk.get()));
    }
    std::vector<uint32_t> hash_values;
    {
        hash_values.assign(num_rows, 0);

        for (const ColumnPtr& column : partition_columns) {
            column->crc32_hash(hash_values.data(), 0, num_rows);
        }
        // find partition id
        for (size_t i = 0; i < hash_values.size(); ++i) {
            hash_values[i] = HashUtil::fmix32(hash_values[i]) & (num_partitions - 1);
        }
    }

    const auto& partitions = hash_values;

    std::vector<uint32_t> selection;
    selection.resize(chunk->num_rows());

    std::vector<int32_t> channel_row_idx_start_points;
    channel_row_idx_start_points.assign(num_partitions + 1, 0);

    for (uint32_t i : partitions) {
        channel_row_idx_start_points[i]++;
    }

    for (int32_t i = 1; i <= channel_row_idx_start_points.size() - 1; ++i) {
        channel_row_idx_start_points[i] += channel_row_idx_start_points[i - 1];
    }

    for (int32_t i = chunk->num_rows() - 1; i >= 0; --i) {
        selection[channel_row_idx_start_points[partitions[i]] - 1] = i;
        channel_row_idx_start_points[partitions[i]]--;
    }

    for (size_t i = 0; i < num_partitions; ++i) {
        auto from = channel_row_idx_start_points[i];
        auto size = channel_row_idx_start_points[i + 1] - from;
        if (size == 0) {
            continue;
        }

        auto& channel = _partition_input_channels[i];

        if (channel.is_empty()) {
            channel.push(chunk->clone_empty());
        }

        if (channel.back()->num_rows() + size <= state->chunk_size()) {
            channel.append_selective_to_back(*chunk, selection.data(), from, size);
        } else {
            channel.push(chunk->clone_empty());
            channel.append_selective_to_back(*chunk, selection.data(), from, size);
        }

        while (channel.is_full()) {
            RETURN_IF_ERROR(_builders[i]->append_chunk(state, channel.pull()));
        }
    }
    return Status::OK();
}

Status AdaptivePartitionHashJoinBuilder::do_append_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (_partition_num > 1 && !_need_partition_join_for_append(hash_table_row_count())) {
        RETURN_IF_ERROR(_convert_to_single_partition(state));
    }

    if (_partition_num > 1 && ++_pushed_chunks % 8 == 0) {
        const size_t build_row_size = (ht_mem_usage() + _mem_tracker.consumption()) / hash_table_row_count();
        _adjust_partition_rows(build_row_size, _hash_table_probing_bytes_per_row);
        if (_partition_num == 1) {
            RETURN_IF_ERROR(_convert_to_single_partition(state));
        }
    }

    if (_partition_num > 1) {
        RETURN_IF_ERROR(_do_append_chunk(state, chunk));
    } else {
        RETURN_IF_ERROR(_builders[0]->do_append_chunk(state, chunk));
    }

    return Status::OK();
}

Status AdaptivePartitionHashJoinBuilder::prepare_for_spill_start(RuntimeState* state) {
    if (_partition_num > 1) {
        return _flush_buffer_chunks(state);
    }
    return Status::OK();
}

ChunkPtr AdaptivePartitionHashJoinBuilder::convert_to_spill_schema(const ChunkPtr& chunk) const {
    return _builders[0]->convert_to_spill_schema(chunk);
}

Status AdaptivePartitionHashJoinBuilder::_flush_buffer_chunks(RuntimeState* state) {
    if (_stage == Stage::BUFFERING) {
        RETURN_IF_ERROR(_transfer_to_appending_stage(state));
    }
    for (size_t i = 0; i < _partition_input_channels.size(); ++i) {
        auto& channel = _partition_input_channels[i];
        while (!channel.is_empty()) {
            RETURN_IF_ERROR(_builders[i]->do_append_chunk(state, channel.pull()));
        }
    }

    return Status::OK();
}

Status AdaptivePartitionHashJoinBuilder::build(RuntimeState* state) {
    DCHECK_EQ(_partition_num, _builders.size());

    if (_partition_num > 1) {
        if (!_need_partition_join_for_build(hash_table_row_count())) {
            RETURN_IF_ERROR(_convert_to_single_partition(state));
        } else {
            RETURN_IF_ERROR(_flush_buffer_chunks(state));
        }
    }

    for (auto& builder : _builders) {
        RETURN_IF_ERROR(builder->build(state));
    }
    _ready = true;
    return Status::OK();
}

void AdaptivePartitionHashJoinBuilder::visitHt(const std::function<void(JoinHashTable*)>& visitor) {
    for (auto& builder : _builders) {
        builder->visitHt(visitor);
    }
}

std::unique_ptr<HashJoinProberImpl> AdaptivePartitionHashJoinBuilder::create_prober() {
    DCHECK_EQ(_partition_num, _builders.size());

    if (_partition_num == 1) {
        return _builders[0]->create_prober();
    } else {
        std::vector<std::unique_ptr<SingleHashJoinProberImpl>> sub_probers;
        auto prober = std::make_unique<PartitionedHashJoinProberImpl>(_hash_joiner);
        sub_probers.resize(_partition_num);
        for (size_t i = 0; i < _builders.size(); ++i) {
            sub_probers[i].reset(down_cast<SingleHashJoinProberImpl*>(_builders[i]->create_prober().release()));
        }
        prober->set_probers(std::move(sub_probers));
        return prober;
    }
}

void AdaptivePartitionHashJoinBuilder::clone_readable(HashJoinBuilder* other_builder) {
    for (auto& builder : _builders) {
        DCHECK(builder->ready());
    }
    DCHECK(_ready);
    DCHECK_EQ(_partition_num, _builders.size());
    auto other = down_cast<AdaptivePartitionHashJoinBuilder*>(other_builder);
    other->_builders.clear();
    other->_partition_num = _partition_num;
    other->_partition_join_l2_min_rows = _partition_join_l2_min_rows;
    other->_partition_join_l2_max_rows = _partition_join_l2_max_rows;
    other->_partition_join_l3_min_rows = _partition_join_l3_min_rows;
    other->_partition_join_l3_max_rows = _partition_join_l3_max_rows;
    other->_partition_join_l3_max_rows = _partition_join_l3_max_rows;
    other->_ready = _ready;
    for (size_t i = 0; i < _partition_num; ++i) {
        other->_builders.emplace_back(std::make_unique<SingleHashJoinBuilder>(_hash_joiner));
        _builders[i]->clone_readable(other->_builders[i].get());
    }
}

HashJoinBuilder* HashJoinBuilderFactory::create(ObjectPool* pool, const HashJoinBuildOptions& options,
                                                HashJoiner& hash_joiner) {
    if (options.enable_partitioned_hash_join) {
        return pool->add(new AdaptivePartitionHashJoinBuilder(hash_joiner));
    } else {
        return pool->add(new SingleHashJoinBuilder(hash_joiner));
    }
}

} // namespace starrocks
