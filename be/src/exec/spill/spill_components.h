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
#include <queue>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exec/spill/block_manager.h"
#include "exec/spill/executor.h"
#include "exec/spill/input_stream.h"
#include "exec/spill/mem_table.h"
#include "exec/spill/options.h"
#include "exec/spill/partition.h"
#include "exec/spill/serde.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"

namespace starrocks::spill {
class Spiller;

using FlushAllCallBack = std::function<Status()>;
using SpillHashColumn = UInt32Column;

class SpillerReader {
public:
    SpillerReader(Spiller* spiller) : _spiller(spiller) {}

    virtual ~SpillerReader() = default;

    Status set_stream(std::shared_ptr<SpillInputStream> stream) {
        std::lock_guard guard(_mutex);
        _stream = std::move(stream);
        return Status::OK();
    }

    template <class TaskExecutor, class MemGuard>
    StatusOr<ChunkPtr> restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard);

    template <class TaskExecutor, class MemGuard>
    Status trigger_restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard);

    bool has_output_data() { return _stream && _stream->is_ready(); }

    bool restore_finished() const { return _running_restore_tasks == 0; }
    uint64_t running_restore_tasks() const { return _running_restore_tasks.load(); }

    bool has_restore_task() const { return _running_restore_tasks > 0; }

    size_t read_rows() const { return _read_rows; }

protected:
    Spiller* _spiller;
    std::atomic_uint64_t _running_restore_tasks{};
    std::atomic_uint64_t _finished_restore_tasks{};

    std::mutex _mutex;
    std::shared_ptr<SpillInputStream> _stream;
    SerdeContext _spill_read_ctx;

    size_t _read_rows{};
};

class SpillerWriter {
public:
    SpillerWriter(Spiller* spiller, RuntimeState* state) : _spiller(spiller), _runtime_state(state) {}
    virtual ~SpillerWriter() = default;
    virtual bool is_full() = 0;
    virtual bool has_pending_data() = 0;

    virtual Status prepare(RuntimeState* state) = 0;

    virtual Status set_flush_all_call_back(FlushAllCallBack callback) = 0;
    // acquire input stream
    // return empty if writer has multi-partitions
    virtual Status acquire_stream(std::shared_ptr<SpillInputStream>* stream) = 0;

    virtual Status acquire_stream(const SpillPartitionInfo* partition, std::shared_ptr<SpillInputStream>* stream) = 0;

    virtual void cancel() = 0;

    virtual Status get_spill_partitions(std::vector<const SpillPartitionInfo*>* partitions) = 0;

    template <class T>
    T as() {
        return down_cast<T>(this);
    }
    uint64_t running_flush_tasks() const { return _running_flush_tasks.load(); }

protected:
    Status _decrease_running_flush_tasks();

    const SpilledOptions& options();

    Spiller* _spiller;
    RuntimeState* _runtime_state;
    std::atomic_uint64_t _running_flush_tasks{};

    FlushAllCallBack _flush_all_callback;
};

class RawSpillerWriter final : public SpillerWriter {
private:
    MemTablePtr _acquire_mem_table_from_pool() {
        std::lock_guard guard(_mutex);
        if (_mem_table_pool.empty()) {
            return nullptr;
        }
        auto res = std::move(_mem_table_pool.front());
        _mem_table_pool.pop();
        return res;
    }

public:
    RawSpillerWriter(Spiller* spiller, RuntimeState* state, MemTracker* tracker)
            : SpillerWriter(spiller, state), _parent_tracker(tracker) {}
    RawSpillerWriter(Spiller* spiller, RuntimeState* state) : RawSpillerWriter(spiller, state, nullptr) {}

    ~RawSpillerWriter() override = default;

    bool is_full() override {
        std::lock_guard guard(_mutex);
        return _mem_table_pool.empty() && _mem_table == nullptr;
    }

    bool has_pending_data() override;

    Status set_flush_all_call_back(FlushAllCallBack callback) override {
        _running_flush_tasks++;
        _flush_all_callback = std::move(callback);
        return _decrease_running_flush_tasks();
    }

    template <class TaskExecutor, class MemGuard>
    Status spill(RuntimeState* state, const ChunkPtr& chunk, TaskExecutor&& executor, MemGuard&& guard);

    template <class TaskExecutor, class MemGuard>
    Status flush(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard);

    Status prepare(RuntimeState* state) override;

    Status flush_task(RuntimeState* state, const MemTablePtr& mem_table);

    void acquire_mem_table() {
        if (_mem_table == nullptr) {
            _mem_table = _acquire_mem_table_from_pool();
        }
    }

    const auto& mem_table() const { return _mem_table; }

    BlockPtr& block() { return _block; }

    BlockGroup& block_group() { return _block_group; }

    Status acquire_stream(std::shared_ptr<SpillInputStream>* stream) override;

    Status acquire_stream(const SpillPartitionInfo* partition, std::shared_ptr<SpillInputStream>* stream) override;

    void cancel() override {}

    Status get_spill_partitions(std::vector<const SpillPartitionInfo*>* partitions) override { return Status::OK(); }

private:
    BlockGroup _block_group;
    BlockPtr _block;
    MemTablePtr _mem_table;
    std::queue<MemTablePtr> _mem_table_pool;
    std::mutex _mutex;
    SerdeContext _spill_read_ctx;
    MemTracker* _parent_tracker = nullptr;
};
struct SpilledPartition;
using SpilledPartitionPtr = std::unique_ptr<SpilledPartition>;

struct SpilledPartition : public SpillPartitionInfo {
    SpilledPartition(int32_t partition_id_) : SpillPartitionInfo(partition_id_) {}

    // split partition to next level partition
    std::pair<SpilledPartitionPtr, SpilledPartitionPtr> split() {
        return {std::make_unique<SpilledPartition>(partition_id + level_elements()),
                std::make_unique<SpilledPartition>(partition_id + level_elements() * 2)};
    }

    bool is_spliting = false;
    std::unique_ptr<RawSpillerWriter> spill_writer;
};

class PartitionedSpillerWriter final : public SpillerWriter {
public:
    static const constexpr auto max_partition_size = 1024;
    static const constexpr auto max_partition_level = 6;
    PartitionedSpillerWriter(Spiller* spiller, RuntimeState* state);

    Status prepare(RuntimeState* state) override;

    bool is_full() override { return _running_flush_tasks != 0; }

    bool has_pending_data() override { return _running_flush_tasks != 0; }

    Status acquire_stream(std::shared_ptr<SpillInputStream>* stream) override { return Status::OK(); }

    Status acquire_stream(const SpillPartitionInfo* partition, std::shared_ptr<SpillInputStream>* stream) override;

    Status set_flush_all_call_back(FlushAllCallBack callback) override {
        _running_flush_tasks++;
        _flush_all_callback = std::move(callback);
        return _decrease_running_flush_tasks();
    }

    template <class TaskExecutor, class MemGuard>
    Status spill(RuntimeState* state, const ChunkPtr& chunk, TaskExecutor&& executor, MemGuard&& guard);

    template <class TaskExecutor, class MemGuard>
    Status flush(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard);

    template <class TaskExecutor, class MemGuard>
    Status flush_if_full(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard);

    Status get_spill_partitions(std::vector<const SpillPartitionInfo*>* partitions) override;

    Status reset_partition(const std::vector<const SpillPartitionInfo*>& partitions);

    Status reset_partition(RuntimeState* state, size_t num_partitions);

    void cancel() override {}

    void shuffle(std::vector<uint32_t>& dst, const SpillHashColumn* hash_column);

    template <class Processer>
    void process_partition_data(const ChunkPtr& chunk, const std::vector<uint32_t>& shuffle_result,
                                Processer&& processer) {
        std::vector<uint32_t> selection;
        selection.resize(chunk->num_rows());

        std::vector<int32_t> channel_row_idx_start_points;
        channel_row_idx_start_points.assign(_max_partition_id + 2, 0);

        for (uint32_t i : shuffle_result) {
            channel_row_idx_start_points[i]++;
        }

        for (int32_t i = 1; i <= channel_row_idx_start_points.size() - 1; ++i) {
            channel_row_idx_start_points[i] += channel_row_idx_start_points[i - 1];
        }

        for (int32_t i = chunk->num_rows() - 1; i >= 0; --i) {
            selection[channel_row_idx_start_points[shuffle_result[i]] - 1] = i;
            channel_row_idx_start_points[shuffle_result[i]]--;
        }

        for (const auto& [pid, partition] : _id_to_partitions) {
            auto from = channel_row_idx_start_points[pid];
            auto size = channel_row_idx_start_points[pid + 1] - from;
            if (size == 0) {
                continue;
            }
            processer(partition, selection, from, size);
        }
    }

    const auto& level_to_partitions() { return _level_to_partitions; }

    template <class ChunkProvider>
    Status spill_partition(SerdeContext& context, SpilledPartition* partition, ChunkProvider&& provider);

    int64_t mem_consumption() const { return _mem_tracker->consumption(); }

private:
    Status _init_with_partition_nums(RuntimeState* state, int num_partitions);
    // prepare and acquire mem_table for each partition in _id_to_partitions
    Status _prepare_partitions(RuntimeState* state);

    // split partition by hash
    // hash-based partitioning can have significant degradation in the case of heavily skewed data.
    // TODO:
    // 1. We can actually split partitions based on blocks (they all belong to the same partition, but
    // can be executed in splitting out more parallel tasks). Process all blocks that hit this partition while processing the task
    // 2. If our input is ordered, we can use some sorting-based algorithm to split the partition. This way the probe side can do full streaming of the data
    template <class MemGuard>
    Status _split_partition(SerdeContext& context, SpillerReader* reader, SpilledPartition* partition,
                            SpilledPartition* left_partition, SpilledPartition* right_partition, MemGuard& guard);

    void _add_partition(SpilledPartitionPtr&& partition);
    void _remove_partition(const SpilledPartition* partition);

    size_t _partition_rows() {
        size_t total_rows = 0;
        for (const auto& [pid, partition] : _id_to_partitions) {
            total_rows += partition->num_rows;
        }
        return total_rows;
    }

    int32_t _min_level = 0;
    int32_t _max_level = 0;

    std::unique_ptr<MemTracker> _mem_tracker;

    // level to partition
    std::map<int, std::vector<SpilledPartitionPtr>> _level_to_partitions;

    std::unordered_map<int, SpilledPartition*> _id_to_partitions;

    std::vector<bool> _partition_set;

    int32_t _max_partition_id = 0;

    std::mutex _mutex;
};

} // namespace starrocks::spill