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
#include "exec/spill/data_stream.h"
#include "exec/spill/executor.h"
#include "exec/spill/input_stream.h"
#include "exec/spill/mem_table.h"
#include "exec/spill/options.h"
#include "exec/spill/partition.h"
#include "exec/spill/serde.h"
#include "exec/workgroup/scan_task_queue.h"
#include "fmt/format.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "util/race_detect.h"

namespace starrocks::spill {
class Spiller;

using FlushAllCallBack = std::function<Status()>;
using SpillHashColumn = UInt32Column;

class SpillerReader {
public:
    SpillerReader(Spiller* spiller) : _spiller(spiller) {}

    virtual ~SpillerReader() = default;

    void set_stream(std::shared_ptr<SpillInputStream> stream) {
        std::lock_guard guard(_mutex);
        _stream = std::move(stream);
    }

    template <class TaskExecutor = spill::IOTaskExecutor, class MemGuard>
    StatusOr<ChunkPtr> restore(RuntimeState* state, MemGuard&& guard);

    template <class TaskExecutor = spill::IOTaskExecutor, class MemGuard>
    Status trigger_restore(RuntimeState* state, MemGuard&& guard);

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

    virtual void prepare(RuntimeState* state) = 0;

    virtual Status set_flush_all_call_back(FlushAllCallBack callback) = 0;
    // acquire input stream
    // return empty if writer has multi-partitions
    virtual Status acquire_stream(std::shared_ptr<SpillInputStream>* stream) = 0;

    virtual Status acquire_stream(const SpillPartitionInfo* partition, std::shared_ptr<SpillInputStream>* stream) = 0;

    virtual void cancel() = 0;

    virtual void get_spill_partitions(std::vector<const SpillPartitionInfo*>* partitions) = 0;

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
    Status spill(RuntimeState* state, const ChunkPtr& chunk, MemGuard&& guard);

    template <class TaskExecutor, class MemGuard>
    Status flush(RuntimeState* state, MemGuard&& guard);

    void prepare(RuntimeState* state) override;

    void acquire_mem_table() {
        if (_mem_table == nullptr) {
            _mem_table = _acquire_mem_table_from_pool();
        }
    }

    const auto& mem_table() const { return _mem_table; }

    SpillOutputDataStreamPtr& output_stream() { return _output_stream; }
    void reset_output_stream() { _output_stream = nullptr; }

    BlockGroup& block_group() { return _block_group; }

    Status acquire_stream(std::shared_ptr<SpillInputStream>* stream) override;

    Status acquire_stream(const SpillPartitionInfo* partition, std::shared_ptr<SpillInputStream>* stream) override;

    void cancel() override {}

    void get_spill_partitions(std::vector<const SpillPartitionInfo*>* partitions) override {}

    Status yieldable_flush_task(workgroup::YieldContext& ctx, RuntimeState* state, const MemTablePtr& mem_table);

public:
    struct FlushContext : public SpillIOTaskContext {
        std::shared_ptr<SpillOutputDataStream> output;
    };
    using FlushContextPtr = std::shared_ptr<FlushContext>;

private:
    BlockGroup _block_group;
    SpillOutputDataStreamPtr _output_stream;
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

    std::string debug_string() {
        return fmt::format("[id={},bytes={},mem_size={},num_rows={},in_mem={},is_spliting={}]", partition_id, bytes,
                           mem_size, num_rows, in_mem, is_spliting);
    }

    bool is_spliting = false;
    std::unique_ptr<RawSpillerWriter> spill_writer;
};

class PartitionedSpillerWriter final : public SpillerWriter {
public:
    PartitionedSpillerWriter(Spiller* spiller, RuntimeState* state);

    void prepare(RuntimeState* state) override;

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
    Status spill(RuntimeState* state, const ChunkPtr& chunk, MemGuard&& guard);

    template <class TaskExecutor, class MemGuard>
    Status flush(RuntimeState* state, bool is_final_flush, MemGuard&& guard);

    template <class TaskExecutor, class MemGuard>
    Status flush_if_full(RuntimeState* state, MemGuard&& guard);

    void get_spill_partitions(std::vector<const SpillPartitionInfo*>* partitions) override;

    void reset_partition(const std::vector<const SpillPartitionInfo*>& partitions);

    void reset_partition(RuntimeState* state, size_t num_partitions);

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

    Status spill_partition(workgroup::YieldContext& ctx, SerdeContext& context, SpilledPartition* partition);

    int64_t mem_consumption() const { return _mem_tracker->consumption(); }

public:
    struct PartitionedFlushContext : public SpillIOTaskContext {
        // used in spill stage
        struct SpillStageContext {
            size_t processing_idx{};
        };
        // used in split stage
        struct SplitStageContext {
            SplitStageContext() = default;
            SplitStageContext(SplitStageContext&&) = default;
            SplitStageContext& operator=(SplitStageContext&&) = default;
            size_t spliting_idx{};
            SpilledPartitionPtr left;
            SpilledPartitionPtr right;
            std::unique_ptr<SpillerReader> reader;
            void reset_read_context() {
                left.reset();
                right.reset();
                reader.reset();
            }
        };

        PartitionedFlushContext() = default;
        PartitionedFlushContext(PartitionedFlushContext&&) = default;
        PartitionedFlushContext& operator=(PartitionedFlushContext&&) = default;

        SpillStageContext spill_stage_ctx;
        SplitStageContext split_stage_ctx;
    };
    using PartitionedFlushContextPtr = std::shared_ptr<PartitionedFlushContext>;

    Status yieldable_flush_task(workgroup::YieldContext& ctx,
                                const std::vector<SpilledPartition*>& splitting_partitions,
                                const std::vector<SpilledPartition*>& spilling_partitions);

private:
    void _init_with_partition_nums(RuntimeState* state, int num_partitions);
    // prepare and acquire mem_table for each partition in _id_to_partitions
    void _prepare_partitions(RuntimeState* state);

    Status _spill_input_partitions(workgroup::YieldContext& ctx, SerdeContext& context,
                                   const std::vector<SpilledPartition*>& spilling_partitions);

    Status _split_input_partitions(workgroup::YieldContext& ctx, SerdeContext& context,
                                   const std::vector<SpilledPartition*>& splitting_partitions);

    // split partition by hash
    // hash-based partitioning can have significant degradation in the case of heavily skewed data.
    // TODO:
    // 1. We can actually split partitions based on blocks (they all belong to the same partition, but
    // can be executed in splitting out more parallel tasks). Process all blocks that hit this partition while processing the task
    // 2. If our input is ordered, we can use some sorting-based algorithm to split the partition. This way the probe side can do full streaming of the data
    Status _split_partition(workgroup::YieldContext& ctx, SerdeContext& context, SpillerReader* reader,
                            SpilledPartition* partition, SpilledPartition* left_partition,
                            SpilledPartition* right_partition);

    void _add_partition(SpilledPartitionPtr&& partition);
    void _remove_partition(const SpilledPartition* partition);

    Status _choose_partitions_to_flush(bool is_final_flush, std::vector<SpilledPartition*>& partitions_need_spilt,
                                       std::vector<SpilledPartition*>& partitions_need_flush);

    size_t _partition_rows() {
        size_t total_rows = 0;
        for (const auto& [pid, partition] : _id_to_partitions) {
            total_rows += partition->num_rows;
        }
        return total_rows;
    }

    int32_t _min_level = 0;
    int32_t _max_level = 0;

    bool _need_final_flush = false;

    std::unique_ptr<MemTracker> _mem_tracker;

    // level to partition
    std::map<int, std::vector<SpilledPartitionPtr>> _level_to_partitions;

    std::unordered_map<int, SpilledPartition*> _id_to_partitions;

    std::vector<bool> _partition_set;

    int32_t _max_partition_id = 0;

    std::mutex _mutex;

    DECLARE_RACE_DETECTOR(detect_flush)
};

} // namespace starrocks::spill