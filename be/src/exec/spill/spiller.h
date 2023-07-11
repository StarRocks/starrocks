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

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exec/spill/block_manager.h"
#include "exec/spill/common.h"
#include "exec/spill/input_stream.h"
#include "exec/spill/mem_table.h"
#include "exec/spill/options.h"
#include "exec/spill/partition.h"
#include "exec/spill/serde.h"
#include "exec/spill/spill_components.h"
#include "exec/spill/spiller_factory.h"
#include "fs/fs.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "util/blocking_queue.hpp"
#include "util/compression/block_compression.h"
#include "util/runtime_profile.h"

namespace starrocks::spill {

// some metrics for spill
struct SpillProcessMetrics {
    SpillProcessMetrics() = default;
    SpillProcessMetrics(RuntimeProfile* profile);

    std::shared_ptr<RuntimeProfile> _spiller_metrics;

    // time spent to append data into Spiller
    RuntimeProfile::Counter* append_data_timer = nullptr;
    // the number of rows appended to Spiller
    RuntimeProfile::Counter* spill_rows = nullptr;
    // time spent to flush data to disk
    RuntimeProfile::Counter* flush_timer = nullptr;
    // disk io time during flush
    RuntimeProfile::Counter* write_io_timer = nullptr;
    // time spent to restore data from Spiller, which includes the time to try to get data from buffer and drive the next prefetch
    RuntimeProfile::Counter* restore_from_buffer_timer = nullptr;
    // disk io time during restore
    RuntimeProfile::Counter* read_io_timer = nullptr;
    // the number of rows restored from Spiller
    RuntimeProfile::Counter* restore_rows = nullptr;
    // data bytes flushed to disk
    RuntimeProfile::Counter* flush_bytes = nullptr;
    // data bytes restored from disk
    RuntimeProfile::Counter* restore_bytes = nullptr;
    // time spent to serialize data before flush it to disk
    RuntimeProfile::Counter* serialize_timer = nullptr;
    // time spent to deserialize data after read it from disk
    RuntimeProfile::Counter* deserialize_timer = nullptr;
    // peak memory usage of mem table
    RuntimeProfile::HighWaterMarkCounter* mem_table_peak_memory_usage = nullptr;
    // peak memory usage of input stream
    RuntimeProfile::HighWaterMarkCounter* input_stream_peak_memory_usage = nullptr;

    // time spent to shuffle data to the corresponding partition, only used in join operator
    RuntimeProfile::Counter* shuffle_timer = nullptr;
    // time spent to split partitions, only used in join operator
    RuntimeProfile::Counter* split_partition_timer = nullptr;
    // data bytes restored from mem table in memory, only used in join operator
    RuntimeProfile::Counter* restore_from_mem_table_bytes = nullptr;
    // the number of rows restored from mem table in memory, only used in join operator
    RuntimeProfile::Counter* restore_from_mem_table_rows = nullptr;
    // peak memory usage of partition writer, only used in join operator
    RuntimeProfile::HighWaterMarkCounter* partition_writer_peak_memory_usage = nullptr;
};

// major spill interfaces
class Spiller {
public:
    Spiller(SpilledOptions opts, const std::shared_ptr<SpillerFactory>& factory)
            : _opts(std::move(opts)), _parent(factory) {}
    virtual ~Spiller() { TRACE_SPILL_LOG << "SPILLER:" << this << " call destructor"; }

    // some init work
    Status prepare(RuntimeState* state);

    void set_metrics(const SpillProcessMetrics& metrics) { _metrics = metrics; }

    const SpillProcessMetrics& metrics() { return _metrics; }

    // set partitions for spiller only works when spiller has partitioned spill writer
    Status set_partition(const std::vector<const SpillPartitionInfo*>& parititons);
    // init partition by `num_partitions`
    Status set_partition(RuntimeState* state, size_t num_partitions);

    // no thread-safe
    // TaskExecutor: Executor for runing io tasks
    // MemGuard: interface for record/update memory usage in io tasks
    template <class TaskExecutor, class MemGuard>
    Status spill(RuntimeState* state, const ChunkPtr& chunk, TaskExecutor&& executor, MemGuard&& guard);

    template <class Processer, class TaskExecutor, class MemGuard>
    Status partitioned_spill(RuntimeState* state, const ChunkPtr& chunk, SpillHashColumn* hash_column,
                             Processer&& processer, TaskExecutor&& executor, MemGuard&& guard);

    // restore chunk from spilled chunks
    template <class TaskExecutor, class MemGuard>
    StatusOr<ChunkPtr> restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard);

    // trigger a restore task
    template <class TaskExecutor, class MemGuard>
    Status trigger_restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard);

    bool is_full() { return _writer->is_full(); }

    bool has_pending_data() { return _writer->has_pending_data(); }

    // all data has been sent
    // prepared for as read
    template <class TaskExecutor, class MemGuard>
    Status flush(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard);
    template <class MemGuard>
    Status set_flush_all_call_back(const FlushAllCallBack& callback, RuntimeState* state, IOTaskExecutor& executor,
                                   const MemGuard& guard) {
        auto flush_call_back = [this, callback, state, &executor, guard]() {
<<<<<<< HEAD
=======
            auto defer = DeferOp([&]() { guard.scoped_end(); });
            RETURN_IF(!guard.scoped_begin(), Status::Cancelled("cancelled"));
>>>>>>> a7689e4c23 ([BugFix] Fixed the problem of inaccurate memory statistics with auto spill turned on (#26890))
            RETURN_IF_ERROR(callback());
            if (!_is_cancel && spilled()) {
                RETURN_IF_ERROR(_acquire_input_stream(state));
                RETURN_IF_ERROR(trigger_restore(state, executor, guard));
            }
            return Status::OK();
        };
        return _writer->set_flush_all_call_back(flush_call_back);
    }

    bool has_output_data() { return _reader->has_output_data(); }

    size_t spilled_append_rows() const { return _spilled_append_rows; }

    size_t restore_read_rows() const { return _restore_read_rows; }

    bool spilled() const { return spilled_append_rows() > 0; }

    bool restore_finished() const { return _reader->restore_finished(); }

    bool is_cancel() { return _is_cancel; }

    void cancel() {
        _is_cancel = true;
        _writer->cancel();
    }

    void set_finished() { cancel(); }

    const auto& options() const { return _opts; }

    void update_spilled_task_status(Status&& st);

    Status task_status() {
        std::lock_guard l(_mutex);
        return _spilled_task_status;
    }

    void get_all_partitions(std::vector<const SpillPartitionInfo*>* parititons) {
        _writer->get_spill_partitions(parititons);
    }

    std::vector<std::shared_ptr<SpillerReader>> get_partition_spill_readers(
            const std::vector<const SpillPartitionInfo*>& parititons);

    const std::unique_ptr<SpillerWriter>& writer() { return _writer; }

    const std::shared_ptr<spill::Serde>& serde() { return _serde; }
    BlockManager* block_manager() { return _block_manager; }
    const ChunkBuilder& chunk_builder() { return _chunk_builder; }

private:
    Status _acquire_input_stream(RuntimeState* state);

    Status _decrease_running_flush_tasks();

private:
    SpillProcessMetrics _metrics;
    SpilledOptions _opts;
    std::weak_ptr<SpillerFactory> _parent;

    std::unique_ptr<SpillerWriter> _writer;
    std::unique_ptr<SpillerReader> _reader;

    std::mutex _mutex;

    Status _spilled_task_status;
    ChunkBuilder _chunk_builder;

    // stats
    size_t _spilled_append_rows{};
    size_t _restore_read_rows{};

    std::shared_ptr<spill::Serde> _serde;
    spill::BlockManager* _block_manager = nullptr;
    std::shared_ptr<spill::BlockGroup> _block_group;

    std::atomic_bool _is_cancel = false;
};

} // namespace starrocks::spill