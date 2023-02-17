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
#include "exec/spill/common.h"
#include "exec/spill/mem_table.h"
#include "exec/spill/spilled_stream.h"
#include "exec/spill/spiller_factory.h"
#include "exec/spill/spiller_path_provider.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "util/blocking_queue.hpp"
#include "util/runtime_profile.h"

namespace starrocks {
enum class SpillFormaterType { NONE, SPILL_BY_COLUMN };

using ChunkBuilder = std::function<ChunkUniquePtr()>;

struct SpilledOptions {
    SpilledOptions() : is_unordered(true), sort_exprs(nullptr), sort_desc(nullptr) {}

    SpilledOptions(SortExecExprs* sort_exprs_, const SortDescs* sort_desc_)
            : is_unordered(false), sort_exprs(sort_exprs_), sort_desc(sort_desc_) {}

    bool is_unordered;
    const SortExecExprs* sort_exprs;
    const SortDescs* sort_desc;

    size_t mem_table_pool_size{};
    size_t spill_file_size{};
    SpillFormaterType spill_type{};
    SpillPathProviderFactory path_provider_factory;
    ChunkBuilder chunk_builder;
};

struct SpillProcessMetrics {
    SpillProcessMetrics() = default;
    SpillProcessMetrics(RuntimeProfile* profile);

    RuntimeProfile::Counter* spill_timer = nullptr;
    RuntimeProfile::Counter* spill_rows = nullptr;
    RuntimeProfile::Counter* flush_timer = nullptr;
    RuntimeProfile::Counter* restore_timer = nullptr;
    RuntimeProfile::Counter* write_io_timer = nullptr;
    RuntimeProfile::Counter* restore_rows = nullptr;
};

struct SpillFormatContext {
    std::string io_buffer;
};

enum class SpillStrategy {
    NO_SPILL,
    SPILL_ALL,
};

// thread safe
class SpillFormater {
public:
    virtual ~SpillFormater() = default;
    virtual Status spill_as_fmt(SpillFormatContext& context, std::unique_ptr<WritableFile>& writable,
                                const ChunkPtr& chunk) const noexcept = 0;
    virtual StatusOr<ChunkUniquePtr> restore_from_fmt(SpillFormatContext& context,
                                                      std::unique_ptr<RawInputStreamWrapper>& readable) const = 0;
    virtual Status flush(std::unique_ptr<WritableFile>& writable) const = 0;
    static StatusOr<std::unique_ptr<SpillFormater>> create(SpillFormaterType type, ChunkBuilder chunk_builder);
};

class Spiller {
public:
    using FlushAllCallBack = std::function<Status()>;
    Spiller(SpilledOptions opts, const std::shared_ptr<SpillerFactory>& factory)
            : _opts(std::move(opts)), _parent(factory) {}
    virtual ~Spiller() { TRACE_SPILL_LOG << "SPILLER:" << this << " call destructor"; }

    // some init work
    Status prepare(RuntimeState* state);

    void set_metrics(const SpillProcessMetrics& metrics) { _metrics = metrics; }

    const SpillProcessMetrics& metrics() { return _metrics; }

    // no thread-safe
    // TaskExecutor: Executor for runing io tasks
    // should provide Status TaskExecutor::submit(Runnable)
    //
    // MemGuard: interface for record/update memory usage in io tasks
    template <class TaskExecutor, class MemGuard>
    Status spill(RuntimeState* state, ChunkPtr chunk, TaskExecutor&& executor, MemGuard&& guard);

    // restore chunk
    template <class TaskExecutor, class MemGuard>
    StatusOr<ChunkPtr> restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard);

    template <class TaskExecutor, class MemGuard>
    Status trigger_restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard);

    bool is_full() {
        std::lock_guard guard(_mutex);
        return _mem_table_pool.empty() && _mem_table == nullptr;
    }

    bool has_pending_data() {
        std::lock_guard guard(_mutex);
        return _mem_table_pool.size() != _opts.mem_table_pool_size;
    }

    int64_t running_flush_tasks() { return _running_flush_tasks; }

    int32_t total_restore_tasks() const { return 0; }

    // all data has been sent
    // prepared for as read
    template <class TaskExecutor, class MemGuard>
    Status flush(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard);

    template <class TaskExecutor, class MemGuard>
    Status set_flush_all_call_back(FlushAllCallBack callback, RuntimeState* state, TaskExecutor&& executor,
                                   MemGuard guard) {
        _running_flush_tasks++;
        _flush_all_callback = std::move(callback);
        if (spilled()) {
            _inner_flush_all_callback = [state, &executor, guard, this]() {
                return trigger_restore(state, executor, guard);
            };
        }
        return _decrease_running_flush_tasks();
    }

    bool has_output_data() { return _current_stream && _current_stream->is_ready(); }

    size_t spilled_append_rows() { return _spilled_append_rows; }

    size_t restore_read_rows() { return _restore_read_rows; }

    bool spilled() { return spilled_append_rows() > 0; }

    bool restore_finished() { return _running_restore_tasks == 0; }

    void cancel() {
        std::lock_guard guard(_mutex);
        if (_mem_table != nullptr) {
            _mem_table_pool.push(std::move(_mem_table));
        }
    }

private:
    // open stage
    // should be called in executor threads
    Status _open(RuntimeState* state);

    Status _run_flush_task(RuntimeState* state, const MemTablePtr& writable);

    // should running in executor threads
    // flush and close
    Status _flush_and_closed(std::unique_ptr<WritableFile>& writable);

    void _update_spilled_task_status(Status&& st);

    MemTablePtr _acquire_mem_table_from_pool() {
        std::lock_guard guard(_mutex);
        if (_mem_table_pool.empty()) {
            return nullptr;
        }
        auto res = std::move(_mem_table_pool.front());
        _mem_table_pool.pop();
        return res;
    }

    StatusOr<std::shared_ptr<SpilledInputStream>> _acquire_input_stream(RuntimeState* state);

    Status _decrease_running_flush_tasks();

private:
    SpilledOptions _opts;
    SpillProcessMetrics _metrics;
    std::weak_ptr<SpillerFactory> _parent;

    bool _has_opened = false;
    std::shared_ptr<SpillerPathProvider> _path_provider;

    std::mutex _mutex;
    std::queue<MemTablePtr> _mem_table_pool;
    MemTablePtr _mem_table;

    FlushAllCallBack _flush_all_callback;
    FlushAllCallBack _inner_flush_all_callback;

    std::unique_ptr<SpillFormater> _spill_fmt;
    std::shared_ptr<SpilledFileGroup> _file_group;
    Status _spilled_task_status;

    // std::atomic_int32_t _total_restore_tasks{};
    std::shared_ptr<SpilledInputStream> _current_stream;
    std::queue<SpillRestoreTaskPtr> _restore_tasks;

    // stats
    std::atomic_uint64_t _total_restore_tasks{};
    std::atomic_uint64_t _running_restore_tasks{};
    std::atomic_uint64_t _finished_restore_tasks{};

    std::atomic_uint64_t _running_flush_tasks{};

    size_t _spilled_append_rows{};
    size_t _restore_read_rows{};
    SpillFormatContext _spill_read_ctx;
};
} // namespace starrocks