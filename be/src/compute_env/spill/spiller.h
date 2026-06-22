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

#include "base/compression/block_compression.h"
#include "base/concurrency/blocking_queue.hpp"
#include "column/vectorized_fwd.h"
#include "common/runtime_profile.h"
#include "common/status.h"
#include "compute_env/spill/block_manager.h"
#include "compute_env/spill/common.h"
#include "compute_env/spill/input_stream.h"
#include "compute_env/spill/mem_table.h"
#include "compute_env/spill/options.h"
#include "compute_env/spill/partition.h"
#include "compute_env/spill/serde.h"
#include "compute_env/spill/spill_components.h"
#include "compute_env/spill/spill_metrics.h"
#include "compute_env/spill/spill_observable.h"
#include "compute_env/spill/spiller_factory.h"
#include "compute_env/spill/task_executor.h"
#include "fs/fs.h"
#include "gen_cpp/InternalService_types.h"
#include "gutil/macros.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state_fwd.h"

#define GET_METRICS(remote, metrics, key) (remote ? metrics.remote_##key : metrics.local_##key)

#define RETURN_TRUE_IF_SPILL_TASK_ERROR(spiller) \
    if (!(spiller)->task_status().ok()) {        \
        return true;                             \
    }

namespace starrocks::spill {

TQueryType::type spill_query_type(RuntimeState* state);

// some metrics for spill
struct SpillProcessMetrics {
public:
    SpillProcessMetrics() = default;
    SpillProcessMetrics(RuntimeProfile* profile, std::atomic_int64_t* total_spill_bytes);

    // For query statistics
    std::atomic_int64_t* total_spill_bytes;

    // time spent to append data into Spiller
    RuntimeProfile::Counter* append_data_timer = nullptr;
    // the number of rows appended to Spiller
    RuntimeProfile::Counter* spill_rows = nullptr;
    // time spent to flush data to disk
    RuntimeProfile::Counter* flush_timer = nullptr;
    // disk io time during flush
    RuntimeProfile::Counter* write_io_timer = nullptr;
    RuntimeProfile::Counter* local_write_io_timer = nullptr;
    RuntimeProfile::Counter* remote_write_io_timer = nullptr;
    // time spent to restore data from Spiller, which includes the time to try to get data from buffer and drive the next prefetch
    RuntimeProfile::Counter* restore_from_buffer_timer = nullptr;
    // disk io time during restore
    RuntimeProfile::Counter* read_io_timer = nullptr;
    RuntimeProfile::Counter* local_read_io_timer = nullptr;
    RuntimeProfile::Counter* remote_read_io_timer = nullptr;
    // the number of rows restored from Spiller
    RuntimeProfile::Counter* restore_rows = nullptr;
    // data bytes flushed to disk
    RuntimeProfile::Counter* flush_bytes = nullptr;
    RuntimeProfile::Counter* local_flush_bytes = nullptr;
    RuntimeProfile::Counter* remote_flush_bytes = nullptr;
    // data bytes restored from disk
    RuntimeProfile::Counter* restore_bytes = nullptr;
    RuntimeProfile::Counter* local_restore_bytes = nullptr;
    RuntimeProfile::Counter* remote_restore_bytes = nullptr;
    // time spent to serialize data before flush it to disk
    RuntimeProfile::Counter* serialize_timer = nullptr;
    // time spent to deserialize data after read it from disk
    RuntimeProfile::Counter* deserialize_timer = nullptr;
    // peak memory usage of mem table
    RuntimeProfile::HighWaterMarkCounter* mem_table_peak_memory_usage = nullptr;
    // peak memory usage of input stream
    RuntimeProfile::HighWaterMarkCounter* input_stream_peak_memory_usage = nullptr;
    // time spent to sort chunk before flush
    RuntimeProfile::Counter* sort_chunk_timer = nullptr;
    // time spent to materialize chunk by permutation
    RuntimeProfile::Counter* materialize_chunk_timer = nullptr;

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

    // the number of blocks created
    RuntimeProfile::Counter* block_count = nullptr;
    RuntimeProfile::Counter* local_block_count = nullptr;
    RuntimeProfile::Counter* remote_block_count = nullptr;

    // the number of read io count
    RuntimeProfile::Counter* read_io_count = nullptr;
    RuntimeProfile::Counter* local_read_io_count = nullptr;
    RuntimeProfile::Counter* remote_read_io_count = nullptr;

    // the number of compact table
    RuntimeProfile::Counter* compact_count = nullptr;
    RuntimeProfile::Counter* compact_block_count = nullptr;

    // flush/restore task count
    RuntimeProfile::Counter* flush_io_task_count = nullptr;
    RuntimeProfile::HighWaterMarkCounter* peak_flush_io_task_count = nullptr;
    RuntimeProfile::Counter* restore_io_task_count = nullptr;
    RuntimeProfile::HighWaterMarkCounter* peak_restore_io_task_count = nullptr;

    RuntimeProfile::Counter* mem_table_finalize_timer = nullptr;
    RuntimeProfile::Counter* flush_task_yield_times = nullptr;
    RuntimeProfile::Counter* restore_task_yield_times = nullptr;
    RuntimeProfile::Counter* skew_mem_table_count = nullptr;
    RuntimeProfile::LowWaterMarkCounter* skew_mem_table_skew_ratio = nullptr;
    RuntimeProfile::Counter* skew_mem_table_merge_timer = nullptr;
    RuntimeProfile::Counter* skew_mem_table_input_rows = nullptr;
    RuntimeProfile::Counter* skew_mem_table_output_rows = nullptr;
    RuntimeProfile::Counter* skew_mem_table_input_bytes = nullptr;
    RuntimeProfile::Counter* skew_mem_table_output_bytes = nullptr;

    // Server-level counters pre-resolved in the SpillProcessMetrics
    // constructor so the pointers travel with the value (operators may
    // reassign SpillProcessMetrics via Spiller::set_metrics after prepare,
    // which would lose pointers cached on Spiller itself). Stable for the
    // lifetime of SpillMetrics; callers use `global(is_remote)`
    // and skip updates when null (e.g. in unit tests without a registry).
    SpillMetrics::LabeledCounters* global_local = nullptr;
    SpillMetrics::LabeledCounters* global_remote = nullptr;

    SpillMetrics::LabeledCounters* global(bool is_remote) const { return is_remote ? global_remote : global_local; }
};

// major spill interfaces
class Spiller : public std::enable_shared_from_this<Spiller> {
public:
    Spiller(SpilledOptions opts, const std::shared_ptr<SpillerFactory>& factory)
            : _opts(std::move(opts)), _parent(factory) {}
    virtual ~Spiller() { TRACE_SPILL_LOG << "SPILLER:" << this << " call destructor"; }

    // some init work
    Status prepare(RuntimeState* state);

    void set_metrics(const SpillProcessMetrics& metrics) { _metrics = metrics; }

    const SpillProcessMetrics& metrics() { return _metrics; }

    // Notification layer. It lives on the Spiller and survives reset_state, so the observer lists outlive
    // reallocation of the writer/reader.
    SpillEventObservable& observable() { return _observable; }

    void notify_sink_observers() { _observable.notify_sink_observers(); }
    void notify_source_observers() { _observable.notify_source_observers(); }

    // The single place that owns the completion order for spiller IO:
    //   publish -> decrement the in-flight count -> notify.
    // The notify runs after both the predicates (published by publish_fn) and the count are visible, so one
    // notify serves every sleeper: the OUTPUT_FULL/INPUT_EMPTY predicate sleepers re-check predicates that
    // are already open, and a PENDING_FINISH driver gating on the count sees it already at its new value.
    // The caller must keep the notified observers alive across the call: a task body calls this through
    // defer_complete_io (the notify runs inside the task's query-lifetime guard window), and the
    // submit-failure compensation calls it directly on the pipeline thread, where the executing driver
    // itself keeps the query alive. Every completion path goes through here.
    template <class PublishFn>
    void complete_io(PublishFn&& publish_fn, bool wake_sink) {
        std::forward<PublishFn>(publish_fn)();
        decrease_in_flight_io();
        if (wake_sink) {
            notify_sink_observers();
        }
        notify_source_observers();
    }

    // RAII form of complete_io for IO task bodies; see IOCompletion below. The guard must already be
    // scoped_begin'd; the returned object runs the completion and then guard.scoped_end() at scope exit.
    template <class PublishFn, class Guard>
    [[nodiscard]] auto defer_complete_io(PublishFn publish_fn, bool wake_sink, const Guard& guard);

    // Counts in-flight IO across writer (flush) and reader (restore) tasks. It is in addition to the
    // per-writer/reader counters, which stay as lifetime gates for transient readers. It is incremented
    // before submit at every choke point, and decremented in the same defer as the per-task counter.
    void increase_in_flight_io() { _in_flight_io.fetch_add(1, std::memory_order_acq_rel); }
    void decrease_in_flight_io() { _in_flight_io.fetch_sub(1, std::memory_order_acq_rel); }

    bool has_running_io_tasks() const { return _in_flight_io.load(std::memory_order_acquire) > 0; }

    // set partitions for spiller only works when spiller has partitioned spill writer
    void set_partition(const std::vector<const SpillPartitionInfo*>& parititons);
    // init partition by `num_partitions`
    void set_partition(RuntimeState* state, size_t num_partitions);

    // no thread-safe
    // TaskExecutor: Executor for runing io tasks
    // MemGuard: interface for record/update memory usage in io tasks
    template <class TaskExecutor = spill::IOTaskExecutor, class MemGuard>
    Status spill(RuntimeState* state, const ChunkPtr& chunk, MemGuard&& guard);

    template <class TaskExecutor = spill::IOTaskExecutor, class Processer, class MemGuard>
    Status partitioned_spill(RuntimeState* state, const ChunkPtr& chunk, SpillHashColumn* hash_column,
                             Processer&& processer, MemGuard&& guard);

    // restore chunk from spilled chunks
    template <class TaskExecutor = spill::IOTaskExecutor, class MemGuard>
    StatusOr<ChunkPtr> restore(RuntimeState* state, MemGuard&& guard);

    // trigger a restore task
    template <class TaskExecutor = spill::IOTaskExecutor, class MemGuard>
    Status trigger_restore(RuntimeState* state, MemGuard&& guard);

    bool is_full() { return _writer->is_full(); }

    bool has_pending_data() { return _writer->has_pending_data(); }

    // all data has been sent
    // prepared for as read
    template <class TaskExecutor = spill::IOTaskExecutor, class MemGuard>
    Status flush(RuntimeState* state, MemGuard&& guard);
    template <class TaskExecutor = spill::IOTaskExecutor, class MemGuard>
    Status set_flush_all_call_back(const FlushAllCallBack& callback, RuntimeState* state, const MemGuard& guard) {
        auto flush_call_back = [this, callback, state, guard]() {
            auto defer = DeferOp([&]() { guard.scoped_end(); });
            RETURN_IF(!guard.scoped_begin(), Status::Cancelled("cancelled"));
            RETURN_IF_ERROR(callback());
            if (!_is_cancel && spilled()) {
                RETURN_IF_ERROR(_acquire_input_stream(state));
                RETURN_IF_ERROR(trigger_restore<TaskExecutor>(state, guard));
            }
            return Status::OK();
        };
        return _writer->set_flush_all_call_back(flush_call_back);
    }

    bool has_output_data() { return is_cancel() || _reader->has_output_data(); }

    size_t spilled_append_rows() const { return _spilled_append_rows; }

    size_t& mutable_spilled_append_rows() { return _spilled_append_rows; }

    size_t restore_read_rows() const { return _restore_read_rows; }

    bool spilled() const { return spilled_append_rows() > 0; }

    bool restore_finished() const { return _reader->restore_finished(); }

    bool is_cancel() const { return _is_cancel; }

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
    const std::shared_ptr<SpillerReader>& reader() { return _reader; }

    const std::shared_ptr<spill::Serde>& serde() { return _serde; }
    BlockManager* block_manager() { return _block_manager; }
    const ChunkBuilder& chunk_builder() { return _chunk_builder; }

    // Shared flag used to bump the global query_spill_trigger_total
    // counter at most once per Spiller instance. Lives on Spiller itself
    // (not SpillProcessMetrics) because std::atomic_bool would break
    // SpillProcessMetrics copy-assignment in set_metrics().
    std::atomic_bool& global_spill_triggered() { return _global_spill_triggered; }

    Status reset_state(RuntimeState* state);

    size_t max_sorted_block_cnt() const { return _max_sorted_block_cnt; }

    void release() {
        _writer.reset();
        _reader.reset();
    }

private:
    Status _acquire_input_stream(RuntimeState* state);

    Status _decrease_running_flush_tasks();

    void _init_max_block_nums();

private:
    SpillProcessMetrics _metrics;
    SpilledOptions _opts;
    std::weak_ptr<SpillerFactory> _parent;

    std::unique_ptr<SpillerWriter> _writer;
    std::shared_ptr<SpillerReader> _reader;

    std::mutex _mutex;

    Status _spilled_task_status;
    ChunkBuilder _chunk_builder;

    // stats
    size_t _spilled_append_rows{};
    size_t _restore_read_rows{};

    std::shared_ptr<spill::Serde> _serde;
    spill::BlockManager* _block_manager = nullptr;
    size_t _max_sorted_block_cnt = 0;
    std::atomic_bool _is_cancel = false;
    std::atomic_bool _global_spill_triggered{false};

    SpillEventObservable _observable;
    // Count of in-flight flush/restore IO tasks. It survives reset_state (it lives on the Spiller, not on
    // the writer/reader), so it can gate teardown and re-prepare against tasks that captured raw
    // writer/reader pointers.
    std::atomic<int64_t> _in_flight_io{0};
};

// RAII completion for an IO task body, returned by Spiller::defer_complete_io. At scope exit it runs the
// completion (complete_io: publish -> decrement -> notify) and then the guard's scoped_end, in that order.
// The order matters for correctness, not for style: scoped_end releases the query-lifetime pin the guard
// took in scoped_begin, and that pin is what keeps the notified observers alive -- if scoped_end ran first,
// the notify could dereference freed observers. Do not reorder. cancel_for_yield() drops the completion AND
// marks the yield context unfinished in one call (on a yield-resubmit the task lives on, keeps its
// in-flight increment, and re-enters scoped_begin on its next run); scoped_end still runs to balance this
// invocation's begin. The two cancels are coupled on purpose: cancelling the completion without cancelling
// the yield defer would let the executor drop the task with the in-flight count still held.
template <class PublishFn, class Guard>
class IOCompletion {
public:
    IOCompletion(Spiller* spiller, PublishFn publish_fn, bool wake_sink, const Guard& guard)
            : _spiller(spiller), _publish_fn(std::move(publish_fn)), _wake_sink(wake_sink), _guard(guard) {}
    ~IOCompletion() noexcept {
        if (!_cancelled) {
            _spiller->complete_io(_publish_fn, _wake_sink);
        }
        _guard.scoped_end();
    }
    template <class YieldDefer>
    void cancel_for_yield(YieldDefer& yield_defer) {
        _cancelled = true;
        yield_defer.cancel();
    }
    DISALLOW_COPY_AND_MOVE(IOCompletion);

private:
    Spiller* _spiller;
    PublishFn _publish_fn;
    const bool _wake_sink;
    const Guard& _guard;
    bool _cancelled = false;
};

template <class PublishFn, class Guard>
[[nodiscard]] auto Spiller::defer_complete_io(PublishFn publish_fn, bool wake_sink, const Guard& guard) {
    // Returned as a prvalue: C++17 guaranteed elision constructs it in the caller's variable, so the
    // deleted copy/move constructors are never used.
    return IOCompletion<PublishFn, Guard>(this, std::move(publish_fn), wake_sink, guard);
}

} // namespace starrocks::spill
