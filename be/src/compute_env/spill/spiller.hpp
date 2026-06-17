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

#include <algorithm>
#include <cstdint>
#include <mutex>
#include <utility>

#include "base/failpoint/fail_point.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "common/config_scan_io_fwd.h"
#include "common/logging.h"
#include "common/runtime_profile.h"
#include "common/status.h"
#include "compute_env/spill/common.h"
#include "compute_env/spill/input_stream.h"
#include "compute_env/spill/mem_tracker_guard.h"
#include "compute_env/spill/restore_task.h"
#include "compute_env/spill/runtime_context.h"
#include "compute_env/spill/serde.h"
#include "compute_env/spill/spill_components.h"
#include "compute_env/spill/spiller.h"
#include "compute_env/spill/task_executor.h"
#include "compute_env/workgroup/work_group_fwd.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/runtime_state_fwd.h"

namespace starrocks::spill {
DECLARE_FAIL_POINT(spill_restore_sleep);
DECLARE_FAIL_POINT(spill_restore_error);
DECLARE_FAIL_POINT(spill_submit_error);
DECLARE_FAIL_POINT(spill_flush_block);
DECLARE_FAIL_POINT(spill_restore_block);

#ifdef FIU_ENABLE
// Test-only rendezvous points. A flush/restore IO task that hits the matching fail point waits at
// arrive_A() until a test thread calls arrive_B(). Used to hold the writer-full / no-restored-data window
// open deterministically.
failpoint::OneToAnyBarrier& spill_flush_block_barrier();
failpoint::OneToAnyBarrier& spill_restore_block_barrier();
#endif

// Compile-time proof that an IO task body decided the fate of its completion. The task lambdas declare
// CompletionDecided as their return type, so every return path must construct one: either from the live
// IOCompletion (the completion runs at scope exit, or was deliberately cancelled for a yield-resubmit),
// or via abandoned() on the scoped_begin-false path. scoped_begin() fails only when the spiller or the
// query context is already destroyed; nobody reads the spiller's counters then, and touching the spiller
// from the task would be a use-after-free, so that path must leave everything untouched. A new return
// path that presents neither does not compile. The executor never sees this type:
// ScanTask::WorkFunction is std::function<void(YieldContext&)>, which discards the lambda's return value.
class CompletionDecided {
public:
    template <class PublishFn, class Guard>
    explicit CompletionDecided(const IOCompletion<PublishFn, Guard>&) {}
    static CompletionDecided abandoned() { return {}; }

private:
    CompletionDecided() = default;
};

template <class TaskExecutor, class MemGuard>
Status Spiller::spill(RuntimeState* state, const ChunkPtr& chunk, MemGuard&& guard) {
    SCOPED_TIMER(_metrics.append_data_timer);
    RETURN_IF_ERROR(task_status());
    DCHECK(!chunk->is_empty());
    DCHECK(!is_full());

    COUNTER_UPDATE(_metrics.spill_rows, chunk->num_rows());
    _spilled_append_rows += chunk->num_rows();
    TRACE_SPILL_LOG << "spilled rows:" << chunk->num_rows() << ",cumulative:" << _spilled_append_rows
                    << ",spiller:" << this;

    if (_chunk_builder.chunk_schema()->empty()) {
        if (!_opts.splittable && _opts.init_partition_nums > 0) {
            // For splittable spiller, we need to set the schema before spilling.
            // This is because the partitioned spiller needs to know the schema of the chunk.
            ChunkPtr new_chunk = chunk->clone_empty(0);
            new_chunk->remove_column_by_slot_id(Chunk::HASH_AGG_SPILL_HASH_SLOT_ID);
            _chunk_builder.chunk_schema()->set_schema(new_chunk);
        } else {
            // For non-splittable spiller, we can set the schema after spilling.
            // This is because the raw spiller does not need to know the schema of the chunk.
            _chunk_builder.chunk_schema()->set_schema(chunk);
        }

        RETURN_IF_ERROR(_serde->prepare());
        _init_max_block_nums();
    }

    if (_opts.init_partition_nums > 0) {
        return _writer->as<PartitionedSpillerWriter*>()->spill<TaskExecutor>(state, chunk, guard);
    } else {
        return _writer->as<RawSpillerWriter*>()->spill<TaskExecutor>(state, chunk, guard);
    }
}

template <class TaskExecutor, class Processer, class MemGuard>
Status Spiller::partitioned_spill(RuntimeState* state, const ChunkPtr& chunk, SpillHashColumn* hash_column,
                                  Processer&& processer, MemGuard&& guard) {
    SCOPED_TIMER(_metrics.append_data_timer);
    RETURN_IF_ERROR(task_status());
    DCHECK(!chunk->is_empty());
    COUNTER_UPDATE(_metrics.spill_rows, chunk->num_rows());
    DCHECK_GT(_opts.init_partition_nums, 0);

    if (_chunk_builder.chunk_schema()->empty()) {
        _chunk_builder.chunk_schema()->set_schema(chunk);
        RETURN_IF_ERROR(_serde->prepare());
        _init_max_block_nums();
    }

    std::vector<uint32_t> indexs;
    auto writer = _writer->as<PartitionedSpillerWriter*>();
    {
        SCOPED_TIMER(_metrics.shuffle_timer);
        writer->shuffle(indexs, hash_column);
        writer->process_partition_data(chunk, indexs, std::forward<Processer>(processer));
    }
    COUNTER_SET(_metrics.partition_writer_peak_memory_usage, writer->mem_consumption());
    RETURN_IF_ERROR(writer->flush_if_full<TaskExecutor>(state, guard));
    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
Status Spiller::flush(RuntimeState* state, MemGuard&& guard) {
    RETURN_IF_ERROR(task_status());
    if (_opts.init_partition_nums > 0) {
        return _writer->as<PartitionedSpillerWriter*>()->flush<TaskExecutor>(state, true, guard);
    } else {
        return _writer->as<RawSpillerWriter*>()->flush<TaskExecutor>(state, guard);
    }
}

template <class TaskExecutor, class MemGuard>
StatusOr<ChunkPtr> Spiller::restore(RuntimeState* state, MemGuard&& guard) {
    RETURN_IF_ERROR(task_status());
    if (is_cancel()) {
        return Status::Cancelled("cancelled by pipeline");
    }

    ASSIGN_OR_RETURN(auto chunk, _reader->restore<TaskExecutor>(state, guard));
    chunk->check_or_die();
    _restore_read_rows += chunk->num_rows();

    RETURN_IF_ERROR(trigger_restore<TaskExecutor>(state, std::forward<MemGuard>(guard)));
    return chunk;
}

template <class TaskExecutor, class MemGuard>
Status Spiller::trigger_restore(RuntimeState* state, MemGuard&& guard) {
    return _reader->trigger_restore<TaskExecutor>(state, guard);
}

template <class TaskExecutor, class MemGuard>
Status RawSpillerWriter::spill(RuntimeState* state, const ChunkPtr& chunk, MemGuard&& guard) {
    if (_mem_table == nullptr) {
        _mem_table = _acquire_mem_table_from_pool();
        DCHECK(_mem_table != nullptr);
    }

    RETURN_IF_ERROR(_mem_table->append(chunk));

    if (_mem_table->is_full()) {
        return flush<TaskExecutor>(state, std::forward<MemGuard>(guard));
    }

    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
Status RawSpillerWriter::flush(RuntimeState* state, MemGuard&& guard) {
    MemTablePtr captured_mem_table;
    {
        std::lock_guard l(_mutex);
        captured_mem_table = std::move(_mem_table);
    }
    auto defer = DeferOp([&]() {
        if (captured_mem_table) {
            std::lock_guard _(_mutex);
            _mem_table_pool.emplace(std::move(captured_mem_table));
        }
    });

    if (captured_mem_table == nullptr) {
        return Status::OK();
    }
    RETURN_IF_ERROR(captured_mem_table->done());

    _running_flush_tasks++;
    // Keep a shared_ptr copy so the mem-table can be returned to the pool if submit fails. It is moved into
    // the task lambda below, which never runs on a failed submit; without this copy it would leak from the
    // pool forever.
    MemTablePtr compensation_mem_table = captured_mem_table;
    auto task = [this, state, guard = guard, mem_table = std::move(captured_mem_table),
                 trace = TraceInfo(state)](auto& yield_ctx) -> CompletionDecided {
        SCOPED_SET_TRACE_INFO({}, trace.query_id, trace.fragment_id);
        auto yield_defer = yield_ctx.defer_finished();
        if (!guard.scoped_begin()) {
            return CompletionDecided::abandoned();
        }
        // Flush completion, RAII: at scope exit the writer is freed (the mem-table returns to the pool,
        // _decrease_running_flush_tasks publishes is_full()) and both lists are woken -- the freed writer
        // matters to the OUTPUT_FULL sink sleeper and to the INPUT_EMPTY source-side pump. Cancelled on a
        // yield-resubmit (the task lives on and keeps its in-flight increment), so the completion fires
        // exactly once, when the task truly finishes.
        auto guarded = _spiller->defer_complete_io(
                [&]() {
                    // Hold the flush in-flight at a rendezvous so a test thread can
                    // observe the writer-full window before the mem-table is returned.
                    FAIL_POINT_TRIGGER_EXECUTE(spill_flush_block, { spill_flush_block_barrier().arrive_A(); });
                    {
                        std::lock_guard _(_mutex);
                        _mem_table_pool.emplace(mem_table);
                    }
                    _spiller->update_spilled_task_status(_decrease_running_flush_tasks());
                },
                /*wake_sink=*/true, guard);
        SCOPED_TIMER(_spiller->metrics().flush_timer);
        DCHECK_GT(_running_flush_tasks, 0);
        DCHECK(has_pending_data());
        //
        if (!yield_ctx.task_context_data.has_value()) {
            yield_ctx.task_context_data =
                    SpillIOTaskContextPtr(std::make_shared<FlushContext>(_spiller->shared_from_this()));
        }

        if (_spiller->is_cancel() || !_spiller->task_status().ok()) {
            return CompletionDecided(guarded);
        }

        yield_ctx.time_spent_ns = 0;
        yield_ctx.need_yield = false;

        _spiller->update_spilled_task_status(yieldable_flush_task(yield_ctx, state, mem_table));
        if (yield_ctx.need_yield && !yield_ctx.is_finished()) {
            COUNTER_UPDATE(_spiller->metrics().flush_task_yield_times, 1);
            guarded.cancel_for_yield(yield_defer);
        }

        return CompletionDecided(guarded);
    };

    auto yield_func = [&](workgroup::ScanTask&& task) { TaskExecutor::force_submit(std::move(task)); };
    auto query_type = spill_query_type(state);
    auto io_task = workgroup::ScanTask(_spiller->options().wg, std::move(task), std::move(yield_func));
    io_task.set_query_type(query_type);
    // Increment the in-flight count before submit: incrementing after submit would race a task that
    // completes (and decrements) before the increment lands.
    _spiller->increase_in_flight_io();
    // The fail point must fire before the real submit: injecting after it would run the compensation
    // concurrently with a live task and complete it twice.
    Status submit_st;
    FAIL_POINT_TRIGGER_EXECUTE(spill_submit_error, { submit_st = Status::InternalError("inject spill_submit_error"); });
    if (submit_st.ok()) {
        submit_st = TaskExecutor::submit(std::move(io_task));
    }
    if (!submit_st.ok()) {
        // Full compensation: the task lambda never runs, so none of its defers fire, but every flush
        // attempt must still produce exactly one completion (a failed submit is itself an event). Return
        // the mem-table to the pool, then run the standard completion.
        _spiller->complete_io(
                [&]() {
                    {
                        std::lock_guard _(_mutex);
                        _mem_table_pool.emplace(std::move(compensation_mem_table));
                    }
                    _spiller->update_spilled_task_status(_decrease_running_flush_tasks());
                    _spiller->update_spilled_task_status(std::move(submit_st));
                },
                /*wake_sink=*/true);
        return _spiller->task_status();
    }
    COUNTER_UPDATE(_spiller->metrics().flush_io_task_count, 1);
    COUNTER_SET(_spiller->metrics().peak_flush_io_task_count, _running_flush_tasks.load());
    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
StatusOr<ChunkPtr> SpillerReader::restore(RuntimeState* state, MemGuard&& guard) {
    SCOPED_TIMER(_spiller->metrics().restore_from_buffer_timer);
    workgroup::YieldContext mock_ctx;
    ASSIGN_OR_RETURN(auto chunk, _stream->get_next(mock_ctx, _spill_read_ctx));
    RETURN_IF_ERROR(trigger_restore<TaskExecutor>(state, std::forward<MemGuard>(guard)));
    _read_rows += chunk->num_rows();
    COUNTER_UPDATE(_spiller->metrics().restore_rows, chunk->num_rows());
    TRACE_SPILL_LOG << "restore rows: " << chunk->num_rows() << ", total restored: " << _read_rows << ", " << this;
    return chunk;
}

template <class TaskExecutor, class MemGuard>
Status SpillerReader::trigger_restore(RuntimeState* state, MemGuard&& guard) {
    if (_stream == nullptr) {
        return Status::OK();
    }
    // if all is well and input stream enable prefetch and not eof
    if (!_stream->eof()) {
        // make sure _running_restore_tasks < io_tasks_per_scan_operator to avoid scan overloaded
        if (_stream->is_ready() && _running_restore_tasks >= config::io_tasks_per_scan_operator) {
            return Status::OK();
        }
        _running_restore_tasks++;
        auto restore_task = [this, guard, trace = TraceInfo(state),
                             _stream = _stream](auto& yield_ctx) -> CompletionDecided {
            SCOPED_SET_TRACE_INFO({}, trace.query_id, trace.fragment_id);
            SCOPED_SET_MODULE_TYPE(ThreadModuleType::QUERY);
            auto yield_defer = yield_ctx.defer_finished();
            if (!guard.scoped_begin()) {
                return CompletionDecided::abandoned();
            }
            // Restore completion, RAII: do_read has already published the stream data; at scope exit the
            // per-reader counter drops and the source-side sleepers waiting on restore IO are woken. This
            // is common to the main, transient and sort sub-stream readers, so they all go through here.
            // Cancelled on a yield-resubmit, so the completion fires once, when the restore finishes.
            auto guarded = _spiller->defer_complete_io(
                    [&]() {
                        FAIL_POINT_TRIGGER_EXECUTE(spill_restore_block, { spill_restore_block_barrier().arrive_A(); });
                        _running_restore_tasks--;
                    },
                    /*wake_sink=*/false, guard);
            if (_spiller->is_cancel() || !_spiller->task_status().ok()) {
                return CompletionDecided(guarded);
            }
            Status res;
            SerdeContext serd_ctx;
            if (!yield_ctx.task_context_data.has_value()) {
                yield_ctx.task_context_data = std::make_shared<SpillIOTaskContext>();
            }

            auto ctx = std::any_cast<SpillIOTaskContextPtr>(yield_ctx.task_context_data);
            yield_ctx.time_spent_ns = 0;
            yield_ctx.need_yield = false;

            FAIL_POINT_TRIGGER_EXECUTE(spill_restore_sleep, { sleep(10); });

            YieldableRestoreTask task(_stream);
            res = task.do_read(yield_ctx, serd_ctx);

            FAIL_POINT_TRIGGER_EXECUTE(spill_restore_sleep, { sleep(10); });
            // Simulate a non-EOF error coming out of the restore IO task
            // (e.g. ColumnarSerde::deserialize hitting a short read).
            FAIL_POINT_TRIGGER_EXECUTE(spill_restore_error,
                                       { res = Status::InternalError("inject spill_restore_error"); });

            if (yield_ctx.need_yield && !yield_ctx.is_finished()) {
                COUNTER_UPDATE(_spiller->metrics().restore_task_yield_times, 1);
                guarded.cancel_for_yield(yield_defer);
            }

            if (!res.is_ok_or_eof()) {
                _spiller->update_spilled_task_status(std::move(res));
            }
            _finished_restore_tasks += !res.ok();
            return CompletionDecided(guarded);
        };
        auto yield_func = [&](workgroup::ScanTask&& task) {
            auto ctx = std::any_cast<SpillIOTaskContextPtr>(task.get_work_context().task_context_data);
            TaskExecutor::force_submit(std::move(task));
        };
        auto query_type = spill_query_type(state);
        auto io_task = workgroup::ScanTask(_spiller->options().wg, std::move(restore_task), std::move(yield_func));
        io_task.set_query_type(query_type);
        _spiller->increase_in_flight_io();
        Status submit_st;
        FAIL_POINT_TRIGGER_EXECUTE(spill_submit_error,
                                   { submit_st = Status::InternalError("inject spill_submit_error"); });
        if (submit_st.ok()) {
            submit_st = TaskExecutor::submit(std::move(io_task));
        }
        if (!submit_st.ok()) {
            // Full compensation: the restore task lambda never runs, so its completion defer does not fire,
            // but every restore attempt must still produce exactly one completion.
            _spiller->complete_io(
                    [&]() {
                        _running_restore_tasks--;
                        _spiller->update_spilled_task_status(std::move(submit_st));
                    },
                    /*wake_sink=*/false);
            return _spiller->task_status();
        }
        COUNTER_UPDATE(_spiller->metrics().restore_io_task_count, 1);
        COUNTER_SET(_spiller->metrics().peak_restore_io_task_count, _running_restore_tasks.load());
    }
    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
Status PartitionedSpillerWriter::spill(RuntimeState* state, const ChunkPtr& chunk, MemGuard&& guard) {
    DCHECK(!chunk->is_empty());
    DCHECK(!is_full());

    // the last column was hash column
    auto hash_column = chunk->columns().back();

    {
        SCOPED_TIMER(_spiller->metrics().shuffle_timer);
        std::vector<uint32_t> shuffle_result;
        shuffle(shuffle_result, down_cast<SpillHashColumn*>(hash_column->as_mutable_raw_ptr()));
        process_partition_data(chunk, shuffle_result,
                               [&chunk](SpilledPartition* partition, const std::vector<uint32_t>& selection,
                                        int32_t from, int32_t size) {
                                   auto mem_table = partition->spill_writer->mem_table();
                                   (void)mem_table->append_selective(*chunk, selection.data(), from, size);
                                   partition->mem_size = mem_table->mem_usage();
                                   partition->num_rows += size;
                               });
    }

    DCHECK_EQ(_spiller->spilled_append_rows(), _partition_rows());

    RETURN_IF_ERROR(flush_if_full<TaskExecutor>(state, guard));

    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
Status PartitionedSpillerWriter::flush_if_full(RuntimeState* state, MemGuard&& guard) {
    if (_mem_tracker->consumption() > options().spill_mem_table_bytes_size) {
        return flush<TaskExecutor>(state, false, guard);
    }
    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
Status PartitionedSpillerWriter::flush(RuntimeState* state, bool is_final_flush, MemGuard&& guard) {
    std::vector<SpilledPartition*> splitting_partitions, spilling_partitions;
    RETURN_IF_ERROR(_choose_partitions_to_flush(is_final_flush, splitting_partitions, spilling_partitions));
    if (spilling_partitions.empty() && splitting_partitions.empty()) {
        return Status::OK();
    }

    if (is_final_flush && _running_flush_tasks > 0) {
        _need_final_flush = true;
        return Status::OK();
    }

    std::vector<SpilledPartition*> flushing_partitions;
    flushing_partitions.reserve(spilling_partitions.size() + splitting_partitions.size());
    flushing_partitions.insert(flushing_partitions.end(), splitting_partitions.begin(), splitting_partitions.end());
    flushing_partitions.insert(flushing_partitions.end(), spilling_partitions.begin(), spilling_partitions.end());
    RETURN_IF_ERROR(_pick_and_compact_skew_partitions(flushing_partitions));

    DCHECK_EQ(_running_flush_tasks, 0);
    _running_flush_tasks++;

    auto task = [this, guard = guard, splitting_partitions = std::move(splitting_partitions),
                 spilling_partitions = std::move(spilling_partitions),
                 trace = TraceInfo(state)](auto& yield_ctx) -> CompletionDecided {
        SCOPED_SET_TRACE_INFO({}, trace.query_id, trace.fragment_id);
        auto yield_defer = yield_ctx.defer_finished();
        if (!guard.scoped_begin()) {
            return CompletionDecided::abandoned();
        }
        // Partitioned writer freed (the partitionwise/join flush path), RAII: is_full() reads
        // running_flush_tasks, published by the decrement; both lists are woken, as on the single-writer
        // flush completion above. Cancelled on a yield-resubmit, so the completion fires once, when the
        // flush finishes.
        auto guarded = _spiller->defer_complete_io(
                [&]() {
                    FAIL_POINT_TRIGGER_EXECUTE(spill_flush_block, { spill_flush_block_barrier().arrive_A(); });
                    _spiller->update_spilled_task_status(_decrease_running_flush_tasks());
                },
                /*wake_sink=*/true, guard);
        // concurrency test
        RACE_DETECT(detect_flush);

        if (_spiller->is_cancel() || !_spiller->task_status().ok()) {
            return CompletionDecided(guarded);
        }
        yield_ctx.time_spent_ns = 0;
        yield_ctx.need_yield = false;
        if (!yield_ctx.task_context_data.has_value()) {
            yield_ctx.task_context_data =
                    SpillIOTaskContextPtr(std::make_shared<PartitionedFlushContext>(_spiller->shared_from_this()));
        }
        _spiller->update_spilled_task_status(
                yieldable_flush_task(yield_ctx, splitting_partitions, spilling_partitions));

        if (yield_ctx.need_yield && !yield_ctx.is_finished()) {
            COUNTER_UPDATE(_spiller->metrics().flush_task_yield_times, 1);
            guarded.cancel_for_yield(yield_defer);
        }
        return CompletionDecided(guarded);
    };
    auto yield_func = [&](workgroup::ScanTask&& task) { TaskExecutor::force_submit(std::move(task)); };
    auto query_type = spill_query_type(state);
    auto io_task = workgroup::ScanTask(_spiller->options().wg, std::move(task), std::move(yield_func));
    io_task.set_query_type(query_type);
    _spiller->increase_in_flight_io();
    Status submit_st;
    FAIL_POINT_TRIGGER_EXECUTE(spill_submit_error, { submit_st = Status::InternalError("inject spill_submit_error"); });
    if (submit_st.ok()) {
        submit_st = TaskExecutor::submit(std::move(io_task));
    }
    if (!submit_st.ok()) {
        // Full compensation: the task lambda never runs, so its completion defer does not fire, but every
        // flush attempt must still produce exactly one completion. Partitions own their mem-tables (nothing
        // to return to a pool here).
        _spiller->complete_io(
                [&]() {
                    _spiller->update_spilled_task_status(_decrease_running_flush_tasks());
                    _spiller->update_spilled_task_status(std::move(submit_st));
                },
                /*wake_sink=*/true);
        return _spiller->task_status();
    }
    COUNTER_UPDATE(_spiller->metrics().flush_io_task_count, 1);
    COUNTER_SET(_spiller->metrics().peak_flush_io_task_count, _running_flush_tasks.load());

    return Status::OK();
}

} // namespace starrocks::spill
