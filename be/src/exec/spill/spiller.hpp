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
#include <mutex>
#include <utility>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/spill/common.h"
#include "exec/spill/executor.h"
#include "exec/spill/input_stream.h"
#include "exec/spill/serde.h"
#include "exec/spill/spill_components.h"
#include "exec/spill/spiller.h"
#include "storage/chunk_helper.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"

namespace starrocks::spill {
template <class TaskExecutor, class MemGuard>
Status Spiller::spill(RuntimeState* state, const ChunkPtr& chunk, TaskExecutor&& executor, MemGuard&& guard) {
    SCOPED_TIMER(_metrics.append_data_timer);
    RETURN_IF_ERROR(task_status());
    DCHECK(!chunk->is_empty());
    DCHECK(!is_full());

    COUNTER_UPDATE(_metrics.spill_rows, chunk->num_rows());
    _spilled_append_rows += chunk->num_rows();
    TRACE_SPILL_LOG << "spilled rows:" << chunk->num_rows() << ",cumulative:" << _spilled_append_rows
                    << ",spiller:" << this;

    if (_chunk_builder.chunk_schema()->empty()) {
        _chunk_builder.chunk_schema()->set_schema(chunk);
        RETURN_IF_ERROR(_serde->prepare());
    }

    if (_opts.init_partition_nums > 0) {
        return _writer->as<PartitionedSpillerWriter*>()->spill(state, chunk, executor, guard);
    } else {
        return _writer->as<RawSpillerWriter*>()->spill(state, chunk, executor, guard);
    }
}

template <class Processer, class TaskExecutor, class MemGuard>
Status Spiller::partitioned_spill(RuntimeState* state, const ChunkPtr& chunk, SpillHashColumn* hash_column,
                                  Processer&& processer, TaskExecutor&& executor, MemGuard&& guard) {
    SCOPED_TIMER(_metrics.append_data_timer);
    RETURN_IF_ERROR(task_status());
    DCHECK(!chunk->is_empty());
    COUNTER_UPDATE(_metrics.spill_rows, chunk->num_rows());
    DCHECK_GT(_opts.init_partition_nums, 0);

    if (_chunk_builder.chunk_schema()->empty()) {
        _chunk_builder.chunk_schema()->set_schema(chunk);
        RETURN_IF_ERROR(_serde->prepare());
    }

    std::vector<uint32_t> indexs;
    auto writer = _writer->as<PartitionedSpillerWriter*>();
    {
        SCOPED_TIMER(_metrics.shuffle_timer);
        writer->shuffle(indexs, hash_column);
        writer->process_partition_data(chunk, indexs, std::forward<Processer>(processer));
    }
    COUNTER_SET(_metrics.partition_writer_peak_memory_usage, writer->mem_consumption());
    RETURN_IF_ERROR(writer->flush_if_full(state, executor, guard));
    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
Status Spiller::flush(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
    RETURN_IF_ERROR(task_status());
    if (_opts.init_partition_nums > 0) {
        return _writer->as<PartitionedSpillerWriter*>()->flush(state, true, executor, guard);
    } else {
        return _writer->as<RawSpillerWriter*>()->flush(state, executor, guard);
    }
}

template <class TaskExecutor, class MemGuard>
StatusOr<ChunkPtr> Spiller::restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
    RETURN_IF_ERROR(task_status());

    ASSIGN_OR_RETURN(auto chunk, _reader->restore(state, executor, guard));
    chunk->check_or_die();
    _restore_read_rows += chunk->num_rows();

    RETURN_IF_ERROR(trigger_restore(state, std::forward<TaskExecutor>(executor), std::forward<MemGuard>(guard)));
    return chunk;
}

template <class TaskExecutor, class MemGuard>
Status Spiller::trigger_restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
    return _reader->trigger_restore(state, executor, guard);
}

template <class TaskExecutor, class MemGuard>
Status RawSpillerWriter::spill(RuntimeState* state, const ChunkPtr& chunk, TaskExecutor&& executor, MemGuard&& guard) {
    if (_mem_table == nullptr) {
        _mem_table = _acquire_mem_table_from_pool();
        DCHECK(_mem_table != nullptr);
    }

    RETURN_IF_ERROR(_mem_table->append(chunk));

    if (_mem_table->is_full()) {
        return flush(state, std::forward<TaskExecutor>(executor), std::forward<MemGuard>(guard));
    }

    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
Status RawSpillerWriter::flush(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
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
    // TODO: handle spill queue
    auto task = [this, state, guard = guard, mem_table = std::move(captured_mem_table),
                 trace = TraceInfo(state)](auto& yield_ctx) {
        SCOPED_SET_TRACE_INFO({}, trace.query_id, trace.fragment_id);
        RETURN_IF(!guard.scoped_begin(), Status::Cancelled("cancelled"));
        DEFER_GUARD_END(guard);
        SCOPED_TIMER(_spiller->metrics().flush_timer);
        DCHECK_GT(_running_flush_tasks, 0);
        DCHECK(has_pending_data());
        //
        if (!yield_ctx.task_context_data.has_value()) {
            yield_ctx.task_context_data = std::make_shared<FlushContext>();
        }
        auto defer = CancelableDefer([&]() {
            {
                std::lock_guard _(_mutex);
                _mem_table_pool.emplace(std::move(mem_table));
            }
            _spiller->update_spilled_task_status(_decrease_running_flush_tasks());
            yield_ctx.set_finished();
        });

        if (_spiller->is_cancel() || !_spiller->task_status().ok()) {
            return Status::OK();
        }

        yield_ctx.time_spent_ns = 0;
        yield_ctx.need_yield = false;

        if (!yield_ctx.task_context_data.has_value()) {
            yield_ctx.task_context_data = std::make_shared<FlushContext>();
        }
        _spiller->update_spilled_task_status(yieldable_flush_task(yield_ctx, state, mem_table));
        if (yield_ctx.need_yield) {
            COUNTER_UPDATE(_spiller->metrics().flush_task_yield_times, 1);
            defer.cancel();
        }

        return Status::OK();
    };
    // submit io task
    RETURN_IF_ERROR(executor.submit(std::move(task)));
    COUNTER_UPDATE(_spiller->metrics().flush_io_task_count, 1);
    COUNTER_SET(_spiller->metrics().peak_flush_io_task_count, _running_flush_tasks);
    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
StatusOr<ChunkPtr> SpillerReader::restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
    SCOPED_TIMER(_spiller->metrics().restore_from_buffer_timer);
    ASSIGN_OR_RETURN(auto chunk, _stream->get_next(_spill_read_ctx));
    RETURN_IF_ERROR(trigger_restore(state, std::forward<TaskExecutor>(executor), std::forward<MemGuard>(guard)));
    _read_rows += chunk->num_rows();
    COUNTER_UPDATE(_spiller->metrics().restore_rows, chunk->num_rows());
    TRACE_SPILL_LOG << "restore rows: " << chunk->num_rows() << ", total restored: " << _read_rows << ", " << this;
    return chunk;
}

template <class TaskExecutor, class MemGuard>
Status SpillerReader::trigger_restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
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
        auto restore_task = [this, guard, trace = TraceInfo(state), _stream = _stream](auto& yield_ctx) {
            SCOPED_SET_TRACE_INFO({}, trace.query_id, trace.fragment_id);
            RETURN_IF(!guard.scoped_begin(), (void)0);
            DEFER_GUARD_END(guard);
            {
                auto defer = CancelableDefer([&]() {
                    _running_restore_tasks--;
                    yield_ctx.set_finished();
                });
                Status res;
                SerdeContext serd_ctx;

                yield_ctx.time_spent_ns = 0;
                yield_ctx.need_yield = false;

                YieldableRestoreTask task(_stream);
                res = task.do_read(yield_ctx, serd_ctx);

                if (yield_ctx.need_yield) {
                    COUNTER_UPDATE(_spiller->metrics().restore_task_yield_times, 1);
                    defer.cancel();
                }

                if (!res.is_ok_or_eof()) {
                    _spiller->update_spilled_task_status(std::move(res));
                }
                _finished_restore_tasks += !res.ok();
            };
        };
        RETURN_IF_ERROR(executor.submit(std::move(restore_task)));
        COUNTER_UPDATE(_spiller->metrics().restore_io_task_count, 1);
        COUNTER_SET(_spiller->metrics().peak_flush_io_task_count, _running_restore_tasks);
    }
    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
Status PartitionedSpillerWriter::spill(RuntimeState* state, const ChunkPtr& chunk, TaskExecutor&& executor,
                                       MemGuard&& guard) {
    DCHECK(!chunk->is_empty());
    DCHECK(!is_full());

    // the last column was hash column
    auto hash_column = chunk->columns().back();

    {
        SCOPED_TIMER(_spiller->metrics().shuffle_timer);
        std::vector<uint32_t> shuffle_result;
        shuffle(shuffle_result, down_cast<SpillHashColumn*>(hash_column.get()));
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

    RETURN_IF_ERROR(flush_if_full(state, executor, guard));

    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
Status PartitionedSpillerWriter::flush_if_full(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
    if (_mem_tracker->consumption() > options().spill_mem_table_bytes_size) {
        return flush(state, false, executor, guard);
    }
    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
Status PartitionedSpillerWriter::flush(RuntimeState* state, bool is_final_flush, TaskExecutor&& executor,
                                       MemGuard&& guard) {
    std::vector<SpilledPartition*> splitting_partitions, spilling_partitions;
    RETURN_IF_ERROR(_choose_partitions_to_flush(is_final_flush, splitting_partitions, spilling_partitions));

    if (spilling_partitions.empty() && splitting_partitions.empty()) {
        return Status::OK();
    }

    if (is_final_flush && _running_flush_tasks > 0) {
        _need_final_flush = true;
        return Status::OK();
    }
    DCHECK_EQ(_running_flush_tasks, 0);
    _running_flush_tasks++;

    auto task = [this, guard = guard, splitting_partitions = std::move(splitting_partitions),
                 spilling_partitions = std::move(spilling_partitions), trace = TraceInfo(state)](auto& yield_ctx) {
        SCOPED_SET_TRACE_INFO({}, trace.query_id, trace.fragment_id);
        RETURN_IF(!guard.scoped_begin(), Status::Cancelled("cancelled"));
        DEFER_GUARD_END(guard);
        // concurrency test
        RACE_DETECT(detect_flush, var1);
        auto defer = CancelableDefer([&]() {
            _spiller->update_spilled_task_status(_decrease_running_flush_tasks());
            yield_ctx.set_finished();
        });

        if (_spiller->is_cancel() || !_spiller->task_status().ok()) {
            return Status::OK();
        }
        yield_ctx.time_spent_ns = 0;
        yield_ctx.need_yield = false;
        if (!yield_ctx.task_context_data.has_value()) {
            yield_ctx.task_context_data = std::make_shared<PartitionedFlushContext>();
        }
        _spiller->update_spilled_task_status(
                yieldable_flush_task(yield_ctx, splitting_partitions, spilling_partitions));

        if (yield_ctx.need_yield) {
            COUNTER_UPDATE(_spiller->metrics().flush_task_yield_times, 1);
            defer.cancel();
        }
        return Status::OK();
    };

    RETURN_IF_ERROR(executor.submit(std::move(task)));
    COUNTER_UPDATE(_spiller->metrics().flush_io_task_count, 1);
    COUNTER_SET(_spiller->metrics().peak_flush_io_task_count, _running_flush_tasks);

    return Status::OK();
}

} // namespace starrocks::spill