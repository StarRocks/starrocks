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
    COUNTER_SET(_metrics.partition_writer_peak_memory_usage, 0);
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
    auto query_ctx = state->query_ctx()->weak_from_this();
    auto task = [this, state, guard = guard, mem_table = std::move(captured_mem_table), query_ctx,
                 trace = TraceInfo(state)]() {
        SCOPED_SET_TRACE_INFO({}, trace.query_id, trace.fragment_id);
        auto lcked = query_ctx.lock();
        RETURN_IF(!lcked || !guard.scoped_begin(), Status::Cancelled("cancelled"));
        SCOPED_TIMER(_spiller->metrics().flush_timer);
        DCHECK_GT(_running_flush_tasks, 0);
        DCHECK(has_pending_data());
        //
        auto defer = DeferOp([&]() {
            {
                std::lock_guard _(_mutex);
                _mem_table_pool.emplace(std::move(mem_table));
            }

            _spiller->update_spilled_task_status(_decrease_running_flush_tasks());
            guard.scoped_end();
        });
        if (_spiller->is_cancel() || !_spiller->task_status().ok()) {
            return Status::OK();
        }
        _spiller->update_spilled_task_status(flush_task(state, mem_table));
        return Status::OK();
    };
    // submit io task
    RETURN_IF_ERROR(executor.submit(std::move(task)));
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

    DCHECK(_stream->enable_prefetch());
    // if all is well and input stream enable prefetch and not eof
    if (!_stream->eof()) {
        _running_restore_tasks++;
        auto query_ctx = state->query_ctx()->weak_from_this();
        auto restore_task = [this, state, guard, query_ctx, trace = TraceInfo(state)]() {
            SCOPED_SET_TRACE_INFO({}, trace.query_id, trace.fragment_id);
            auto lcked = query_ctx.lock();
            RETURN_IF(!lcked || !guard.scoped_begin(), Status::OK());
            auto defer = DeferOp([&]() { _running_restore_tasks--; });
            {
                Status res;
                SerdeContext ctx;
                res = _stream->prefetch(ctx);

                if (!res.is_end_of_file() && !res.ok()) {
                    _spiller->update_spilled_task_status(std::move(res));
                }
                if (!res.ok()) {
                    _finished_restore_tasks++;
                }
            };
            guard.scoped_end();
            return Status::OK();
        };
        RETURN_IF_ERROR(executor.submit(std::move(restore_task)));
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
                                   mem_table->append_selective(*chunk, selection.data(), from, size);
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

template <class ChunkProvider>
Status PartitionedSpillerWriter::spill_partition(SerdeContext& ctx, SpilledPartition* partition,
                                                 ChunkProvider&& provider) {
    auto& serde = _spiller->serde();

    if (partition->spill_writer->block() == nullptr) {
        spill::AcquireBlockOptions opts;
        opts.query_id = _runtime_state->query_id();
        opts.plan_node_id = options().plan_node_id;
        opts.name = options().name;
        ASSIGN_OR_RETURN(auto block, _spiller->block_manager()->acquire_block(opts));
        std::lock_guard<std::mutex> l(_mutex);
        partition->spill_writer->block_group().append(block);
        partition->spill_writer->block() = block;
    }

    auto& block = partition->spill_writer->block();
    DCHECK(block != nullptr);

    auto consumer = [&](const auto& chunk) {
        RETURN_IF_ERROR(serde->serialize(ctx, chunk, block));
        partition->bytes += chunk->memory_usage();
        return Status::OK();
    };

    RETURN_IF_ERROR(provider(consumer));

    if (partition->spill_writer->block()->size() > options().spill_mem_table_bytes_size) {
        RETURN_IF_ERROR(block->flush());
        RETURN_IF_ERROR(_spiller->block_manager()->release_block(block));
        block.reset();
    }
    return Status::OK();
}

template <class Consumer>
class AccumulateWriter {
public:
    AccumulateWriter(Consumer& consumer, size_t chunk_size) : _consumer(consumer) {
        _accumulator.set_max_size(chunk_size);
    }

    Status write(const ChunkPtr& chunk) {
        DCHECK(_accumulator.need_input());
        _accumulator.push(chunk);
        if (_accumulator.has_output()) {
            auto normalized = std::move(_accumulator.pull());
            RETURN_IF_ERROR(_consumer(normalized));
        }
        return Status::OK();
    }

    Status flush() {
        _accumulator.finalize();
        if (auto chunk = std::move(_accumulator.pull())) {
            if (!chunk->is_empty()) {
                RETURN_IF_ERROR(_consumer(chunk));
            }
        }
        return Status::OK();
    }

private:
    Consumer& _consumer;
    ChunkPipelineAccumulator _accumulator;
};

struct DoSpillPartition {
public:
    DoSpillPartition(SerdeContext& spill_ctx_, SpilledPartition* partition_, PartitionedSpillerWriter* writer_)
            : spill_ctx(spill_ctx_), partition(partition_), writer(writer_) {}

    Status operator()(const ChunkPtr& chunk) {
        return writer->spill_partition(spill_ctx, partition, [&](auto& consumer) {
            RETURN_IF_ERROR(consumer(chunk));
            partition->num_rows += chunk->num_rows();
            return Status::OK();
        });
    }

private:
    SerdeContext& spill_ctx;
    SpilledPartition* partition;
    PartitionedSpillerWriter* writer;
};

template <class MemGuard>
Status PartitionedSpillerWriter::_split_partition(SerdeContext& spill_ctx, SpillerReader* reader,
                                                  SpilledPartition* partition, SpilledPartition* left_partition,
                                                  SpilledPartition* right_partition, MemGuard& guard) {
    size_t current_level = partition->level;
    size_t restore_rows = 0;

    DoSpillPartition spill_left_partition(spill_ctx, left_partition, this);
    DoSpillPartition spill_right_partition(spill_ctx, right_partition, this);

    AccumulateWriter left_accumulate_writer(spill_left_partition, _runtime_state->chunk_size());
    AccumulateWriter right_accumulate_writer(spill_right_partition, _runtime_state->chunk_size());

    auto defer = DeferOp([&]() {
        left_accumulate_writer.flush();
        right_accumulate_writer.flush();
    });

    while (true) {
        RETURN_IF_ERROR(reader->trigger_restore(_runtime_state, SyncTaskExecutor{}, guard));
        if (!reader->has_output_data()) {
            DCHECK_EQ(restore_rows, partition->num_rows);
            break;
        }
        ASSIGN_OR_RETURN(auto chunk, reader->restore(_runtime_state, SyncTaskExecutor{}, guard));
        restore_rows += chunk->num_rows();
        if (chunk->is_empty()) {
            continue;
        }
        auto hash_column = down_cast<SpillHashColumn*>(chunk->columns().back().get());
        const auto& hash_data = hash_column->get_data();
        // hash data
        std::vector<uint32_t> shuffle_result;
        shuffle_result.resize(hash_data.size());
        size_t left_channel_size = 0;
        for (size_t i = 0; i < hash_data.size(); ++i) {
            shuffle_result[i] = hash_data[i] >> current_level & 0x01;
            left_channel_size += !shuffle_result[i];
        }
        size_t left_cursor = 0;
        size_t right_cursor = left_channel_size;
        std::vector<uint32_t> selection(hash_data.size());
        for (size_t i = 0; i < hash_data.size(); ++i) {
            if (shuffle_result[i] == 0) {
                selection[left_cursor++] = i;
            } else {
                selection[right_cursor++] = i;
            }
        }

#ifndef NDEBUG
        for (size_t i = 0; i < left_cursor; i++) {
            DCHECK_EQ(hash_data[selection[i]] & left_partition->mask(),
                      left_partition->partition_id & left_partition->mask());
        }

        for (size_t i = left_cursor; i < right_cursor; i++) {
            DCHECK_EQ(hash_data[selection[i]] & right_partition->mask(),
                      right_partition->partition_id & right_partition->mask());
        }
#endif

        if (left_channel_size > 0) {
            ChunkPtr left_chunk = chunk->clone_empty();
            left_chunk->append_selective(*chunk, selection.data(), 0, left_channel_size);
            left_accumulate_writer.write(left_chunk);
        }
        if (hash_data.size() != left_channel_size) {
            ChunkPtr right_chunk = chunk->clone_empty();
            right_chunk->append_selective(*chunk, selection.data(), left_channel_size,
                                          hash_data.size() - left_channel_size);
            right_accumulate_writer.write(right_chunk);
        }
    }
    DCHECK_EQ(restore_rows, partition->num_rows);
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

    _running_flush_tasks++;
    auto query_ctx = state->query_ctx()->weak_from_this();

    auto task = [this, state, guard = guard, splitting_partitions = std::move(splitting_partitions),
                 spilling_partitions = std::move(spilling_partitions), query_ctx, trace = TraceInfo(state)]() {
        SCOPED_SET_TRACE_INFO({}, trace.query_id, trace.fragment_id);
        auto lcked = query_ctx.lock();
        RETURN_IF(!lcked || !guard.scoped_begin(), Status::Cancelled("cancelled"));
        DCHECK_EQ(_running_flush_tasks, 1);
        auto defer = DeferOp([&]() {
            _spiller->update_spilled_task_status(_decrease_running_flush_tasks());
            guard.scoped_end();
        });

        // partition memory usage
        // now we partitioned sorted spill
        SerdeContext spill_ctx;
        {
            SCOPED_TIMER(_spiller->metrics().flush_timer);
            for (auto partition : spilling_partitions) {
                RETURN_IF_ERROR(spill_partition(spill_ctx, partition, [&partition](auto& consumer) {
                    auto& mem_table = partition->spill_writer->mem_table();
                    RETURN_IF_ERROR(mem_table->flush(consumer));
                    return Status::OK();
                }));
            }
        }

        {
            SCOPED_TIMER(_spiller->metrics().split_partition_timer);
            for (auto partition : splitting_partitions) {
                auto [left, right] = partition->split();
                left->spill_writer = std::make_unique<RawSpillerWriter>(_spiller, _runtime_state, _mem_tracker.get());
                left->in_mem = false;
                RETURN_IF_ERROR(left->spill_writer->prepare(state));
                left->spill_writer->acquire_mem_table();
                right->in_mem = false;

                right->spill_writer = std::make_unique<RawSpillerWriter>(_spiller, _runtime_state, _mem_tracker.get());
                RETURN_IF_ERROR(right->spill_writer->prepare(state));
                right->spill_writer->acquire_mem_table();

                // write
                std::shared_ptr<SpillInputStream> stream;
                RETURN_IF_ERROR(partition->spill_writer->acquire_stream(&stream));

                auto reader = std::make_unique<SpillerReader>(_spiller);
                reader->set_stream(std::move(stream));

                // split process may be generate many small chunks. we should fix it
                auto st = _split_partition(spill_ctx, reader.get(), partition, left.get(), right.get(), guard);
                DCHECK(st.ok() || st.is_end_of_file());
                DCHECK_EQ(left->num_rows + right->num_rows, partition->num_rows);

                left->spill_writer->acquire_mem_table();
                right->spill_writer->acquire_mem_table();

                _add_partition(std::move(right));
                _add_partition(std::move(left));
            }

            for (auto partition : splitting_partitions) {
                _remove_partition(partition);
            }
        }

        return Status::OK();
    };

    RETURN_IF_ERROR(executor.submit(std::move(task)));

    return Status::OK();
}
} // namespace starrocks::spill