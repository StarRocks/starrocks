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

#include <mutex>
#include <utility>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/spill/common.h"
#include "exec/spill/serde.h"
#include "exec/spill/spiller.h"
#include "util/defer_op.h"

namespace starrocks::spill {
template <class TaskExecutor, class MemGuard>
Status Spiller::spill(RuntimeState* state, const ChunkPtr& chunk, TaskExecutor&& executor, MemGuard&& guard) {
    SCOPED_TIMER(_metrics.spill_timer);
    RETURN_IF_ERROR(_spilled_task_status);
    DCHECK(!chunk->is_empty());
    DCHECK(!is_full());

    COUNTER_UPDATE(_metrics.spill_rows, chunk->num_rows());
    _spilled_append_rows += chunk->num_rows();
    TRACE_SPILL_LOG << "spilled rows:" << chunk->num_rows() << ",cumulative:" << _spilled_append_rows
                    << ",spiller:" << this;

    if (_chunk_builder.chunk_schema()->empty()) {
        _chunk_builder.chunk_schema()->set_schema(chunk);
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
    SCOPED_TIMER(_metrics.spill_timer);
    RETURN_IF_ERROR(_spilled_task_status);
    DCHECK(!chunk->is_empty());
    COUNTER_UPDATE(_metrics.spill_rows, chunk->num_rows());
    DCHECK_GT(_opts.init_partition_nums, 0);

    if (_chunk_builder.chunk_schema()->empty()) {
        _chunk_builder.chunk_schema()->set_schema(chunk);
    }

    std::vector<uint32_t> indexs;
    auto writer = _writer->as<PartitionedSpillerWriter*>();
    writer->shuffle(indexs, hash_column);
    writer->process_partition_data(chunk, indexs, std::forward<Processer>(processer));
    RETURN_IF_ERROR(writer->flush_if_full(state, executor, guard));
    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
Status Spiller::flush(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
    RETURN_IF_ERROR(_spilled_task_status);
    if (_opts.init_partition_nums > 0) {
        return _writer->as<PartitionedSpillerWriter*>()->flush(state, executor, guard);
    } else {
        return _writer->as<RawSpillerWriter*>()->flush(state, executor, guard);
    }
}

template <class TaskExecutor, class MemGuard>
StatusOr<ChunkPtr> Spiller::restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
    RETURN_IF_ERROR(_spilled_task_status);

    ASSIGN_OR_RETURN(auto chunk, _reader->restore(state, executor, guard));
    chunk->check_or_die();
    COUNTER_UPDATE(_metrics.restore_rows, chunk->num_rows());
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
    }

    RETURN_IF_ERROR(_mem_table->append(chunk));

    if (_mem_table->is_full()) {
        return flush(state, std::forward<TaskExecutor>(executor), std::forward<MemGuard>(guard));
    }

    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
Status RawSpillerWriter::flush(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
    auto captured_mem_table = std::move(_mem_table);
    if (captured_mem_table == nullptr) {
        return Status::OK();
    }

    RETURN_IF_ERROR(captured_mem_table->done());
    _running_flush_tasks++;
    // TODO: handle spill queue
    auto task = [this, state, guard = guard, mem_table = std::move(captured_mem_table)]() {
        RETURN_IF(!guard.scoped_begin(), Status::Cancelled("cancelled"));
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

        _spiller->update_spilled_task_status(flush_task(state, mem_table));
        return Status::OK();
    };
    // submit io task
    RETURN_IF_ERROR(executor.submit(std::move(task)));
    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
StatusOr<ChunkPtr> SpillerReader::restore(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
    SCOPED_TIMER(_spiller->metrics().restore_timer);
    ASSIGN_OR_RETURN(auto chunk, _stream->get_next(_spill_read_ctx));
    RETURN_IF_ERROR(trigger_restore(state, std::forward<TaskExecutor>(executor), std::forward<MemGuard>(guard)));
    _read_rows += chunk->num_rows();
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
        auto restore_task = [this, state, guard]() {
            RETURN_IF(!guard.scoped_begin(), Status::OK());
            _running_restore_tasks++;
            SerdeContext ctx;
            auto res = _stream->prefetch(ctx);

            if (!res.is_end_of_file() && !res.ok()) {
                _spiller->update_spilled_task_status(std::move(res));
            }

            guard.scoped_end();
            _running_restore_tasks--;
            if (!res.ok()) {
                _finished_restore_tasks++;
            }
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
    if (_mem_tracker->consumption() > options().spill_file_size) {
        return flush(state, executor, guard);
    }
    return Status::OK();
}

template <class ChunkProvider>
Status PartitionedSpillerWriter::_spill_partition(SerdeContext& ctx, SpilledPartition* partition,
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

    if (partition->spill_writer->block()->size() > options().spill_file_size) {
        RETURN_IF_ERROR(block->flush());
        RETURN_IF_ERROR(_spiller->block_manager()->release_block(block));
        block.reset();
    }
    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
Status PartitionedSpillerWriter::flush(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
    // check need split partition
    std::vector<SpilledPartition*> splitting_partitions;
    if (options().splittable) {
        for (const auto& [pid, partition] : _id_to_partitions) {
            const auto& mem_table = partition->spill_writer->mem_table();
            // partition not in memory
            if (!partition->in_mem && partition->level < max_partition_level &&
                mem_table->mem_usage() + partition->bytes > options().max_memory_size_each_partition) {
                RETURN_IF_ERROR(mem_table->done());
                partition->in_mem = false;
                partition->mem_size = 0;
                partition->bytes += mem_table->mem_usage();
                partition->is_spliting = true;
                splitting_partitions.emplace_back(partition);
            }
        }
    }

    //
    std::vector<SpilledPartition*> spilling_partitions;
    for (const auto& [pid, partition] : _id_to_partitions) {
        const auto& mem_table = partition->spill_writer->mem_table();
        if (!partition->is_spliting && (mem_table->is_full() || mem_table->mem_usage() > options().min_spilled_size)) {
            RETURN_IF_ERROR(mem_table->done());
            partition->in_mem = false;
            partition->mem_size = 0;
            spilling_partitions.emplace_back(partition);
        }
    }

    _running_flush_tasks++;

    auto task = [this, state, guard = guard, splitting_partitions = std::move(splitting_partitions),
                 spilling_partitions = std::move(spilling_partitions)]() {
        RETURN_IF(!guard.scoped_begin(), Status::Cancelled("cancelled"));
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
                RETURN_IF_ERROR(_spill_partition(spill_ctx, partition, [&partition](auto& consumer) {
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