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

#include "exec/spill/spill_components.h"

#include <glog/logging.h>

#include <any>
#include <cstdint>
#include <memory>
#include <numeric>

#include "block_manager.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "exec/spill/common.h"
#include "exec/spill/data_stream.h"
#include "exec/spill/executor.h"
#include "exec/spill/serde.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller.hpp"
#include "exec/workgroup/scan_task_queue.h"
#include "exec/workgroup/work_group.h"
#include "exec/workgroup/work_group_fwd.h"
#include "util/bit_util.h"
#include "util/runtime_profile.h"

namespace starrocks::spill {
// implements for SpillerWriter
Status SpillerWriter::_decrease_running_flush_tasks() {
    if (_running_flush_tasks.fetch_sub(1) == 1) {
        if (_flush_all_callback) {
            RETURN_IF_ERROR(_flush_all_callback());
        }
    }
    return Status::OK();
}

const SpilledOptions& SpillerWriter::options() const {
    return _spiller->options();
}

bool RawSpillerWriter::has_pending_data() {
    std::lock_guard guard(_mutex);
    return _mem_table_pool.size() != options().mem_table_pool_size;
}

void RawSpillerWriter::prepare(RuntimeState* state) {
    if (!_mem_table_pool.empty()) {
        return;
    }

    const auto& opts = options();
    for (size_t i = 0; i < opts.mem_table_pool_size; ++i) {
        if (opts.is_unordered) {
            _mem_table_pool.push(std::make_unique<UnorderedMemTable>(state, opts.spill_mem_table_bytes_size,
                                                                     _parent_tracker, _spiller));
        } else {
            _mem_table_pool.push(
                    std::make_unique<OrderedMemTable>(&opts.sort_exprs->lhs_ordering_expr_ctxs(), opts.sort_desc, state,
                                                      opts.spill_mem_table_bytes_size, _parent_tracker, _spiller));
        }
    }
}

Status RawSpillerWriter::yieldable_flush_task(workgroup::YieldContext& yield_ctx, RuntimeState* state,
                                              const MemTablePtr& mem_table) {
    if (state->is_cancelled()) {
        return Status::OK();
    }

    enum WriterStage { FLUSH_MEM_TABLE = 0, COMPACT = 1, FINISH = 2 };

    DCHECK(yield_ctx.task_context_data.has_value()) << "flush context must be set";
    yield_ctx.total_yield_point_cnt = FINISH;

    switch (yield_ctx.yield_point) {
    case FLUSH_MEM_TABLE:
        RETURN_IF_ERROR(_spill_mem_table(yield_ctx, mem_table));
        RETURN_IF_YIELD(yield_ctx.need_yield);
        TO_NEXT_STAGE(yield_ctx.yield_point);
        [[fallthrough]];
    case COMPACT:
        RETURN_IF_ERROR(_compact_mem_table(yield_ctx));
        RETURN_IF_YIELD(yield_ctx.need_yield);
        TO_NEXT_STAGE(yield_ctx.yield_point);
        [[fallthrough]];
    default:
        DCHECK_EQ(yield_ctx.yield_point, 2);
    };

    return Status::OK();
}

Status RawSpillerWriter::_spill_mem_table(workgroup::YieldContext& yield_ctx, const MemTablePtr& mem_table) {
    auto io_task = std::any_cast<SpillIOTaskContextPtr>(yield_ctx.task_context_data);
    auto flush_ctx = std::static_pointer_cast<FlushContext>(io_task);

    // flush_ctx->output is not nullptr means the task is resumed from yield point
    DCHECK(flush_ctx->input_stream == nullptr);
    if (flush_ctx->output == nullptr) {
        DCHECK(flush_ctx->block_group == nullptr);
        auto block_group = std::make_shared<BlockGroup>();
        flush_ctx->output = create_spill_output_stream(_spiller, block_group.get(), _spiller->block_manager());
        flush_ctx->block_group = std::move(block_group);
    }

    // serialize mem table to data stream
    RETURN_IF_ERROR(mem_table->finalize(yield_ctx, flush_ctx->output));
    RETURN_IF_YIELD(yield_ctx.need_yield);
    RETURN_IF_ERROR(flush_ctx->output->flush());

    mem_table->reset();
    flush_ctx->output.reset();
    this->add_block_group(std::move(flush_ctx->block_group));

    return Status::OK();
}

Status RawSpillerWriter::_compact_mem_table(workgroup::YieldContext& yield_ctx) {
    auto io_task = std::any_cast<SpillIOTaskContextPtr>(yield_ctx.task_context_data);
    auto flush_ctx = std::static_pointer_cast<FlushContext>(io_task);
    // flush_ctx->output is not nullptr means the task is resumed from yield point
    if (flush_ctx->output == nullptr) {
        if (!_need_compact_block()) {
            return Status::OK();
        }

        std::vector<BlockGroupPtr> block_groups = _block_group_set.select_compaction_block_groups();
        if (block_groups.empty()) {
            return Status::OK();
        }

        COUNTER_UPDATE(_spiller->metrics().compact_count, 1);
        COUNTER_UPDATE(_spiller->metrics().compact_block_count, block_groups.size());

        flush_ctx->compact_input_num_rows =
                std::accumulate(block_groups.begin(), block_groups.end(), 0,
                                [](size_t sum, const auto& block_group) { return sum + block_group->num_rows(); });
        TRACE_SPILL_LOG << "spill compact block group input rows:" << flush_ctx->compact_input_num_rows;

        DCHECK(flush_ctx->block_group == nullptr);
        auto block_group = std::make_shared<BlockGroup>();
        flush_ctx->output = create_spill_output_stream(_spiller, block_group.get(), _spiller->block_manager());
        flush_ctx->block_group = std::move(block_group);
        ASSIGN_OR_RETURN(flush_ctx->input_stream,
                         BlockGroupSet::build_ordered_stream(block_groups, _runtime_state, _spiller->serde(), _spiller,
                                                             options().sort_exprs, options().sort_desc));
    }
    auto st = DataTranster::transfer(yield_ctx, _runtime_state, _spiller->serde().get(), flush_ctx->output,
                                     flush_ctx->input_stream);
    RETURN_IF(!st.is_ok_or_eof(), st);
    RETURN_IF_YIELD(yield_ctx.need_yield);
    RETURN_IF_ERROR(flush_ctx->output->flush());
    flush_ctx->output.reset();
    DCHECK_EQ(flush_ctx->compact_input_num_rows, flush_ctx->block_group->num_rows());
    this->add_block_group(std::move(flush_ctx->block_group));

    return Status::OK();
}

bool RawSpillerWriter::_need_compact_block() const {
    const auto& opts = options();
    if (!opts.enable_block_compaction) {
        return false;
    }

    if (opts.is_unordered) {
        return false;
    }

    if (_block_group_set.size() < _spiller->max_sorted_block_cnt()) {
        return false;
    }

    return true;
}

Status RawSpillerWriter::acquire_stream(const SpillPartitionInfo* partition,
                                        std::shared_ptr<SpillInputStream>* stream) {
    return acquire_stream(stream);
}

Status RawSpillerWriter::acquire_stream(std::shared_ptr<SpillInputStream>* stream) {
    std::shared_ptr<SpillInputStream> input_stream;
    const auto& serde = _spiller->serde();
    const auto& opts = options();

    if (opts.is_unordered) {
        ASSIGN_OR_RETURN(input_stream, _block_group_set.as_unordered_stream(serde, _spiller));
    } else {
        ASSIGN_OR_RETURN(input_stream, _block_group_set.as_ordered_stream(_runtime_state, serde, _spiller,
                                                                          opts.sort_exprs, opts.sort_desc));
    }

    *stream = input_stream;

    if (_mem_table != nullptr && !_mem_table->is_empty()) {
        DCHECK(opts.is_unordered);
        ASSIGN_OR_RETURN(auto mem_table_stream, _mem_table->as_input_stream(opts.read_shared));
        *stream = SpillInputStream::union_all(mem_table_stream, *stream);
    }

    return Status::OK();
}

PartitionedSpillerWriter::PartitionedSpillerWriter(Spiller* spiller, RuntimeState* state)
        : SpillerWriter(spiller, state), _mem_tracker(std::make_unique<MemTracker>(-1)) {}

void PartitionedSpillerWriter::prepare(RuntimeState* state) {
    DCHECK_GT(options().init_partition_nums, 0);
    _partition_set.resize(config::spill_max_partition_size);
    _init_with_partition_nums(state, options().init_partition_nums);
    for (auto [_, partition] : _id_to_partitions) {
        partition->spill_writer->prepare(state);
        partition->spill_writer->acquire_mem_table();
    }
}

Status PartitionedSpillerWriter::acquire_stream(const SpillPartitionInfo* partition,
                                                std::shared_ptr<SpillInputStream>* stream) {
    DCHECK(_id_to_partitions.count(partition->partition_id));
    RETURN_IF_ERROR(_id_to_partitions.at(partition->partition_id)->spill_writer->acquire_stream(partition, stream));
    return Status::OK();
}

void PartitionedSpillerWriter::get_spill_partitions(std::vector<const SpillPartitionInfo*>* res) {
    for (const auto& [level, partitions] : _level_to_partitions) {
        for (const auto& partition : partitions) {
            res->push_back(partition.get());
        }
    }
}

void PartitionedSpillerWriter::reset_partition(const std::vector<const SpillPartitionInfo*>& partitions) {
    DCHECK_GT(partitions.size(), 0);
    _level_to_partitions.clear();
    _id_to_partitions.clear();
    _min_level = std::numeric_limits<int32_t>::max();
    _max_level = std::numeric_limits<int32_t>::min();
    _max_partition_id = 0;
    std::fill(_partition_set.begin(), _partition_set.end(), false);
    size_t num_partitions = partitions.size();
    for (size_t i = 0; i < num_partitions; ++i) {
        _level_to_partitions[partitions[i]->level].emplace_back(
                std::make_unique<SpilledPartition>(partitions[i]->partition_id));
        auto* partition = _level_to_partitions[partitions[i]->level].back().get();

        _id_to_partitions.emplace(partition->partition_id, partition);
        partition->spill_writer = std::make_unique<RawSpillerWriter>(_spiller, _runtime_state, _mem_tracker.get());

        _max_partition_id = std::max(partition->partition_id, _max_partition_id);
        _min_level = std::min(_min_level, partition->level);
        _max_level = std::max(_max_level, partition->level);
        _partition_set[partitions[i]->partition_id] = true;
    }
    _prepare_partitions(_runtime_state);
}

void PartitionedSpillerWriter::reset_partition(RuntimeState* state, size_t num_partitions) {
    num_partitions = BitUtil::next_power_of_two(num_partitions);
    num_partitions = std::min<size_t>(num_partitions, 1 << config::spill_max_partition_level);
    num_partitions = std::max<size_t>(num_partitions, _spiller->options().init_partition_nums);
    _level_to_partitions.clear();
    _id_to_partitions.clear();
    std::fill(_partition_set.begin(), _partition_set.end(), false);
    _init_with_partition_nums(state, num_partitions);
    _prepare_partitions(state);
}

void PartitionedSpillerWriter::_init_with_partition_nums(RuntimeState* state, int num_partitions) {
    DCHECK((num_partitions & (num_partitions - 1)) == 0);
    DCHECK(num_partitions > 0);
    DCHECK(_level_to_partitions.empty());

    int level = partition_level(num_partitions);
    _min_level = level;
    _max_level = level;

    auto& partitions = _level_to_partitions[level];
    DCHECK(partitions.empty());

    for (int i = 0; i < num_partitions; ++i) {
        partitions.emplace_back(std::make_unique<SpilledPartition>(i + num_partitions));
        auto* partition = partitions.back().get();
        _id_to_partitions.emplace(partition->partition_id, partition);
        partition->spill_writer = std::make_unique<RawSpillerWriter>(_spiller, _runtime_state, _mem_tracker.get());
        _max_partition_id = std::max(partition->partition_id, _max_partition_id);
        _partition_set[partition->partition_id] = true;
    }
}

void PartitionedSpillerWriter::_prepare_partitions(RuntimeState* state) {
    for (auto [_, partition] : _id_to_partitions) {
        partition->spill_writer->prepare(state);
        partition->spill_writer->acquire_mem_table();
    }
}

void PartitionedSpillerWriter::_add_partition(SpilledPartitionPtr&& partition_ptr) {
    auto& partitions = _level_to_partitions[partition_ptr->level];
    partitions.emplace_back(std::move(partition_ptr));
    auto partition = partitions.back().get();
    _id_to_partitions.emplace(partition->partition_id, partition);
    _max_partition_id = std::max(partition->partition_id, _max_partition_id);
    _max_level = std::max(_max_level, partition->level);
    _min_level = std::min(_min_level, partition->level);
    std::sort(partitions.begin(), partitions.end(),
              [](const auto& left, const auto& right) { return left->partition_id < right->partition_id; });
    _partition_set[partition->partition_id] = true;
    _total_partition_num += 1;
}

void PartitionedSpillerWriter::_remove_partition(const SpilledPartition* partition) {
    auto affinity_group = partition->block_group->get_affinity_group();
    DCHECK(affinity_group != kDefaultBlockAffinityGroup);
    _id_to_partitions.erase(partition->partition_id);
    size_t level = partition->level;
    auto& partitions = _level_to_partitions[level];
    _partition_set[partition->partition_id] = false;
    auto iter = std::find_if(partitions.begin(), partitions.end(),
                             [partition](auto& val) { return val->partition_id == partition->partition_id; });
    _total_partition_num -= (iter != partitions.end());
    partitions.erase(iter);
    if (partitions.empty()) {
        _level_to_partitions.erase(level);
        if (_min_level == level) {
            _min_level = level + 1;
        }
    }
    WARN_IF_ERROR(_spiller->block_manager()->release_affinity_group(affinity_group),
                  fmt::format("release affinity group {} error", affinity_group));
}

Status PartitionedSpillerWriter::_choose_partitions_to_flush(bool is_final_flush,
                                                             std::vector<SpilledPartition*>& partitions_need_split,
                                                             std::vector<SpilledPartition*>& partitions_need_flush) {
    // find partitions that need split first
    if (options().splittable) {
        for (const auto& [pid, partition] : _id_to_partitions) {
            const auto& mem_table = partition->spill_writer->mem_table();
            // partition not in memory
            if (!partition->in_mem && partition->level < config::spill_max_partition_level &&
                mem_table->mem_usage() + partition->bytes > options().spill_mem_table_bytes_size) {
                RETURN_IF_ERROR(mem_table->done());
                partition->in_mem = false;
                partition->mem_size = 0;
                partition->bytes += mem_table->mem_usage();
                partition->is_spliting = true;
                partitions_need_split.emplace_back(partition);
            }
        }
    }

    // if the mem table of a partition is full, we flush it directly,
    // otherwise, we treat it as a candidate
    std::vector<SpilledPartition*> partitions_can_flush;
    for (const auto& [pid, partition] : _id_to_partitions) {
        const auto& mem_table = partition->spill_writer->mem_table();
        if (partition->is_spliting) {
            continue;
        }
        if (mem_table->is_full()) {
            RETURN_IF_ERROR(mem_table->done());
            partition->in_mem = false;
            partition->mem_size = 0;
            partitions_need_flush.emplace_back(partition);
        } else if (mem_table->mem_usage() > 0) {
            partitions_can_flush.emplace_back(partition);
        }
    }

    // if this is not the final flush and we can find some partitions to flush, just return
    if (!is_final_flush && !partitions_need_flush.empty()) {
        return Status::OK();
    }

    if (is_final_flush) {
        // for the final flush, we need to control the memory usage on hash join probe side,
        // so we should ensure the partitions loaded in memory under a certain threshold.

        // order by bytes desc
        std::sort(partitions_can_flush.begin(), partitions_can_flush.end(),
                  [](SpilledPartition* left, SpilledPartition* right) { return left->bytes > right->bytes; });
        size_t in_mem_bytes = 0;
        for (auto partition : partitions_can_flush) {
            if (in_mem_bytes + partition->bytes > options().spill_mem_table_bytes_size) {
                const auto& mem_table = partition->spill_writer->mem_table();
                RETURN_IF_ERROR(mem_table->done());
                partition->in_mem = false;
                partition->mem_size = 0;
                partitions_need_flush.emplace_back(partition);
                continue;
            }
            in_mem_bytes += partition->bytes;
        }
    } else {
        // for the flush during hash join build process, our goal is to reduce memory usage,
        // so only need to refer to the size of mem table for selection.

        // order by mem usage desc
        std::sort(partitions_can_flush.begin(), partitions_can_flush.end(),
                  [](SpilledPartition* left, SpilledPartition* right) {
                      return left->spill_writer->mem_table()->mem_usage() >
                             right->spill_writer->mem_table()->mem_usage();
                  });
        size_t accumulate_spill_bytes = 0;
        for (auto partition : partitions_can_flush) {
            const auto& mem_table = partition->spill_writer->mem_table();
            RETURN_IF_ERROR(mem_table->done());
            accumulate_spill_bytes += mem_table->mem_usage();
            partition->in_mem = false;
            partition->mem_size = 0;
            partitions_need_flush.emplace_back(partition);
            if (accumulate_spill_bytes > _mem_tracker->consumption() / 2) {
                break;
            }
        }
    }
    return Status::OK();
}

// make shuffle public
void PartitionedSpillerWriter::shuffle(std::vector<uint32_t>& dst, const SpillHashColumn* hash_column) {
    const auto& hashs = hash_column->get_data();
    dst.resize(hashs.size());

    if (_min_level == _max_level) {
        auto& partitions = _level_to_partitions[_min_level];
        DCHECK_EQ(partitions.size(), partitions.front()->level_elements());
        uint32_t hash_mask = partitions.front()->mask();
        uint32_t first_partition = partitions.front()->partition_id;

        for (size_t i = 0; i < hashs.size(); ++i) {
            dst[i] = (hashs[i] & hash_mask) + first_partition;
        }

    } else {
        uint32_t empty = std::numeric_limits<uint32_t>::max();
        std::fill(dst.begin(), dst.end(), empty);
        int32_t current_level = _min_level;
        // if has multi level, may be have performance issue
        while (current_level <= _max_level) {
            if (_level_to_partitions.find(current_level) == _level_to_partitions.end()) {
                current_level++;
                continue;
            }
            auto& partitions = _level_to_partitions[current_level];
            uint32_t hash_mask = partitions.front()->mask();
            for (size_t i = 0; i < hashs.size(); ++i) {
                size_t partition_id = (hashs[i] & hash_mask) + partitions[0]->level_elements();
                partition_id = _partition_set[partition_id] ? partition_id : empty;
                dst[i] = dst[i] == empty ? partition_id : dst[i];
            }
            current_level++;
        }
    }
}

Status PartitionedSpillerWriter::spill_partition(workgroup::YieldContext& yield_ctx, SerdeContext& ctx,
                                                 SpilledPartition* partition) {
    auto mem_table = partition->spill_writer->mem_table();
    auto mem_table_mem_usage = mem_table->mem_usage();
    if (partition->spill_output_stream == nullptr) {
        BlockAffinityGroup affinity_group = _spiller->block_manager()->acquire_affinity_group();
        auto block_group = std::make_shared<BlockGroup>(affinity_group);
        partition->block_group = block_group;
        partition->spill_writer->add_block_group(std::move(block_group));
        auto output = create_spill_output_stream(_spiller, partition->block_group.get(), _spiller->block_manager());
        std::lock_guard<std::mutex> l(_mutex);
        DCHECK_EQ(partition->spill_output_stream, nullptr);
        partition->spill_output_stream = output;
    }
    auto output = partition->spill_output_stream;
    RETURN_IF_ERROR(mem_table->finalize(yield_ctx, output));
    RETURN_IF_YIELD(yield_ctx.need_yield);
    RETURN_IF_ERROR(output->flush());

    partition->bytes += mem_table_mem_usage;
    TRACE_SPILL_LOG << fmt::format("spill partition[{}], bytes[{}] rows[{}]", partition->debug_string(),
                                   mem_table->mem_usage(), mem_table->num_rows());

    mem_table->reset();
    TRACE_SPILL_LOG << fmt::format("spill partition[{}] done ", partition->debug_string());
    return Status::OK();
}

Status PartitionedSpillerWriter::_spill_input_partitions(workgroup::YieldContext& yield_ctx, SerdeContext& context,
                                                         const std::vector<SpilledPartition*>& spilling_partitions) {
    SCOPED_TIMER(_spiller->metrics().flush_timer);
    auto io_task = std::any_cast<SpillIOTaskContextPtr>(yield_ctx.task_context_data);
    auto& flush_ctx = std::static_pointer_cast<PartitionedFlushContext>(io_task)->spill_stage_ctx;

    for (; flush_ctx.processing_idx < spilling_partitions.size();) {
        auto partition = spilling_partitions[flush_ctx.processing_idx];
        TRACE_SPILL_LOG << fmt::format("spill input partition[{}], processing idx[{}]", partition->debug_string(),
                                       flush_ctx.processing_idx);
        {
            SCOPED_RAW_TIMER(&yield_ctx.time_spent_ns);
            RETURN_IF_ERROR(spill_partition(yield_ctx, context, partition));
            RETURN_IF_YIELD(yield_ctx.need_yield);
            ++flush_ctx.processing_idx;
            // check time
            RETURN_OK_IF_NEED_YIELD(yield_ctx.wg, &yield_ctx.need_yield, yield_ctx.time_spent_ns);
        }
    }
    return Status::OK();
}

Status PartitionedSpillerWriter::_split_input_partitions(workgroup::YieldContext& yield_ctx, SerdeContext& context,
                                                         const std::vector<SpilledPartition*>& splitting_partitions) {
    SCOPED_TIMER(_spiller->metrics().split_partition_timer);
    auto io_task = std::any_cast<SpillIOTaskContextPtr>(yield_ctx.task_context_data);
    auto& flush_ctx = std::static_pointer_cast<PartitionedFlushContext>(io_task)->split_stage_ctx;

    // TODO: check skew data here
    for (; flush_ctx.spliting_idx < splitting_partitions.size(); flush_ctx.spliting_idx++) {
        // split stage
        auto partition = splitting_partitions[flush_ctx.spliting_idx];
        if (flush_ctx.reader == nullptr) {
            auto [left, right] = partition->split();
            left->spill_writer = std::make_unique<RawSpillerWriter>(_spiller, _runtime_state, _mem_tracker.get());
            left->in_mem = false;
            left->spill_writer->prepare(_runtime_state);
            left->spill_writer->acquire_mem_table();
            right->in_mem = false;

            right->spill_writer = std::make_unique<RawSpillerWriter>(_spiller, _runtime_state, _mem_tracker.get());
            right->spill_writer->prepare(_runtime_state);
            right->spill_writer->acquire_mem_table();

            // write
            std::shared_ptr<SpillInputStream> stream;
            RETURN_IF_ERROR(partition->spill_writer->acquire_stream(&stream));

            auto reader = std::make_unique<SpillerReader>(_spiller);
            reader->set_stream(std::move(stream));

            flush_ctx.reader = std::move(reader);
            flush_ctx.left = std::move(left);
            flush_ctx.right = std::move(right);
        }

        auto st = _split_partition(yield_ctx, context, flush_ctx.reader.get(), partition, flush_ctx.left.get(),
                                   flush_ctx.right.get());
        RETURN_IF_YIELD(yield_ctx.need_yield);
        RETURN_IF(!st.is_ok_or_eof(), st);
        DCHECK_EQ(flush_ctx.reader.get()->read_rows(), partition->num_rows);
        TRACE_SPILL_LOG << "reader:" << flush_ctx.reader.get() << " read rows:" << flush_ctx.reader->read_rows();
        DCHECK_EQ(flush_ctx.left->num_rows + flush_ctx.right->num_rows, partition->num_rows);

        flush_ctx.left->spill_writer->acquire_mem_table();
        flush_ctx.right->spill_writer->acquire_mem_table();

        DCHECK_EQ(flush_ctx.left->spill_output_stream->append_rows(), flush_ctx.left->num_rows);
        DCHECK_EQ(flush_ctx.left->spill_writer->block_group_num_rows(), flush_ctx.left->num_rows);
        DCHECK_EQ(flush_ctx.right->spill_writer->block_group_num_rows(), flush_ctx.right->num_rows);

        _add_partition(std::move(flush_ctx.right));
        _add_partition(std::move(flush_ctx.left));

        flush_ctx.reset_read_context();
    }

    for (auto partition : splitting_partitions) {
        _remove_partition(partition);
    }
    return Status::OK();
}

Status PartitionedSpillerWriter::_split_partition(workgroup::YieldContext& yield_ctx, SerdeContext& spill_ctx,
                                                  SpillerReader* reader, SpilledPartition* partition,
                                                  SpilledPartition* left_partition, SpilledPartition* right_partition) {
    size_t current_level = partition->level;
    auto left_mem_table = left_partition->spill_writer->mem_table();
    auto right_mem_table = right_partition->spill_writer->mem_table();

    TRACE_SPILL_LOG << fmt::format(
            "split partition [{}] to [{}] and [{}], left_mem_table_rows[{}], right_mem_table_rows[{}]",
            partition->debug_string(), left_partition->debug_string(), right_partition->debug_string(),
            left_mem_table->num_rows(), right_mem_table->num_rows());
    Status st;
    {
        auto flush_partition = [this, &spill_ctx, &yield_ctx](SpilledPartition* partition) -> Status {
            auto mem_table = partition->spill_writer->mem_table();
            if (mem_table->is_empty()) {
                return Status::OK();
            }
            RETURN_IF_ERROR(mem_table->done());
            return this->spill_partition(yield_ctx, spill_ctx, partition);
        };

        auto defer = DeferOp([&]() {
            RETURN_IF(yield_ctx.need_yield, (void)0);
            RETURN_IF(st = flush_partition(left_partition); !st.ok(), (void)0);
            RETURN_IF(yield_ctx.need_yield, (void)0);
            RETURN_IF(st = flush_partition(right_partition); !st.ok(), (void)0);
            RETURN_IF(yield_ctx.need_yield, (void)0);
        });

        // is mem table is done we should flush them firstly
        if (left_mem_table->is_done() || right_mem_table->is_done()) {
            return Status::OK();
        }

        TRY_CATCH_ALLOC_SCOPE_START()
        while (true) {
            {
                SCOPED_RAW_TIMER(&yield_ctx.time_spent_ns);
                RETURN_IF_ERROR(reader->trigger_restore<SyncTaskExecutor>(_runtime_state, EmptyMemGuard{}));
                if (!reader->has_output_data()) {
                    DCHECK_EQ(reader->read_rows(), partition->num_rows);
                    break;
                }
                ASSIGN_OR_RETURN(auto chunk, reader->restore<SyncTaskExecutor>(_runtime_state, EmptyMemGuard{}));
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
                    left_partition->num_rows += left_channel_size;
                    RETURN_IF_ERROR(left_mem_table->append_selective(*chunk, selection.data(), 0, left_channel_size));
                }
                if (hash_data.size() != left_channel_size) {
                    right_partition->num_rows += hash_data.size() - left_channel_size;
                    RETURN_IF_ERROR(right_mem_table->append_selective(*chunk, selection.data(), left_channel_size,
                                                                      hash_data.size() - left_channel_size));
                }
            }
            BREAK_IF_YIELD(yield_ctx.wg, &yield_ctx.need_yield, yield_ctx.time_spent_ns);
        }
        TRY_CATCH_ALLOC_SCOPE_END()
    }
    // TODO: add restore rows check
    return st;
}

Status PartitionedSpillerWriter::yieldable_flush_task(workgroup::YieldContext& ctx,
                                                      const std::vector<SpilledPartition*>& splitting_partitions,
                                                      const std::vector<SpilledPartition*>& spilling_partitions) {
    enum PartitionWriterStage { SPILL = 0, SPLIT = 1, FINISH = 2 };

    ctx.total_yield_point_cnt = FINISH;
    DCHECK(ctx.task_context_data.has_value()) << "spill flush context must be set";

    // partition memory usage
    // now we partitioned sorted spill
    SerdeContext spill_ctx;
    switch (ctx.yield_point) {
    case SPILL:
        RETURN_IF_ERROR(_spill_input_partitions(ctx, spill_ctx, spilling_partitions));
        RETURN_IF_YIELD(ctx.need_yield);
        TO_NEXT_STAGE(ctx.yield_point);
        [[fallthrough]];
    case SPLIT:
        RETURN_IF_ERROR(_split_input_partitions(ctx, spill_ctx, splitting_partitions));
        RETURN_IF_YIELD(ctx.need_yield);
        TO_NEXT_STAGE(ctx.yield_point);
        [[fallthrough]];
    default: {
        DCHECK_EQ(ctx.yield_point, 2);
    }
    }

    return Status::OK();
}

} // namespace starrocks::spill