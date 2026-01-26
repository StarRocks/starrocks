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

#include "storage/load_chunk_spiller.h"

#include "exec/spill/options.h"
#include "exec/spill/serde.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller_factory.h"
#include "runtime/runtime_state.h"
#include "storage/aggregate_iterator.h"
#include "storage/chunk_helper.h"
#include "storage/lake/tablet_internal_parallel_merge_task.h"
#include "storage/lake/tablet_writer.h"
#include "storage/load_spill_block_manager.h"
#include "storage/load_spill_pipeline_merge_context.h"
#include "storage/load_spill_pipeline_merge_iterator.h"
#include "storage/merge_iterator.h"
#include "storage/union_iterator.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

Status LoadSpillOutputDataStream::append(RuntimeState* state, const std::vector<Slice>& data, size_t total_write_size,
                                         size_t write_num_rows) {
    _append_rows += write_num_rows;
    size_t total_size = 0;
    // calculate total size
    std::for_each(data.begin(), data.end(), [&](const Slice& slice) { total_size += slice.size; });
    // preallocate block
    RETURN_IF_ERROR(_preallocate(total_size));
    // append data
    auto st = _block->append(data);
    if (st.is_capacity_limit_exceeded()) {
        // No space left on device
        // Try to acquire a new block from remote storage.
        RETURN_IF_ERROR(_switch_to_remote_block(total_size));
        st = _block->append(data);
    }
    if (st.ok()) {
        _append_bytes += total_size;
    }
    return st;
}

Status LoadSpillOutputDataStream::flush() {
    RETURN_IF_ERROR(_freeze_current_block());
    return Status::OK();
}

bool LoadSpillOutputDataStream::is_remote() const {
    return _block ? _block->is_remote() : false;
}

// this function will be called when local disk is full
Status LoadSpillOutputDataStream::_switch_to_remote_block(size_t block_size) {
    if (_block->size() > 0) {
        // Freeze current block firstly.
        RETURN_IF_ERROR(_freeze_current_block());
    } else {
        // Release empty block.
        RETURN_IF_ERROR(_block_manager->release_block(_block));
        _block = nullptr;
    }
    // Acquire new block.
    ASSIGN_OR_RETURN(_block, _block_manager->acquire_block(block_size, true /* force remote */));
    return Status::OK();
}

Status LoadSpillOutputDataStream::_freeze_current_block() {
    if (_block == nullptr) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_block->flush());
    if (_block->is_remote()) {
        // Update remote block write metric
        StarRocksMetrics::instance()->load_spill_remote_blocks_write_total.increment(1);
        StarRocksMetrics::instance()->load_spill_remote_bytes_write_total.increment(_block->size());
    } else {
        // Update local block write metric
        StarRocksMetrics::instance()->load_spill_local_blocks_write_total.increment(1);
        StarRocksMetrics::instance()->load_spill_local_bytes_write_total.increment(_block->size());
    }
    RETURN_IF_ERROR(_block_manager->release_block(_block));
    // Save this block into the block group, which is tagged with slot_idx for ordering
    _block_manager->block_container()->append_block(_block_group, _block);
    _block = nullptr;
    return Status::OK();
}

Status LoadSpillOutputDataStream::_preallocate(size_t block_size) {
    // Try to preallocate from current block first.
    if (_block == nullptr || !_block->try_acquire_sizes(block_size)) {
        // Freeze current block firstly.
        RETURN_IF_ERROR(_freeze_current_block());
        // Acquire new block.
        ASSIGN_OR_RETURN(_block, _block_manager->acquire_block(block_size));
    }
    return Status::OK();
}

LoadChunkSpiller::LoadChunkSpiller(LoadSpillBlockManager* block_manager, RuntimeProfile* profile,
                                   LoadSpillPipelineMergeContext* pipeline_merge_context)
        : _block_manager(block_manager), _profile(profile), _pipeline_merge_context(pipeline_merge_context) {
    if (_profile == nullptr) {
        // use dummy profile
        _dummy_profile = std::make_unique<RuntimeProfile>("dummy");
        _profile = _dummy_profile.get();
    }
    _runtime_state = std::make_shared<RuntimeState>();
    _spiller_factory = spill::make_spilled_factory();
}

// Spill a chunk to temporary storage for later merge
// @param chunk: data chunk to spill
// @param slot_idx: slot index assigned by flush token, used to track the original flush order.
//                  This is critical for maintaining data order during parallel flush:
//                  when multiple memtables flush concurrently, slot_idx preserves their
//                  original submission order for correct merge sequence.
StatusOr<size_t> LoadChunkSpiller::spill(const Chunk& chunk, int64_t slot_idx) {
    if (chunk.num_rows() == 0) {
        if (_pipeline_merge_context) {
            _pipeline_merge_context->mark_slot_ready(slot_idx);
        }
        return 0;
    }
    // 1. create new block group tagged with slot_idx
    auto block_group = _block_manager->block_container()->create_block_group(slot_idx);
    auto output = std::make_shared<LoadSpillOutputDataStream>(_block_manager, block_group);
    // 2. spill
    RETURN_IF_ERROR(_do_spill(chunk, output));
    // 3. flush
    RETURN_IF_ERROR(output->flush());
    if (_pipeline_merge_context) {
        _pipeline_merge_context->mark_slot_ready(slot_idx);
    }
    return output->append_bytes();
}

Status LoadChunkSpiller::_prepare(const ChunkPtr& chunk_ptr) {
    if (_spiller == nullptr) {
        // 1. alloc & prepare spiller
        spill::SpilledOptions options;
        options.encode_level = 7;
        options.wg = ExecEnv::GetInstance()->workgroup_manager()->get_default_workgroup();
        _spiller = _spiller_factory->create(options);
        RETURN_IF_ERROR(_spiller->prepare(_runtime_state.get()));
        DCHECK(_profile != nullptr) << "LoadChunkSpiller profile is null";
        spill::SpillProcessMetrics metrics(_profile, _runtime_state->mutable_total_spill_bytes());
        _spiller->set_metrics(metrics);
        // 2. prepare serde
        if (const_cast<spill::ChunkBuilder*>(&_spiller->chunk_builder())->chunk_schema()->empty()) {
            const_cast<spill::ChunkBuilder*>(&_spiller->chunk_builder())->chunk_schema()->set_schema(chunk_ptr);
            RETURN_IF_ERROR(_spiller->serde()->prepare());
        }
    }
    return Status::OK();
}

Status LoadChunkSpiller::_do_spill(const Chunk& chunk, const spill::SpillOutputDataStreamPtr& output) {
    // 1. caclulate per row memory usage
    const int64_t per_row_memory_usage = chunk.memory_usage() / chunk.num_rows();
    const int64_t spill_rows = std::min(config::load_spill_max_chunk_bytes / (per_row_memory_usage + 1) + 1,
                                        (int64_t)max_merge_chunk_size);
    // 2. serialize chunk
    for (int64_t rowid = 0; rowid < chunk.num_rows(); rowid += spill_rows) {
        int64_t rows = std::min(spill_rows, (int64_t)chunk.num_rows() - rowid);
        ChunkPtr each_chunk = chunk.clone_empty();
        each_chunk->append(chunk, rowid, rows);
        RETURN_IF_ERROR(_prepare(each_chunk));
        spill::SerdeContext ctx;
        RETURN_IF_ERROR(_spiller->serde()->serialize(_runtime_state.get(), ctx, each_chunk, output, true));
    }
    if (!_schema) {
        _schema = chunk.schema();
    }
    return Status::OK();
}

class BlockGroupIterator : public ChunkIterator {
public:
    BlockGroupIterator(Schema schema, spill::Serde& serde, const std::vector<spill::BlockPtr>& blocks)
            : ChunkIterator(std::move(schema)), _serde(serde), _blocks(blocks) {
        auto& metrics = _serde.parent()->metrics();
        _options.read_io_timer = metrics.read_io_timer;
        _options.read_io_count = metrics.read_io_count;
        _options.read_io_bytes = metrics.restore_bytes;
    }

    Status do_get_next(Chunk* chunk) override {
        while (_block_idx < _blocks.size()) {
            if (!_reader) {
                _reader = _blocks[_block_idx]->get_reader(_options);
                RETURN_IF_UNLIKELY(!_reader, Status::InternalError("Failed to get reader"));
                // Update block count metric
                auto& metrics = _serde.parent()->metrics();
                COUNTER_UPDATE(metrics.block_count, 1);
                if (_blocks[_block_idx]->is_remote()) {
                    COUNTER_UPDATE(metrics.remote_block_count, 1);
                    StarRocksMetrics::instance()->load_spill_remote_blocks_read_total.increment(1);
                    StarRocksMetrics::instance()->load_spill_remote_bytes_read_total.increment(
                            _blocks[_block_idx]->size());
                } else {
                    COUNTER_UPDATE(metrics.local_block_count, 1);
                    StarRocksMetrics::instance()->load_spill_local_blocks_read_total.increment(1);
                    StarRocksMetrics::instance()->load_spill_local_bytes_read_total.increment(
                            _blocks[_block_idx]->size());
                }
            }
            auto st = _serde.deserialize(_ctx, _reader.get());
            if (st.ok()) {
                st.value()->swap_chunk(*chunk);
                return Status::OK();
            } else if (st.status().is_end_of_file()) {
                _block_idx++;
                _reader.reset();
            } else {
                return st.status();
            }
        }
        return Status::EndOfFile("End of block group");
    }

    void close() override {}

private:
    spill::Serde& _serde;
    spill::SerdeContext _ctx;
    spill::BlockReaderOptions _options;
    std::shared_ptr<spill::BlockReader> _reader;
    const std::vector<spill::BlockPtr>& _blocks;
    size_t _block_idx = 0;
};

size_t LoadChunkSpiller::total_bytes() const {
    return _block_manager ? _block_manager->total_bytes() : 0;
}

StatusOr<SpillBlockInputTasks> LoadChunkSpiller::generate_spill_block_input_tasks(size_t target_size,
                                                                                  size_t memory_usage_per_merge,
                                                                                  bool do_sort, bool do_agg) {
    SpillBlockInputTasks result;
    auto& groups = _block_manager->block_container()->block_groups();
    RETURN_IF(groups.empty(), result);
    result.group_count = groups.size();
    std::vector<ChunkIteratorPtr> merge_inputs;
    size_t current_input_bytes = 0;
    // Sort groups by slot_idx to restore original memtable flush order
    // CORRECTNESS: When parallel flush is enabled, block groups are created out of order.
    // Sorting by slot_idx ensures we merge blocks in the same order as they were originally
    // flushed from memtables, which is critical for maintaining data consistency and version order.
    std::sort(groups.begin(), groups.end(),
              [](const BlockGroupPtrWithSlot& a, const BlockGroupPtrWithSlot& b) { return a.slot_idx < b.slot_idx; });
    for (auto& group : groups) {
        merge_inputs.push_back(
                std::make_shared<BlockGroupIterator>(*_schema, *_spiller->serde(), group.block_group->blocks()));
        current_input_bytes += group.block_group->data_size();
        result.total_block_bytes += group.block_group->data_size();
        result.total_blocks += group.block_group->blocks().size();
        // We need to stop merging if:
        // 1. The current input block group size exceed the target_size,
        //    because we don't want to generate too large segment file.
        // 2. The input chunks memory usage exceed the load_spill_memory_usage_per_merge,
        //    because we don't want each thread cost too much memory.
        if (merge_inputs.size() > 0 &&
            (current_input_bytes >= target_size ||
             merge_inputs.size() * config::load_spill_max_chunk_bytes >= memory_usage_per_merge)) {
            auto tmp_itr = do_sort ? new_heap_merge_iterator(merge_inputs) : new_union_iterator(merge_inputs);
            auto merge_itr = do_agg ? new_aggregate_iterator(tmp_itr) : tmp_itr;
            RETURN_IF_ERROR(merge_itr->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
            result.iterators.push_back(merge_itr);
            merge_inputs.clear();
            current_input_bytes = 0;
        }
    }
    if (!merge_inputs.empty()) {
        auto tmp_itr = do_sort ? new_heap_merge_iterator(merge_inputs) : new_union_iterator(merge_inputs);
        auto merge_itr = do_agg ? new_aggregate_iterator(tmp_itr) : tmp_itr;
        RETURN_IF_ERROR(merge_itr->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
        result.iterators.push_back(merge_itr);
    }
    LOG(INFO) << fmt::format(
            "LoadChunkSpiller generate_spill_block_input_tasks finished, load_id:{} fragment_instance_id:{} "
            "blockgroups:{} "
            "iterators:{} total_blocks:{} total_block_bytes:{}",
            (std::ostringstream() << _block_manager->load_id()).str(),
            (std::ostringstream() << _block_manager->fragment_instance_id()).str(), groups.size(),
            result.iterators.size(), result.total_blocks, result.total_block_bytes);
    return result;
}

Status LoadChunkSpiller::merge_write(size_t target_size, size_t memory_usage_per_merge, bool do_sort, bool do_agg,
                                     std::function<Status(Chunk*)> write_func, std::function<Status()> flush_func) {
    auto& groups = _block_manager->block_container()->block_groups();
    RETURN_IF(groups.empty(), Status::OK());

    MonotonicStopWatch timer;
    timer.start();
    size_t total_merges = 0;
    size_t total_rows = 0;
    size_t total_chunk = 0;

    std::vector<ChunkIteratorPtr> merge_inputs;
    auto merge_func = [&](const ChunkIteratorPtr& merge_itr) {
        total_merges++;
        auto chunk_shared_ptr = ChunkHelper::new_chunk(*_schema, config::vector_chunk_size);
        auto chunk = chunk_shared_ptr.get();
        while (true) {
            chunk->reset();
            auto st = merge_itr->get_next(chunk);
            if (st.is_end_of_file()) {
                break;
            } else if (st.ok()) {
                total_rows += chunk->num_rows();
                total_chunk++;
                RETURN_IF_ERROR(write_func(chunk));
            } else {
                return st;
            }
        }
        merge_itr->close();
        return flush_func();
    };
    ASSIGN_OR_RETURN(auto spill_block_iterator_tasks,
                     generate_spill_block_input_tasks(target_size, memory_usage_per_merge, do_sort, do_agg));
    for (const auto& itr : spill_block_iterator_tasks.iterators) {
        RETURN_IF_ERROR(merge_func(itr));
    }
    timer.stop();
    auto duration_ms = timer.elapsed_time() / 1000000;

    LOG(INFO) << fmt::format(
            "LoadChunkSpiller merge finished, load_id:{} fragment_instance_id:{} blockgroups:{} blocks:{} "
            "input_bytes:{} merges:{} rows:{} chunks:{} duration:{}ms",
            (std::ostringstream() << _block_manager->load_id()).str(),
            (std::ostringstream() << _block_manager->fragment_instance_id()).str(), groups.size(),
            spill_block_iterator_tasks.total_blocks, spill_block_iterator_tasks.total_block_bytes, total_merges,
            total_rows, total_chunk, duration_ms);
    COUNTER_UPDATE(ADD_COUNTER(_profile, "SpillMergeInputGroups", TUnit::UNIT), spill_block_iterator_tasks.group_count);
    COUNTER_UPDATE(ADD_COUNTER(_profile, "SpillMergeInputBytes", TUnit::BYTES),
                   spill_block_iterator_tasks.total_block_bytes);
    COUNTER_UPDATE(ADD_COUNTER(_profile, "SpillMergeCount", TUnit::UNIT), total_merges);
    return Status::OK();
}

bool LoadChunkSpiller::empty() {
    return _block_manager->block_container()->empty();
}

/**
 * Generates a single merge task for pipeline execution by pulling a batch of block groups.
 *
 * WHY THIS METHOD EXISTS: Unlike generate_spill_block_input_tasks() which creates ALL tasks
 * upfront (high memory overhead), this method generates ONE task at a time for lazy, on-demand
 * task creation. This enables the pipeline execution model where tasks are generated as
 * resources become available, providing better memory control and load balancing.
 *
 * KEY DIFFERENCES FROM BATCH GENERATION:
 * - Memory: Creates one task at a time vs all tasks upfront
 * - Ownership: Transfers block_group ownership to task (prevents premature destruction)
 * - Continuity: Ensures continuous slot_idx ranges (critical for correctness)
 * - Rounds: Supports both intermediate merges (final_round=false) and final merges (final_round=true)
 *
 * @param target_size - Max bytes per task (prevents oversized segment files)
 * @param memory_usage_per_merge - Memory budget per task (prevents OOM)
 * @param do_sort - Whether to use merge iterator (true) vs union iterator (false)
 * @param do_agg - Whether to apply aggregation (for AGG_KEYS/UNIQUE_KEYS tables)
 * @param final_round - If true, must consume ALL remaining blocks; if false, can stop at gaps
 * @return Task with merge iterator, or task with nullptr iterator if no blocks available
 */
StatusOr<LoadSpillPipelineMergeTaskPtr> LoadChunkSpiller::generate_pipeline_merge_task(size_t target_size,
                                                                                       size_t memory_usage_per_merge,
                                                                                       bool do_sort, bool do_agg,
                                                                                       bool final_round) {
    LoadSpillPipelineMergeTaskPtr result_task = std::make_unique<LoadSpillPipelineMergeTask>();

    // THREAD SAFETY: Lock held for entire operation because we're both reading and modifying
    // the block groups vector (sorting + erasing). Must be atomic to prevent race conditions
    // with concurrent flush operations adding new block groups.
    std::lock_guard<std::mutex> lg(*_block_manager->block_container()->block_groups_mutex());
    auto& groups = _block_manager->block_container()->block_groups();

    // Empty result signals iteration complete (iterator checks merge_itr == nullptr)
    RETURN_IF(groups.empty(), result_task);
    // Return error if _pipeline_merge_context is null
    RETURN_IF(_pipeline_merge_context == nullptr,
              Status::InternalError("LoadChunkSpiller pipeline merge context is null"));

    std::vector<ChunkIteratorPtr> merge_inputs;
    size_t current_input_bytes = 0;

    // Sort groups by slot_idx to restore original memtable flush order
    // CORRECTNESS: When parallel flush is enabled, block groups are created out of order.
    // Sorting by slot_idx ensures we merge blocks in the same order as they were originally
    // flushed from memtables, which is critical for maintaining data consistency and version order.
    std::sort(groups.begin(), groups.end(),
              [](const BlockGroupPtrWithSlot& a, const BlockGroupPtrWithSlot& b) { return a.slot_idx < b.slot_idx; });

    // Tracks the last group index included in this task (used for cleanup at end)
    int64_t stop_idx = -1;

    // Check previous slot id for continuity
    // Initialize to one less than the first slot to validate the first group in the continuity check.
    int64_t last_slot_idx = groups[0].slot_idx - 1;

    // BATCHING LOGIC: Accumulate continuous block groups until hitting size/memory limits
    for (size_t i = 0; i < groups.size(); i++) {
        auto& group = groups[i];

        // CONTINUITY CHECK: Ensure we only merge consecutive slot_idx ranges
        if (!final_round && !_pipeline_merge_context->is_slot_ready(last_slot_idx, group.slot_idx)) {
            break;
        }
        last_slot_idx = group.slot_idx;

        // Create iterator for this block group's data
        merge_inputs.push_back(
                std::make_shared<BlockGroupIterator>(*_schema, *_spiller->serde(), group.block_group->blocks()));
        current_input_bytes += group.block_group->data_size();
        result_task->total_block_bytes += group.block_group->data_size();
        result_task->total_block_groups++;

        // STOPPING CRITERIA: Balance between task size and parallelism.
        // Stop accumulating blocks when either:
        // 1. Data size limit reached (target_size) - prevents creating oversized segment files
        //    which can cause memory pressure and slow I/O during tablet writes
        // 2. Memory limit reached (memory_usage_per_merge) - prevents OOM from holding too
        //    many chunks in memory simultaneously during merge operation
        //
        // This batching strategy enables dynamic load balancing: many small tasks for high
        // parallelism vs fewer large tasks for better merge efficiency.
        if (merge_inputs.size() > 0 &&
            (current_input_bytes >= target_size ||
             merge_inputs.size() * config::load_spill_max_chunk_bytes >= memory_usage_per_merge)) {
            // Build the merge iterator chain based on table type requirements
            auto tmp_itr = do_sort ? new_heap_merge_iterator(merge_inputs) : new_union_iterator(merge_inputs);
            result_task->merge_itr = do_agg ? new_aggregate_iterator(tmp_itr) : tmp_itr;
            RETURN_IF_ERROR(result_task->merge_itr->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
            merge_inputs.clear();
            stop_idx = i;
            break;
        }
    }

    // FINAL ROUND SPECIAL HANDLING: In final merge, consume ALL remaining groups even if
    // they don't reach target_size. This ensures no orphaned blocks are left behind.
    // Non-final rounds can leave partial batches for next iteration.
    if (final_round && merge_inputs.size() > 0) {
        auto tmp_itr = do_sort ? new_heap_merge_iterator(merge_inputs) : new_union_iterator(merge_inputs);
        result_task->merge_itr = do_agg ? new_aggregate_iterator(tmp_itr) : tmp_itr;
        RETURN_IF_ERROR(result_task->merge_itr->init_encoded_schema(EMPTY_GLOBAL_DICTMAPS));
        merge_inputs.clear();
        stop_idx = groups.size() - 1;
    }

    // OWNERSHIP TRANSFER: Move block groups into task to prevent premature destruction.
    // The merge iterator (created above) needs these blocks to remain valid during async
    // task execution. By holding shared_ptr in result_task, we ensure blocks outlive iterator.
    for (int64_t i = 0; i <= stop_idx; i++) {
        result_task->block_groups.push_back(groups[i].block_group);
    }

    // CLEANUP: Remove processed groups from container so they won't be included in future
    // tasks. This is why we need the lock - concurrent access during erase would corrupt vector.
    groups.erase(groups.begin(), groups.begin() + stop_idx + 1);

    return result_task;
}

} // namespace starrocks
