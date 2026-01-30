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

#include "storage/lake/spill_mem_table_sink.h"

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
#include "storage/storage_engine.h"
#include "util/runtime_profile.h"

namespace starrocks::lake {

SpillMemTableSink::SpillMemTableSink(LoadSpillBlockManager* block_manager, TabletWriter* writer,
                                     RuntimeProfile* profile)
        : _writer(writer),
          _pipeline_merge_context(std::make_unique<LoadSpillPipelineMergeContext>(_writer)),
          _load_chunk_spiller(
                  std::make_unique<LoadChunkSpiller>(block_manager, profile, _pipeline_merge_context.get())) {
    std::string tracker_label =
            "LoadSpillMerge-" + std::to_string(writer->tablet_id()) + "-" + std::to_string(writer->txn_id());
    _merge_mem_tracker = std::make_unique<MemTracker>(MemTrackerType::COMPACTION_TASK, -1, std::move(tracker_label),
                                                      GlobalEnv::GetInstance()->compaction_mem_tracker());
}

SpillMemTableSink::~SpillMemTableSink() = default;

Status SpillMemTableSink::flush_chunk(const Chunk& chunk, starrocks::SegmentPB* segment, bool eos,
                                      int64_t* flush_data_size, int64_t slot_idx) {
    if (eos && _load_chunk_spiller->empty() && slot_idx == 0) {
        // Optimization: If there is only one flush, write directly to segment without spilling
        // This avoids the overhead of spill/merge for single-chunk loads
        RETURN_IF_ERROR(_writer->write(chunk, segment, eos));
        return _writer->flush(segment);
    }

    // Spill chunk to temporary storage with slot_idx for ordering
    auto res = _load_chunk_spiller->spill(chunk, slot_idx);
    RETURN_IF_ERROR(res.status());
    // record append bytes to `flush_data_size`
    if (flush_data_size != nullptr) {
        *flush_data_size = res.value();
    }

    // EAGER MERGE OPTIMIZATION: When spilled data accumulates to a threshold, proactively
    // start merging blocks in the background BEFORE all flushes complete. This provides:
    // 1. Better parallelism - merge tasks run concurrently with ongoing memtable flushes
    // 2. Reduced final merge time - much of the merge work completes during the load phase
    // 3. Lower memory pressure - blocks get merged and reclaimed earlier
    //
    // WHY THIS THRESHOLD: pk_index_eager_build_threshold_bytes indicates bulk load scenario
    // where parallel merge benefits outweigh task coordination overhead.
    if (_load_chunk_spiller->total_bytes() >= config::pk_index_eager_build_threshold_bytes &&
        config::enable_load_spill_parallel_merge) {
        // Disable auto-flush to manually control segment finalization timing
        _writer->set_auto_flush(false);

        // For PK tables in bulk load, eagerly build primary key index during merge
        // instead of waiting until commit. This parallelizes expensive index construction.
        _writer->try_enable_pk_index_eager_build();

        // Lazy initialization: create thread pool token only when first needed
        _pipeline_merge_context->create_thread_pool_token();
        if (!_pipeline_merge_context->token()) {
            // Thread pool exhausted - cannot submit merge tasks now
            // Skip eager merge for this flush
            return Status::OK();
        }

        // Generate ONE merge task eagerly (not all tasks). This allows pipeline execution
        // where merge tasks are generated and submitted incrementally as resources allow,
        // rather than all upfront which would consume excessive memory.
        // final_round=false means this merges to intermediate blocks, not final tablet.
        LoadSpillPipelineMergeIterator task_iterator(_load_chunk_spiller.get(), _writer,
                                                     _pipeline_merge_context->quit_flag(), false /* final_round */);
        task_iterator.init();
        if (task_iterator.has_more()) {
            auto current_task = task_iterator.current_task();
            _pipeline_merge_context->add_merge_task(current_task);
            auto submit_st = _pipeline_merge_context->token()->submit(current_task);
            if (!submit_st.ok()) {
                // Submit failure doesn't fail the flush - task will report error when checked later
                current_task->update_status(submit_st);
            }
        }
        RETURN_IF_ERROR(task_iterator.status());
    }
    return Status::OK();
}

Status SpillMemTableSink::flush_chunk_with_deletes(const Chunk& upserts, const Column& deletes,
                                                   starrocks::SegmentPB* segment, bool eos, int64_t* flush_data_size,
                                                   int64_t slot_idx) {
    if (eos && _load_chunk_spiller->empty() && slot_idx == 0) {
        // If there is only one flush, flush it to segment directly
        RETURN_IF_ERROR(_writer->flush_del_file(deletes));
        RETURN_IF_ERROR(_writer->write(upserts, segment, eos));
        return _writer->flush(segment);
    }
    // 1. flush upsert
    RETURN_IF_ERROR(flush_chunk(upserts, segment, eos, flush_data_size, slot_idx));
    // 2. flush deletes
    RETURN_IF_ERROR(_writer->flush_del_file(deletes));
    return Status::OK();
}

Status SpillMemTableSink::merge_blocks_to_segments() {
    TEST_SYNC_POINT_CALLBACK("SpillMemTableSink::merge_blocks_to_segments", this);
    SCOPED_THREAD_LOCAL_MEM_SETTER(_merge_mem_tracker.get(), false);
    RETURN_IF(_load_chunk_spiller->empty(), Status::OK());

    // Manual flush control needed because we're coordinating multiple parallel writers
    _writer->set_auto_flush(false);

    if (_load_chunk_spiller->total_bytes() >= config::pk_index_eager_build_threshold_bytes) {
        // When bulk load happens, try to enable eager PK index build
        _writer->try_enable_pk_index_eager_build();
    }

    SCOPED_TIMER(ADD_TIMER(_load_chunk_spiller->profile(), "SpillMergeTime"));

    // Lazy token creation: may already exist from eager merge in flush_chunk()
    if (config::enable_load_spill_parallel_merge) {
        _pipeline_merge_context->create_thread_pool_token();
    }

    // FINAL MERGE PHASE: Merge all remaining spilled blocks to final tablet segments.
    // final_round=true ensures ALL remaining blocks are consumed (no partial batches left).
    // This iterator generates tasks lazily - one at a time as we iterate, enabling
    // dynamic load balancing and memory control.
    LoadSpillPipelineMergeIterator task_iterator(_load_chunk_spiller.get(), _writer,
                                                 _pipeline_merge_context->quit_flag(), true /* final_round */);

    // PIPELINE EXECUTION MODEL: Generate tasks on-demand and submit for parallel execution.
    // Each task processes a batch of block groups (up to load_spill_max_merge_bytes).
    // This approach balances parallelism (multiple tasks running concurrently) with
    // memory efficiency (not creating all tasks upfront).
    for (task_iterator.init(); task_iterator.has_more(); task_iterator.next()) {
        auto current_task = task_iterator.current_task();
        _pipeline_merge_context->add_merge_task(current_task);

        if (_pipeline_merge_context->token()) {
            // PARALLEL PATH: Submit to thread pool for async execution
            auto submit_st = _pipeline_merge_context->token()->submit(current_task);
            if (!submit_st.ok()) {
                current_task->update_status(submit_st);
                break; // Stop submitting new tasks if thread pool is unavailable
            }
        } else {
            // SERIAL PATH: Fallback when parallel merge is disabled or token unavailable.
            // Executes task synchronously in current thread. This ensures correctness
            // even if thread pool is exhausted or config disables parallelism.
            current_task->run();
            if (!current_task->status().ok()) {
                break; // Stop on first error
            }
        }
    }

    // RESULT CONSOLIDATION: Check all task statuses and merge their tablet writer results
    // into the parent writer. This is the critical phase that combines all parallel work.
    // Any task failure here will cause the entire load to fail.
    RETURN_IF_ERROR(_pipeline_merge_context->merge_task_results());

    // Return iterator status to catch any errors during task generation
    return task_iterator.status();
}

int64_t SpillMemTableSink::txn_id() {
    return _writer->txn_id();
}

int64_t SpillMemTableSink::tablet_id() {
    return _writer->tablet_id();
}

} // namespace starrocks::lake
