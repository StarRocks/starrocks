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

#include "storage/load_spill_pipeline_merge_iterator.h"

#include "exec/spill/spiller.h"
#include "exec/spill/spiller_factory.h"
#include "runtime/runtime_state.h"
#include "storage/lake/tablet_internal_parallel_merge_task.h"
#include "storage/lake/tablet_writer.h"
#include "storage/load_chunk_spiller.h"
#include "storage/load_spill_block_manager.h"
#include "storage/merge_iterator.h"
#include "storage/union_iterator.h"

namespace starrocks {

LoadSpillPipelineMergeIterator::LoadSpillPipelineMergeIterator(LoadChunkSpiller* spiller,
                                                               lake::TabletWriter* parent_writer,
                                                               std::atomic<bool>* quit_flag, bool final_round)
        : _spiller(spiller), _parent_writer(parent_writer), _quit_flag(quit_flag), _final_round(final_round) {
    SchemaPtr schema = _spiller->schema();

    // PERFORMANCE OPTIMIZATION: DUP_KEYS tables don't require sorting or aggregation
    // during merge, so we can use a simpler union iterator instead of merge iterator.
    // This saves significant CPU time (no comparisons) and memory (no aggregation state).
    // For AGG_KEYS/UNIQUE_KEYS tables, we must perform sorted merge with aggregation
    // to maintain correctness (combine duplicate keys, apply aggregate functions).
    if (schema->keys_type() == KeysType::DUP_KEYS) {
        _do_agg = false;
    } else {
        _do_agg = true;
    }
}

void LoadSpillPipelineMergeIterator::_next() {
    // Simple forwarding wrapper to allow public API (init/next) to remain clean
    // while keeping actual generation logic in a separate Status-returning method
    _status = _generate_next_task();
}

Status LoadSpillPipelineMergeIterator::_generate_next_task() {
    // Pull next batch of block groups from spiller and create merge task.
    // PARAMETERS EXPLAINED:
    // - config::load_spill_max_merge_bytes: Max total bytes per task (load balancing)
    // - config::load_spill_memory_usage_per_merge: Memory budget for merge operation
    // - true (sort): Whether to sort during merge (always true for correctness)
    // - _do_agg: Whether to perform aggregation (depends on table keys type)
    // - _final_round: Whether this merges to final tablet vs intermediate blocks
    ASSIGN_OR_RETURN(auto task, _spiller->generate_pipeline_merge_task(config::load_spill_max_merge_bytes,
                                                                       config::load_spill_memory_usage_per_merge,
                                                                       true /*sort*/, _do_agg, _final_round));

    // nullptr merge_itr indicates no more block groups available - iteration complete
    if (task->merge_itr != nullptr) {
        // Update metrics for monitoring merge workload and performance analysis.
        // These counters help identify bottlenecks (too many small merges vs few large ones)
        // and validate that workload is being distributed evenly across pipeline tasks.
        COUNTER_UPDATE(ADD_COUNTER(_spiller->profile(), "SpillMergeInputGroups", TUnit::UNIT),
                       task->total_block_groups);
        COUNTER_UPDATE(ADD_COUNTER(_spiller->profile(), "SpillMergeInputBytes", TUnit::BYTES), task->total_block_bytes);
        COUNTER_UPDATE(ADD_COUNTER(_spiller->profile(), "SpillMergeCount", TUnit::UNIT), 1);

        // WHY CLONE WRITER: Each parallel merge task needs its own writer instance to
        // avoid lock contention and enable true parallel writes. The cloned writers
        // maintain independent file handles and buffers. Results are merged back to
        // parent writer later via merge_other_writer() in the merge context.
        ASSIGN_OR_RETURN(auto writer, _parent_writer->clone());

        // Create the parallel merge task with all necessary context: writer clone,
        // merge iterator, schema, quit flag for cancellation, and I/O metrics timer.
        _current_task = std::make_shared<lake::TabletInternalParallelMergeTask>(
                std::move(writer), std::move(task), _spiller->_schema.get(), _quit_flag,
                _spiller->spiller()->metrics().write_io_timer);
    } else {
        // No more data to merge - signal iteration completion by returning nullptr.
        // Pipeline operators check has_more() which tests for nullptr to know when to stop.
        _current_task = nullptr;
    }
    return Status::OK();
}

} // namespace starrocks