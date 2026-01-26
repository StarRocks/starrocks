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

#include "storage/lake/tablet_internal_parallel_merge_task.h"

#include "exec/spill/options.h"
#include "exec/spill/serde.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller_factory.h"
#include "runtime/runtime_state.h"
#include "storage/aggregate_iterator.h"
#include "storage/chunk_helper.h"
#include "storage/lake/tablet_writer.h"
#include "storage/load_chunk_spiller.h"
#include "storage/load_spill_block_manager.h"
#include "storage/load_spill_pipeline_merge_iterator.h"
#include "storage/merge_iterator.h"
#include "storage/storage_engine.h"
#include "util/runtime_profile.h"

namespace starrocks::lake {

TabletInternalParallelMergeTask::TabletInternalParallelMergeTask(std::unique_ptr<TabletWriter> writer,
                                                                 std::unique_ptr<LoadSpillPipelineMergeTask> task,
                                                                 const Schema* schema, std::atomic<bool>* quit_flag,
                                                                 RuntimeProfile::Counter* write_io_timer)
        : _writer(std::move(writer)),
          _task(std::move(task)),
          _schema(schema),
          _quit_flag(quit_flag),
          _write_io_timer(write_io_timer) {
    std::string tracker_label =
            "LoadSpillMerge-" + std::to_string(_writer->tablet_id()) + "-" + std::to_string(_writer->txn_id());
    _merge_mem_tracker = std::make_unique<MemTracker>(MemTrackerType::COMPACTION_TASK, -1, std::move(tracker_label),
                                                      GlobalEnv::GetInstance()->compaction_mem_tracker());
}

TabletInternalParallelMergeTask::~TabletInternalParallelMergeTask() {
    if (_task->merge_itr != nullptr) {
        _task->merge_itr->close();
    }
}

void TabletInternalParallelMergeTask::run() {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_merge_mem_tracker.get(), false);
    MonotonicStopWatch timer;
    timer.start();
    auto char_field_indexes = ChunkHelper::get_char_field_indexes(*_schema);
    auto chunk_shared_ptr = ChunkHelper::new_chunk(*_schema, config::vector_chunk_size);
    auto chunk = chunk_shared_ptr.get();
    auto st = Status::OK();

    // CANCELLATION CHECK: Loop while quit flag is not set. The condition "_quit_flag == nullptr ||"
    // handles the case where quit_flag is nullptr (cancellation not supported). When nullptr,
    // we skip the quit check entirely and run to completion. When non-null, we check the
    // atomic flag on each iteration to support early termination on error or user cancellation.
    while (_quit_flag == nullptr || !_quit_flag->load()) {
        chunk->reset();
        auto itr_st = _task->merge_itr->get_next(chunk);
        if (itr_st.is_end_of_file()) {
            break;
        } else if (itr_st.ok()) {
            SCOPED_TIMER(_write_io_timer);
            ChunkHelper::padding_char_columns(char_field_indexes, *_schema, _writer->tablet_schema(), chunk);
            st = _writer->write(*chunk, nullptr);
            if (!st.ok()) {
                break;
            }
        } else {
            st = itr_st;
            break;
        }
    }
    if (st.ok()) {
        SCOPED_TIMER(_write_io_timer);
        st = _writer->flush();
    }
    if (st.ok()) {
        SCOPED_TIMER(_write_io_timer);
        st = _writer->finish();
    }
    // Release block groups to free up spill disk space
    _task->release_block_groups();
    timer.stop();
    LOG(INFO) << fmt::format(
            "SpillMemTableSink parallel merge blocks to segment finished, txn:{} tablet:{} "
            "total_block_groups: {}, total_block_bytes: {}, cost {} ms",
            _writer->txn_id(), _writer->tablet_id(), _task->total_block_groups, _task->total_block_bytes,
            timer.elapsed_time() / 1000000);
    update_status(st);
}

void TabletInternalParallelMergeTask::cancel() {
    update_status(Status::Cancelled("TabletInternalParallelMergeTask cancelled"));
}

void TabletInternalParallelMergeTask::update_status(const Status& st) {
    // Update task's status (Status::update is idempotent - first error wins)
    _status.update(st);

    // COOPERATIVE CANCELLATION: When one task fails, signal all other tasks to abort.
    // WHY: No point continuing other merges if one failed - the entire load will fail anyway.
    // This saves CPU/IO resources and provides faster failure detection. All tasks check
    // this flag in their run() loop and exit early when set.
    if (!st.ok() && _quit_flag != nullptr) {
        _quit_flag->store(true);
    }
}

} // namespace starrocks::lake