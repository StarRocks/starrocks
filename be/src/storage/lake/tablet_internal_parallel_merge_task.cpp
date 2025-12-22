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
#include "storage/load_spill_block_manager.h"
#include "storage/merge_iterator.h"
#include "storage/storage_engine.h"
#include "util/runtime_profile.h"

namespace starrocks::lake {

TabletInternalParallelMergeTask::TabletInternalParallelMergeTask(TabletWriter* writer, ChunkIterator* block_iterator,
                                                                 MemTracker* merge_mem_tracker, Schema* schema,
                                                                 int32_t task_index, QuitFlag* quit_flag)
        : _writer(writer),
          _block_iterator(block_iterator),
          _merge_mem_tracker(merge_mem_tracker),
          _schema(schema),
          _task_index(task_index),
          _quit_flag(quit_flag) {}

TabletInternalParallelMergeTask::~TabletInternalParallelMergeTask() {
    if (_block_iterator != nullptr) {
        _block_iterator->close();
    }
}

void TabletInternalParallelMergeTask::run() {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_merge_mem_tracker, false);
    MonotonicStopWatch timer;
    timer.start();
    auto char_field_indexes = ChunkHelper::get_char_field_indexes(*_schema);
    auto chunk_shared_ptr = ChunkHelper::new_chunk(*_schema, config::vector_chunk_size);
    auto chunk = chunk_shared_ptr.get();
    auto st = Status::OK();
    while (!_quit_flag->quit.load()) {
        chunk->reset();
        auto itr_st = _block_iterator->get_next(chunk);
        if (itr_st.is_end_of_file()) {
            break;
        } else if (itr_st.ok()) {
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
        st = _writer->flush();
    }
    if (st.ok()) {
        st = _writer->finish();
    }
    timer.stop();
    LOG(INFO) << fmt::format(
            "SpillMemTableSink parallel merge blocks to segment finished, txn:{} tablet:{} "
            "task_index:{}, cost {} ms",
            _writer->txn_id(), _writer->tablet_id(), _task_index, timer.elapsed_time() / 1000000);
    update_status(st);
}

void TabletInternalParallelMergeTask::cancel() {
    update_status(Status::Cancelled("TabletInternalParallelMergeTask cancelled"));
}

void TabletInternalParallelMergeTask::update_status(const Status& st) {
    _status.update(st);
    if (!st.ok() && _quit_flag != nullptr) {
        // Notify other tasks to quit
        _quit_flag->quit.store(true);
    }
}

} // namespace starrocks::lake