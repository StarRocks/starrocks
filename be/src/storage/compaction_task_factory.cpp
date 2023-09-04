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

#include "storage/compaction_task_factory.h"

#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "storage/chunk_helper.h"
#include "storage/compaction_manager.h"
#include "storage/compaction_task.h"
#include "storage/compaction_utils.h"
#include "storage/horizontal_compaction_task.h"
#include "storage/tablet.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_reader_params.h"
#include "storage/vertical_compaction_task.h"

namespace starrocks {

std::shared_ptr<CompactionTask> CompactionTaskFactory::create_compaction_task() {
    auto iterator_num_res = Rowset::get_segment_num(_input_rowsets);
    if (!iterator_num_res.ok()) {
        LOG(WARNING) << "fail to get segment iterator num. tablet=" << _tablet->tablet_id()
                     << ", err=" << iterator_num_res.status().to_string();
        return nullptr;
    }
    size_t segment_iterator_num = iterator_num_res.value();
    size_t num_columns = _tablet->num_columns();
    CompactionAlgorithm algorithm = CompactionUtils::choose_compaction_algorithm(
            num_columns, config::vertical_compaction_max_columns_per_group, segment_iterator_num);
    std::shared_ptr<CompactionTask> compaction_task;
    VLOG(2) << "choose algorithm:" << CompactionUtils::compaction_algorithm_to_string(algorithm)
            << ", for tablet:" << _tablet->tablet_id() << ", segment_iterator_num:" << segment_iterator_num
            << ", max_columns_per_group:" << config::vertical_compaction_max_columns_per_group
            << ", num_columns:" << num_columns;
    if (algorithm == HORIZONTAL_COMPACTION) {
        compaction_task = std::make_shared<HorizontalCompactionTask>();
    } else if (algorithm == VERTICAL_COMPACTION) {
        compaction_task = std::make_shared<VerticalCompactionTask>();
    }
    // init the compaction task
    size_t input_rows_num = 0;
    size_t input_rowsets_size = 0;
    size_t input_segments_num = 0;
    for (auto& rowset : _input_rowsets) {
        input_rows_num += rowset->num_rows();
        input_rowsets_size += rowset->data_disk_size();
        input_segments_num += rowset->num_segments();
    }
    compaction_task->set_compaction_score(_compaction_score);
    compaction_task->set_compaction_type(_compaction_type);
    compaction_task->set_input_rows_num(input_rows_num);
    compaction_task->set_input_rowsets(std::move(_input_rowsets));
    compaction_task->set_input_rowsets_size(input_rowsets_size);
    compaction_task->set_input_segments_num(input_segments_num);
    compaction_task->set_output_version(_output_version);
    compaction_task->set_tablet(_tablet);
    compaction_task->set_segment_iterator_num(segment_iterator_num);
    std::unique_ptr<MemTracker> mem_tracker = std::make_unique<MemTracker>(
            MemTracker::COMPACTION, -1, "Compaction-" + std::to_string(compaction_task->task_id()),
            GlobalEnv::GetInstance()->compaction_mem_tracker());
    compaction_task->set_mem_tracker(mem_tracker.release());
    return compaction_task;
}

} // namespace starrocks
