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

#include "storage/lake/compaction_task.h"

#include "gen_cpp/lake_types.pb.h"
#include "runtime/exec_env.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_writer.h"

namespace starrocks::lake {

CompactionTask::CompactionTask(VersionedTablet tablet, std::vector<std::shared_ptr<Rowset>> input_rowsets,
                               CompactionTaskContext* context)
        : _txn_id(context->txn_id),
          _tablet(std::move(tablet)),
          _input_rowsets(std::move(input_rowsets)),
          _mem_tracker(std::make_unique<MemTracker>(MemTracker::COMPACTION, -1,
                                                    "Compaction-" + std::to_string(_tablet.metadata()->id()),
                                                    GlobalEnv::GetInstance()->compaction_mem_tracker())),
          _context(context) {}

Status CompactionTask::fill_compaction_segment_info(TxnLogPB_OpCompaction* op_compaction, TabletWriter* writer,
                                                    bool is_pk) {
    for (auto& rowset : _input_rowsets) {
        op_compaction->add_input_rowsets(rowset->id());
    }

    // check last rowset whether this is a partial compaction
    if (!is_pk && _input_rowsets.size() > 0 && _input_rowsets.back()->partial_segments_compaction()) {
        uint64_t uncompacted_num_rows = 0;
        uint64_t uncompacted_data_size = 0;
        RETURN_IF_ERROR(_input_rowsets.back()->add_partial_compaction_segments_info(
                op_compaction, writer, uncompacted_num_rows, uncompacted_data_size));
        op_compaction->mutable_output_rowset()->set_num_rows(writer->num_rows() + uncompacted_num_rows);
        op_compaction->mutable_output_rowset()->set_data_size(writer->data_size() + uncompacted_data_size);
        op_compaction->mutable_output_rowset()->set_overlapped(true);
    } else {
        for (auto& file : writer->files()) {
            op_compaction->mutable_output_rowset()->add_segments(file.path);
            op_compaction->mutable_output_rowset()->add_segment_size(file.size.value());
        }
        op_compaction->mutable_output_rowset()->set_num_rows(writer->num_rows());
        op_compaction->mutable_output_rowset()->set_data_size(writer->data_size());
        op_compaction->mutable_output_rowset()->set_overlapped(false);
        op_compaction->mutable_output_rowset()->set_next_compaction_offset(0);
    }
    return Status::OK();
}

} // namespace starrocks::lake
