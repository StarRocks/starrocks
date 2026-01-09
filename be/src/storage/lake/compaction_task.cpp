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
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/update_manager.h"

namespace starrocks::lake {

CompactionTask::CompactionTask(VersionedTablet tablet, std::vector<std::shared_ptr<Rowset>> input_rowsets,
                               CompactionTaskContext* context, std::shared_ptr<const TabletSchema> tablet_schema)
        : _txn_id(context->txn_id),
          _tablet(std::move(tablet)),
          _input_rowsets(std::move(input_rowsets)),
          _mem_tracker(std::make_unique<MemTracker>(MemTrackerType::COMPACTION_TASK, -1,
                                                    "Compaction-" + std::to_string(_tablet.metadata()->id()),
                                                    GlobalEnv::GetInstance()->compaction_mem_tracker())),
          _context(context),
          _tablet_schema(std::move(tablet_schema)) {}

Status CompactionTask::execute_index_major_compaction(TxnLogPB* txn_log) {
    if (_tablet.get_schema()->keys_type() == KeysType::PRIMARY_KEYS) {
        SCOPED_RAW_TIMER(&_context->stats->pk_sst_merge_ns);
        auto metadata = _tablet.metadata();
        if (metadata->enable_persistent_index() &&
            metadata->persistent_index_type() == PersistentIndexTypePB::CLOUD_NATIVE) {
            // For parallel compaction subtasks, skip SST compaction here.
            // SST compaction will be executed once after all subtasks complete,
            // in TabletParallelCompactionManager::get_merged_txn_log.
            // This avoids multiple subtasks competing to compact the same SST files.
            if (_context->subtask_id >= 0) {
                return Status::OK();
            }
            RETURN_IF_ERROR(_tablet.tablet_manager()->update_mgr()->execute_index_major_compaction(metadata, txn_log));
            if (txn_log->has_op_compaction() && !txn_log->op_compaction().input_sstables().empty()) {
                size_t total_input_sstable_file_size = 0;
                for (const auto& input_sstable : txn_log->op_compaction().input_sstables()) {
                    total_input_sstable_file_size += input_sstable.filesize();
                }
                _context->stats->input_file_size += total_input_sstable_file_size;
            }
            return Status::OK();
        }
    }
    return Status::OK();
}

Status CompactionTask::fill_compaction_segment_info(TxnLogPB_OpCompaction* op_compaction, TabletWriter* writer) {
    for (auto& rowset : _input_rowsets) {
        op_compaction->add_input_rowsets(rowset->id());
    }

    // check last rowset whether this is a partial compaction
    if (_tablet_schema->keys_type() != KeysType::PRIMARY_KEYS && _input_rowsets.size() > 0 &&
        _input_rowsets.back()->partial_segments_compaction()) {
        uint64_t uncompacted_num_rows = 0;
        uint64_t uncompacted_data_size = 0;
        RETURN_IF_ERROR(_input_rowsets.back()->add_partial_compaction_segments_info(
                op_compaction, writer, uncompacted_num_rows, uncompacted_data_size));
        op_compaction->mutable_output_rowset()->set_num_rows(writer->num_rows() + uncompacted_num_rows);
        op_compaction->mutable_output_rowset()->set_data_size(writer->data_size() + uncompacted_data_size);
        op_compaction->mutable_output_rowset()->set_overlapped(true);
    } else {
        op_compaction->set_new_segment_offset(0);
        for (const auto& file : writer->segments()) {
            op_compaction->mutable_output_rowset()->add_segments(file.path);
            op_compaction->mutable_output_rowset()->add_segment_size(file.size.value());
            op_compaction->mutable_output_rowset()->add_segment_encryption_metas(file.encryption_meta);
            auto* segment_meta = op_compaction->mutable_output_rowset()->add_segment_metas();
            file.sort_key_min.to_proto(segment_meta->mutable_sort_key_min());
            file.sort_key_max.to_proto(segment_meta->mutable_sort_key_max());
            segment_meta->set_num_rows(file.num_rows);
        }
        op_compaction->set_new_segment_count(writer->segments().size());
        op_compaction->mutable_output_rowset()->set_num_rows(writer->num_rows());
        op_compaction->mutable_output_rowset()->set_data_size(writer->data_size());
        op_compaction->mutable_output_rowset()->set_overlapped(false);
        op_compaction->mutable_output_rowset()->set_next_compaction_offset(0);
        for (auto& sst : writer->ssts()) {
            auto* file_meta = op_compaction->add_ssts();
            file_meta->set_name(sst.path);
            file_meta->set_size(sst.size.value());
            file_meta->set_encryption_meta(sst.encryption_meta);
        }
        for (auto& sst_range : writer->sst_ranges()) {
            op_compaction->add_sst_ranges()->CopyFrom(sst_range);
        }
    }
    return Status::OK();
}

bool CompactionTask::should_enable_pk_index_eager_build(int64_t input_bytes) {
    if (_tablet.get_schema()->keys_type() != KeysType::PRIMARY_KEYS) {
        return false;
    }
    // Eager PK index build only works when all conditions are met:
    // 1. whether use cloud native index
    // 2. whether use light compaction publish
    // 3. whether input_bytes is large enough
    auto metadata = _tablet.metadata();
    bool use_cloud_native_index = metadata->enable_persistent_index() &&
                                  metadata->persistent_index_type() == PersistentIndexTypePB::CLOUD_NATIVE;
    bool use_light_compaction_publish = config::enable_light_pk_compaction_publish &&
                                        StorageEngine::instance()->get_persistent_index_store(_tablet.id()) != nullptr;
    return use_cloud_native_index && use_light_compaction_publish &&
           input_bytes >= config::pk_index_eager_build_threshold_bytes;
}

} // namespace starrocks::lake
