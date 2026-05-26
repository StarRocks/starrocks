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

#include <bvar/bvar.h>

#include "common/config_compaction_fwd.h"
#include "common/config_primary_key_fwd.h"
#include "gen_cpp/lake_types.pb.h"
#include "runtime/exec_env.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_reshard_helper.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/update_manager.h"
#include "storage/storage_metrics.h"

namespace starrocks::lake {

// Distribution of output segment file sizes (bytes) produced by lake compaction. Exposed via
// the BE metrics endpoint as gauge series (count, percentiles, max) under this name.
static bvar::LatencyRecorder g_lake_compaction_output_segment_size_bytes("lake_compaction_output_segment_size_bytes");

int64_t lake_compaction_output_segment_size_recorded_count() {
    return g_lake_compaction_output_segment_size_bytes.count();
}

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
            uint32_t segment_idx = op_compaction->output_rowset().segment_metas_size();
            file.to_proto(segment_idx, op_compaction->mutable_output_rowset()->add_segment_metas());
        }
        op_compaction->set_new_segment_count(writer->segments().size());
        op_compaction->mutable_output_rowset()->set_num_rows(writer->num_rows());
        op_compaction->mutable_output_rowset()->set_data_size(writer->data_size());
        op_compaction->mutable_output_rowset()->set_overlapped(false);
        op_compaction->mutable_output_rowset()->set_next_compaction_offset(0);
        for (auto& sst : writer->ssts()) {
            to_file_meta_pb(sst, op_compaction->add_ssts());
        }
        for (auto& sst_range : writer->sst_ranges()) {
            op_compaction->add_sst_ranges()->CopyFrom(sst_range);
        }
        // Record lcrm file metadata in transaction log if it exists
        // WHY: During parallel pk index execution, mapper files are stored on remote storage
        // (.lcrm extension). Recording metadata in txn log enables:
        // 1. Light publish optimization - skip re-reading compaction data during publish
        // 2. Performance - file size avoids expensive S3/HDFS get_size() calls
        // 3. Lifecycle management - metadata tracked for proper GC cleanup
        // CONSTRAINT: Only applies to remote storage files (.lcrm), not local files (.crm)
        if (is_lcrm(writer->lcrm_file().path)) {
            to_file_meta_pb(writer->lcrm_file(), op_compaction->mutable_lcrm_file());
        }
    }
    // Fresh uid: a compaction output is a new logical rowset and must not dedup
    // with the input rowsets it supersedes (it leaves their split family). Use
    // set_ (always re-mint), not ensure_ (mint-if-absent): the output is derived
    // from inputs, so it must never alias an input's uid even if one is ever
    // copied in. Matches tablet_parallel_compaction_manager's merged output.
    tablet_reshard_helper::set_rowset_uid(op_compaction->mutable_output_rowset());

    // Observability: record output segment sizes and count the object-storage PUT calls this
    // compaction issued. Each newly written file (segment, sst, .lcrm) maps to one PUT here;
    // the txn log PUT is counted at the put_txn_log call site in the concrete tasks. These are
    // the files the writer actually produced, so this is correct for both the full-compaction
    // and partial-compaction branches above.
    int64_t put_count = 0;
    for (const auto& file : writer->segments()) {
        if (file.size.has_value()) {
            g_lake_compaction_output_segment_size_bytes << file.size.value();
        }
        ++put_count;
    }
    put_count += static_cast<int64_t>(writer->ssts().size());
    if (is_lcrm(writer->lcrm_file().path)) {
        ++put_count;
    }
    StorageMetrics::instance()->lake_compaction_s3_put_count.increment(put_count);
    return Status::OK();
}

CompactionTask::SstStats CompactionTask::compute_sst_stats(const std::vector<FileInfo>& writer_ssts,
                                                           const TxnLogPB* txn_log) {
    SstStats stats;
    // SST output from eager PK index build
    stats.output_files = static_cast<int32_t>(writer_ssts.size());
    for (const auto& sst : writer_ssts) {
        stats.output_bytes += sst.size.value_or(0);
    }
    // SST input/output from PK index major compaction
    if (txn_log != nullptr && txn_log->has_op_compaction()) {
        const auto& op = txn_log->op_compaction();
        stats.input_files = op.input_sstables_size();
        for (const auto& sst : op.input_sstables()) {
            stats.input_bytes += sst.filesize();
        }
        for (const auto& sst : op.output_sstables()) {
            stats.output_files += 1;
            stats.output_bytes += sst.filesize();
        }
        if (op.has_output_sstable()) {
            stats.output_files += 1;
            stats.output_bytes += op.output_sstable().filesize();
        }
    }
    return stats;
}

void CompactionTask::collect_sst_stats(const TabletWriter* writer, const TxnLogPB* txn_log) {
    auto stats = compute_sst_stats(writer->ssts(), txn_log);
    _sst_input_files = stats.input_files;
    _sst_input_bytes = stats.input_bytes;
    _sst_output_files = stats.output_files;
    _sst_output_bytes = stats.output_bytes;
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
