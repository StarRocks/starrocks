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

<<<<<<< HEAD
=======
int64_t CompactionTask::stream_buffer_size(int64_t num_streams) {
    const int64_t configured = config::lake_compaction_stream_buffer_size_bytes;
    const int64_t mem_limit = config::compaction_memory_limit_per_worker;
    if (configured <= kMinStreamBufferSize || mem_limit <= 0 || num_streams <= 0) {
        return configured;
    }
    return std::clamp<int64_t>(mem_limit / 2 / num_streams, kMinStreamBufferSize, configured);
}

int64_t CompactionTask::stream_buffer_size_for_pass(int64_t num_streams) {
    const int64_t buffer_size = stream_buffer_size(num_streams);
    if (buffer_size < config::lake_compaction_stream_buffer_size_bytes) {
        _context->stats->stream_buffer_shrunk_passes++;
        VLOG(1) << "Compaction read buffer shrunk. tablet: " << _tablet.id() << ", txn: " << _txn_id
                << ", streams: " << num_streams << ", buffer: " << buffer_size
                << ", configured: " << config::lake_compaction_stream_buffer_size_bytes;
    }
    return buffer_size;
}

int32_t CompactionTask::chunk_size_with_held_segments(int64_t held_segments_bytes, int64_t total_num_rows,
                                                      int64_t total_mem_footprint, size_t source_num,
                                                      int64_t stream_buffer_bytes) {
    // The read buffers are allocated whatever the chunk size is, so they come off the budget first.
    // stream_buffer_size() keeps them within half of it unless its floor kicks in; even then leave
    // the chunk a quarter, so a huge fan-in degrades to small chunks rather than one-row reads.
    const int64_t worker_limit = config::compaction_memory_limit_per_worker;
    const int64_t mem_limit =
            worker_limit <= 0 ? worker_limit : std::max(worker_limit - stream_buffer_bytes, worker_limit / 4);
    const int32_t config_chunk_size = config::lake_compaction_chunk_size;
    // Baseline for judging how much holding shrinks the read chunk. A non-positive limit means
    // "no memory cap", even when segment metadata remains resident.
    const int32_t unheld_chunk_size = CompactionUtils::get_read_chunk_size(mem_limit, config_chunk_size, total_num_rows,
                                                                           total_mem_footprint, source_num);
    if (mem_limit <= 0) {
        return unheld_chunk_size;
    }

    const auto chunk_size_for_resident_segments = [&](int64_t resident_bytes) -> int32_t {
        const int64_t remaining = mem_limit - resident_bytes;
        // A memo can retain segments even after holding is disabled. An exhausted budget must
        // produce the smallest read chunk, not the unlimited sizing a non-positive limit selects.
        return remaining > 0 ? CompactionUtils::get_read_chunk_size(remaining, config_chunk_size, total_num_rows,
                                                                    total_mem_footprint, source_num)
                             : 1;
    };
    if (!_hold_input_segments) {
        return chunk_size_for_resident_segments(held_segments_bytes);
    }

    const int32_t held_chunk_size = chunk_size_for_resident_segments(held_segments_bytes);
    // Holding buys one segment load for the whole task; it is not worth an order-of-magnitude
    // smaller read chunk (that many more iterations, and at the floor a single row per read).
    if (held_chunk_size >= std::max<int32_t>(2, unheld_chunk_size / kMaxHeldChunkShrink)) {
        return held_chunk_size;
    }
    LOG(WARNING) << "Compaction input segments do not leave a workable read budget, falling back to the metadata "
                    "cache. tablet: "
                 << _tablet.id() << ", txn: " << _txn_id << ", held bytes: " << held_segments_bytes
                 << ", budget: " << mem_limit << ", chunk size held/unheld: " << held_chunk_size << "/"
                 << unheld_chunk_size;
    for (auto& rowset : _input_rowsets) {
        rowset->release_held_segments();
    }
    _hold_input_segments = false;
    // get_segments_checked() may still own the same segments for flat-JSON inspection. Only the
    // bytes actually released are available to the read buffers, both now and on later passes.
    int64_t retained_segments_bytes = 0;
    for (const auto& rowset : _input_rowsets) {
        retained_segments_bytes += rowset->held_segments_bytes();
    }
    return chunk_size_for_resident_segments(retained_segments_bytes);
}

>>>>>>> 80ca09f ([BugFix] Bound lake compaction read buffers by the per-worker memory budget (#80229))
Status CompactionTask::execute_index_major_compaction(TxnLogPB* txn_log) {
    if (_tablet.get_schema()->keys_type() == KeysType::PRIMARY_KEYS) {
        SCOPED_RAW_TIMER(&_context->stats->pk_sst_merge_ns);
        auto metadata = _tablet.metadata();
        if (metadata->enable_persistent_index() &&
            metadata->persistent_index_type() == PersistentIndexTypePB::CLOUD_NATIVE) {
            RETURN_IF_ERROR(_tablet.tablet_manager()->update_mgr()->execute_index_major_compaction(*metadata, txn_log));
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
        for (auto& file : writer->files()) {
            op_compaction->mutable_output_rowset()->add_segments(file.path);
            op_compaction->mutable_output_rowset()->add_segment_size(file.size.value());
            op_compaction->mutable_output_rowset()->add_segment_encryption_metas(file.encryption_meta);
        }
        op_compaction->set_new_segment_count(writer->files().size());
        op_compaction->mutable_output_rowset()->set_num_rows(writer->num_rows());
        op_compaction->mutable_output_rowset()->set_data_size(writer->data_size());
        op_compaction->mutable_output_rowset()->set_overlapped(false);
        op_compaction->mutable_output_rowset()->set_next_compaction_offset(0);
    }
    return Status::OK();
}

} // namespace starrocks::lake
