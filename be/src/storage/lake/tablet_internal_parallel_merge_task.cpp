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

#include "column/chunk_factory.h"
#include "column/chunk_schema_helper.h"
#include "column/raw_data_visitor.h"
#include "common/config_exec_fwd.h"
#include "common/runtime_profile.h"
#include "compute_env/load_spill/load_spill_merge_input_batch.h"
#include "compute_env/spill/block_group.h"
#include "gen_cpp/Types_types.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_env.h"
#include "storage/chunk_helper.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/vacuum.h"
#include "storage_primitive/chunk_iterator.h"
#include "storage_primitive/primary_key_encoder.h"

namespace starrocks::lake {

TabletInternalParallelMergeTask::TabletInternalParallelMergeTask(std::unique_ptr<TabletWriter> writer,
                                                                 std::unique_ptr<LoadSpillMergeInputBatch> task,
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
                                                      RuntimeEnv::GetInstance()->compaction_mem_tracker());
}

TabletInternalParallelMergeTask::~TabletInternalParallelMergeTask() {
    if (_task->merge_itr != nullptr) {
        _task->merge_itr->close();
    }
}

namespace {

// Write one merged chunk. Plain (no __op) merge: write the chunk as a segment. Op-aware merge
// (trailing __op column, REPLACE-aggregated so the latest op per key wins): split into upsert rows
// (-> a segment, __op dropped) and net-deleted keys (accumulated into `deletes` for this batch's del
// file); set `wrote_upsert` when an upsert segment was produced. `merge_schema` carries __op (used by
// the PK encoder, which reads only the leading key columns); `write_schema` is the segment schema.
Status write_one_merged_chunk(TabletWriter* writer, Chunk* chunk, bool has_op, const Schema& merge_schema,
                              const Schema& write_schema, const std::vector<size_t>& char_field_indexes,
                              PrimaryKeyEncodingType pk_enc, MutableColumnPtr* deletes, bool* wrote_upsert) {
    if (!has_op) {
        ChunkHelper::padding_char_columns(char_field_indexes, write_schema, writer->tablet_schema(), chunk);
        return writer->write(*chunk, nullptr);
    }
    // Split the merged chunk by __op (last column).
    const size_t op_idx = chunk->num_columns() - 1;
    RawDataVisitor visitor;
    RETURN_IF_ERROR(chunk->get_column_by_index(op_idx)->accept(&visitor));
    const auto* ops = visitor.result();
    const size_t nrows = chunk->num_rows();
    std::vector<uint32_t> up_idx;
    std::vector<uint32_t> del_idx;
    up_idx.reserve(nrows);
    for (uint32_t i = 0; i < nrows; i++) {
        (ops[i] == TOpType::UPSERT ? up_idx : del_idx).push_back(i);
    }
    // Encode net-deleted keys.
    if (!del_idx.empty()) {
        if (*deletes == nullptr) {
            RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(merge_schema, deletes, pk_enc));
        }
        PrimaryKeyEncoder::encode_selective(merge_schema, *chunk, del_idx.data(), del_idx.size(), deletes->get(),
                                            pk_enc);
    }
    // Write upsert rows as a chunk without __op. Copy the upsert rows into a temp chunk (Chunk-level
    // append_selective handles the immutable columns), then repackage its leading data columns into a
    // segment chunk backed by a FRESH copy of write_schema. Do NOT clone-then-remove_column_by_index:
    // clone_empty_with_schema shares the SchemaPtr and remove_column_by_index mutates it in place, which
    // would shrink the reused loop chunk's shared schema and corrupt the next iteration's op split.
    if (!up_idx.empty()) {
        auto tmp = chunk->clone_empty_with_schema(up_idx.size());
        tmp->append_selective(*chunk, up_idx.data(), 0, up_idx.size());
        Columns up_cols(tmp->columns().begin(), tmp->columns().begin() + write_schema.num_fields()); // drop __op
        auto up_chunk = std::make_shared<Chunk>(std::move(up_cols), std::make_shared<Schema>(write_schema));
        ChunkHelper::padding_char_columns(char_field_indexes, write_schema, writer->tablet_schema(), up_chunk.get());
        RETURN_IF_ERROR(writer->write(*up_chunk, nullptr));
        *wrote_upsert = true;
    }
    return Status::OK();
}

// After the merge loop: flush this batch's segments and (for an op-aware batch with net-deletes) its del
// file, then finish the writer. A net-delete-only batch (deletes but no upsert segment) first writes a
// 0-row segment so its del file anchors at a real segment index (assigned in merge_other_writer) instead
// of colliding with a later batch's segment 0 -- which would let the delete (reserved UINT32_MAX rowid)
// sort after, and wrongly erase, a key re-upserted in that later segment. Mirrors the serial path, which
// writes a 0-row segment for a delete-only flush.
Status finalize_merged_batch(TabletWriter* writer, bool has_op, const Schema& write_schema,
                             const MutableColumnPtr& deletes, bool wrote_upsert,
                             RuntimeProfile::Counter* write_io_timer) {
    const bool has_deletes = has_op && deletes != nullptr && deletes->size() > 0;
    if (has_deletes && !wrote_upsert) {
        SCOPED_TIMER(write_io_timer);
        auto empty_chunk = ChunkFactory::new_chunk(write_schema, 0);
        RETURN_IF_ERROR(writer->write(*empty_chunk, nullptr));
    }
    {
        SCOPED_TIMER(write_io_timer);
        RETURN_IF_ERROR(writer->flush());
    }
    if (has_deletes) {
        SCOPED_TIMER(write_io_timer);
        // op_offset is a placeholder here; the real (global) value is assigned in merge_other_writer when
        // this batch's writer is consolidated into the parent, based on the cumulative segment count.
        RETURN_IF_ERROR(writer->flush_del_file(*deletes, kUnknownDelOpOffset));
    }
    SCOPED_TIMER(write_io_timer);
    return writer->finish();
}

// Proactively batch-delete the merged-and-committed spill files: under flat layout their per-block dtors
// are no-ops (skip_file_deletion = true). Only FileBlocks under flat layout return a non-empty path;
// LogBlock and legacy FileBlock return nullopt and are skipped. MUST run before release_block_groups()
// clears the vector. Callers invoke this only on merge success -- failed-merge files are reclaimed by the
// offline vacuum_full job once the txn is inactive, avoiding races with files still in use upstream.
void hot_delete_merged_spill_files(TabletWriter* writer, LoadSpillMergeInputBatch* task) {
    std::vector<std::string> spill_paths;
    for (const auto& bg : task->block_groups) {
        if (bg == nullptr) continue;
        for (const auto& block : bg->blocks()) {
            if (block == nullptr) continue;
            if (auto p = block->path(); p.has_value() && !p->empty()) {
                spill_paths.emplace_back(std::move(*p));
            }
        }
    }
    if (!spill_paths.empty()) {
        VLOG(2) << "load spill hot delete: " << spill_paths.size() << " files for txn " << writer->txn_id();
        delete_files_async(std::move(spill_paths));
    }
}

} // namespace

void TabletInternalParallelMergeTask::run() {
    SCOPED_THREAD_LOCAL_MEM_SETTER(_merge_mem_tracker.get(), false);
    MonotonicStopWatch timer;
    timer.start();
    // Op-aware spill: when the merged schema's last column is __op (REPLACE-aggregated, so per key the
    // latest op by slot wins), each merged chunk is split into upsert rows (-> segments) and deleted keys
    // (-> a del file); see write_one_merged_chunk / finalize_merged_batch. The del file's op_offset is
    // assigned at result consolidation (TabletWriter::merge_other_writer) so it follows this batch's
    // segments and a later batch's re-upsert of the same key correctly wins.
    const auto& field_names = _schema->field_names();
    const bool has_op = !field_names.empty() && field_names.back() == "__op";
    // Schema used to write segments (drops the trailing __op when present).
    std::vector<ColumnId> write_cids;
    for (size_t i = 0; i + (has_op ? 1 : 0) < _schema->num_fields(); i++) {
        write_cids.push_back(static_cast<ColumnId>(i));
    }
    Schema write_schema(const_cast<Schema*>(_schema), write_cids);
    auto char_field_indexes = ChunkSchemaHelper::get_char_field_indexes(write_schema);
    PrimaryKeyEncodingType pk_enc = PrimaryKeyEncodingType::PK_ENCODING_TYPE_NONE;
    if (has_op) {
        auto enc_or = _writer->tablet_schema()->primary_key_encoding_type_or_error();
        if (!enc_or.ok()) {
            update_status(enc_or.status());
            return;
        }
        pk_enc = enc_or.value();
    }

    auto chunk_shared_ptr = ChunkFactory::new_chunk(*_schema, config::vector_chunk_size);
    auto chunk = chunk_shared_ptr.get();
    auto st = Status::OK();
    MutableColumnPtr deletes;  // accumulates net-deleted keys for this batch's del file
    bool wrote_upsert = false; // whether this batch produced any upsert segment
    // Read and write each merged chunk. _quit_flag (when non-null) lets a sibling task's error abort the
    // loop early; when nullptr, cancellation is unsupported and we run to completion.
    while (_quit_flag == nullptr || !_quit_flag->load()) {
        chunk->reset();
        auto itr_st = _task->merge_itr->get_next(chunk);
        if (itr_st.is_end_of_file()) {
            break;
        }
        if (!itr_st.ok()) {
            st = itr_st;
            break;
        }
        SCOPED_TIMER(_write_io_timer);
        st = write_one_merged_chunk(_writer.get(), chunk, has_op, *_schema, write_schema, char_field_indexes, pk_enc,
                                    &deletes, &wrote_upsert);
        if (!st.ok()) {
            break;
        }
    }
    if (st.ok()) {
        st = finalize_merged_batch(_writer.get(), has_op, write_schema, deletes, wrote_upsert, _write_io_timer);
    }
    if (st.ok()) {
        hot_delete_merged_spill_files(_writer.get(), _task.get());
    }
    // Release block groups to free up spill disk space.
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

int64_t TabletInternalParallelMergeTask::slot_idx() const {
    return _task->slot_idx;
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
