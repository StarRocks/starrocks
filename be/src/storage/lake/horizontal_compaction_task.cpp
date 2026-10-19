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

#include "storage/lake/horizontal_compaction_task.h"

#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/compaction_utils.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/update_manager.h"
#include "storage/rows_mapper.h"
#include "storage/rowset/column_reader.h"
#include "storage/storage_engine.h"
#include "storage/tablet_reader_params.h"
#include "util/defer_op.h"

namespace starrocks::lake {

Status HorizontalCompactionTask::execute(CancelFunc cancel_func, ThreadPool* flush_pool) {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker.get());

    int64_t total_num_rows = 0;
    for (auto& rowset : _input_rowsets) {
        total_num_rows += rowset->num_rows();
        _context->stats->read_segment_count += rowset->num_segments();
    }

    ASSIGN_OR_RETURN(auto chunk_size, calculate_chunk_size());

    VLOG(3) << "Start horizontal compaction. tablet: " << _tablet.id() << ", reader chunk size: " << chunk_size;

    Schema schema = ChunkHelper::convert_schema(_tablet_schema);
    TabletReader reader(_tablet.tablet_manager(), _tablet.metadata(), schema, _input_rowsets, _tablet_schema);
    RETURN_IF_ERROR(reader.prepare());
    TabletReaderParams reader_params;
    reader_params.reader_type = READER_CUMULATIVE_COMPACTION;
    reader_params.chunk_size = chunk_size;
    reader_params.profile = nullptr;
    reader_params.use_page_cache = false;
<<<<<<< HEAD
    reader_params.lake_io_opts = {.fill_data_cache = true,
                                  .buffer_size = config::lake_compaction_stream_buffer_size_bytes};
=======
    // The read pass reuses the segments that calculate_chunk_size() already opened, and there are
    // exactly two mechanisms for that -- never both at once:
    //   * holding: the Segment objects stay on the Rowset instance for the whole task. The shared
    //     metadata cache is then not needed, and filling it would only evict neighbours' entries
    //     with input segments this task is about to delete.
    //   * the shared metadata cache: the reuse path before hold_segments existed, and what the kill
    //     switch (and a rowset that cannot hold, see Rowset::segments) falls back to.
    // Hence the mutual exclusion below. `_hold_input_segments` is the task-wide snapshot taken at
    // the top of execute(), so the two phases never disagree mid-task.
    // `fill_metadata_cache` must stay named explicitly: assigning the whole struct replaces the
    // TabletReaderParams default (`{.fill_data_cache = true, .fill_metadata_cache = true}`) with
    // LakeIOOptions' in-class defaults for every field not listed, which silently turned metadata
    // caching off.
    const bool reuse_via_shared_cache = !_hold_input_segments;
    reader_params.lake_io_opts = {.fill_data_cache = config::lake_enable_horizontal_compaction_fill_data_cache,
                                  .buffer_size = _stream_buffer_size,
                                  .fill_metadata_cache = reuse_via_shared_cache,
                                  .hold_segments = _hold_input_segments};
>>>>>>> 80ca09f ([BugFix] Bound lake compaction read buffers by the per-worker memory budget (#80229))
    reader_params.column_access_paths = &_column_access_paths;
    RETURN_IF_ERROR(reader.open(reader_params));

    ASSIGN_OR_RETURN(auto writer,
                     _tablet.new_writer_with_schema(kHorizontal, _txn_id, 0, flush_pool, true /** compaction **/,
                                                    _tablet_schema /** output rowset schema**/))
    RETURN_IF_ERROR(writer->open());
    DeferOp defer([&]() { writer->close(); });

    auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
    auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);
    std::vector<uint64_t> rssid_rowids;
    rssid_rowids.reserve(chunk_size);

    const bool enable_light_pk_compaction_publish = StorageEngine::instance()->enable_light_pk_compaction_publish();
    while (true) {
        if (UNLIKELY(StorageEngine::instance()->bg_worker_stopped())) {
            return Status::Aborted("background worker stopped");
        }

        RETURN_IF_ERROR(cancel_func());

#ifndef BE_TEST
        RETURN_IF_ERROR(tls_thread_status.mem_tracker()->check_mem_limit("Compaction"));
#endif
        {
            auto st = Status::OK();
            if (_tablet_schema->keys_type() == KeysType::PRIMARY_KEYS && enable_light_pk_compaction_publish) {
                st = reader.get_next(chunk.get(), &rssid_rowids);
            } else {
                st = reader.get_next(chunk.get());
            }
            if (st.is_end_of_file()) {
                break;
            } else if (!st.ok()) {
                return st;
            }
        }
        ChunkHelper::padding_char_columns(char_field_indexes, schema, _tablet_schema, chunk.get());
        if (rssid_rowids.empty()) {
            RETURN_IF_ERROR(writer->write(*chunk));
        } else {
            // pk table compaction
            RETURN_IF_ERROR(writer->write(*chunk, rssid_rowids));
        }
        chunk->reset();
        rssid_rowids.clear();

        _context->progress.update(100 * reader.stats().raw_rows_read / total_num_rows);
        _context->stats->collect(reader.stats());
    }

    RETURN_IF_ERROR(writer->finish());

    // Adjust the progress here for 2 reasons:
    // 1. For primary key, due to the existence of the delete vector, the rows read may be less than "total_num_rows"
    // 2. If the "total_num_rows" is 0, the progress will not be updated above
    _context->progress.update(100);

    // Close reader to ensure IO statistics are updated via SegmentIterator::_update_stats() before collecting
    reader.close();

    _context->stats->collect(reader.stats());
    _context->stats->collect(writer->stats());

    auto txn_log = std::make_shared<TxnLog>();
    auto op_compaction = txn_log->mutable_op_compaction();
    txn_log->set_tablet_id(_tablet.id());
    txn_log->set_txn_id(_txn_id);
    RETURN_IF_ERROR(fill_compaction_segment_info(op_compaction, writer.get()));
    op_compaction->set_compact_version(_tablet.metadata()->version());
    RETURN_IF_ERROR(execute_index_major_compaction(txn_log.get()));
    RETURN_IF_ERROR(_tablet.tablet_manager()->put_txn_log(txn_log));
    if (_tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
        // preload primary key table's compaction state
        Tablet t(_tablet.tablet_manager(), _tablet.id());
        _tablet.tablet_manager()->update_mgr()->preload_compaction_state(*txn_log, t, _tablet_schema);
    }

    LOG(INFO) << "Horizontal compaction finished. tablet: " << _tablet.id() << ", txn_id: " << _txn_id
              << ", statistics: " << _context->stats->to_json_stats();

    return Status::OK();
}

StatusOr<int32_t> HorizontalCompactionTask::calculate_chunk_size() {
    // The single read pass opens one buffered stream per column for every input source, all at once.
    int64_t total_input_segs = 0;
    for (const auto& rowset : _input_rowsets) {
        total_input_segs += rowset->is_overlapped() ? rowset->num_segments() : 1;
    }
    const int64_t num_streams = total_input_segs * static_cast<int64_t>(_tablet_schema->num_columns());
    _stream_buffer_size = stream_buffer_size_for_pass(num_streams);
    if (_input_rowsets.size() > 0 && _input_rowsets.back()->partial_segments_compaction()) {
        // can not call `get_read_chunk_size`, for example, if `total_input_segs` is shrinked to half,
        // read_chunk_size might be doubled, in this case, this optimization will not take effect
        return config::lake_compaction_chunk_size;
    }

    int64_t total_num_rows = 0;
    int64_t total_mem_footprint = 0;
    for (auto& rowset : _input_rowsets) {
        total_num_rows += rowset->num_rows();
<<<<<<< HEAD
        total_input_segs += rowset->is_overlapped() ? rowset->num_segments() : 1;
        LakeIOOptions lake_io_opts{.fill_data_cache = false,
                                   .buffer_size = config::lake_compaction_stream_buffer_size_bytes,
                                   .fill_metadata_cache = false};
=======
        // This pass only touches segment footers and column indexes, never column data, so the
        // data cache stays off. With hold_segments the read pass in execute() reuses the Segment
        // objects held on the Rowset instance, so the shared metadata cache is not filled — that
        // would only evict neighbors' entries with soon-to-be-deleted input segments. With the kill
        // switch off, filling is the only way execute() avoids re-reading every footer from remote
        // storage (TabletManager::load_segment always probes the metacache but only inserts when
        // asked). Holding and cache-filling are the two alternative reuse mechanisms, never both --
        // same reasoning as in execute().
        const bool reuse_via_shared_cache = !_hold_input_segments;
        LakeIOOptions lake_io_opts{.fill_data_cache = false,
                                   .buffer_size = _stream_buffer_size,
                                   .fill_metadata_cache = reuse_via_shared_cache,
                                   .hold_segments = _hold_input_segments};
>>>>>>> 80ca09f ([BugFix] Bound lake compaction read buffers by the per-worker memory budget (#80229))
        ASSIGN_OR_RETURN(auto segments, rowset->segments(lake_io_opts));
        for (auto& segment : segments) {
            for (size_t i = 0; i < segment->num_columns(); ++i) {
                auto uid = _tablet_schema->column(i).unique_id();
                const auto* column_reader = segment->column_with_uid(uid);
                if (column_reader == nullptr) {
                    continue;
                }
                total_mem_footprint += column_reader->total_mem_footprint();
            }
        }
    }

<<<<<<< HEAD
    return CompactionUtils::get_read_chunk_size(config::compaction_memory_limit_per_worker,
                                                config::lake_compaction_chunk_size, total_num_rows, total_mem_footprint,
                                                total_input_segs);
=======
    // The held input set stays resident for the whole task, so it comes out of the same per-worker
    // budget the read buffers are sized from; charging it is what keeps the chunk sizing honest.
    // When holding would starve that budget the task stops holding instead -- see there.
    return chunk_size_with_held_segments(held_segments_bytes, total_num_rows, total_mem_footprint, total_input_segs,
                                         num_streams * _stream_buffer_size);
>>>>>>> 80ca09f ([BugFix] Bound lake compaction read buffers by the per-worker memory budget (#80229))
}

} // namespace starrocks::lake
