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
    reader_params.lake_io_opts = {false, config::lake_compaction_stream_buffer_size_bytes};
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

    int64_t reader_time_ns = 0;
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
            SCOPED_RAW_TIMER(&reader_time_ns);
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
        VLOG_EVERY_N(3, 1000) << "Tablet: " << _tablet.id() << ", compaction progress: " << _context->progress.value();
    }

    // Adjust the progress here for 2 reasons:
    // 1. For primary key, due to the existence of the delete vector, the rows read may be less than "total_num_rows"
    // 2. If the "total_num_rows" is 0, the progress will not be updated above
    _context->progress.update(100);
    RETURN_IF_ERROR(writer->finish());

    // add reader stats
    _context->stats->reader_time_ns += reader_time_ns;
    _context->stats->accumulate(reader.stats());

    // update writer stats
    _context->stats->segment_write_ns += writer->stats().segment_write_ns;

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
    if (_input_rowsets.size() > 0 && _input_rowsets.back()->partial_segments_compaction()) {
        // can not call `get_read_chunk_size`, for example, if `total_input_segs` is shrinked to half,
        // read_chunk_size might be doubled, in this case, this optimization will not take effect
        return config::lake_compaction_chunk_size;
    }

    int64_t total_num_rows = 0;
    int64_t total_input_segs = 0;
    int64_t total_mem_footprint = 0;
    for (auto& rowset : _input_rowsets) {
        total_num_rows += rowset->num_rows();
        total_input_segs += rowset->is_overlapped() ? rowset->num_segments() : 1;
        LakeIOOptions lake_io_opts{.fill_data_cache = false,
                                   .buffer_size = config::lake_compaction_stream_buffer_size_bytes};
        auto fill_meta_cache = false;
        ASSIGN_OR_RETURN(auto segments, rowset->segments(lake_io_opts, fill_meta_cache));
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

    return CompactionUtils::get_read_chunk_size(config::compaction_memory_limit_per_worker,
                                                config::lake_compaction_chunk_size, total_num_rows, total_mem_footprint,
                                                total_input_segs);
}

} // namespace starrocks::lake
