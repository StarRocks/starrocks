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
#include "storage/rowset/column_reader.h"
#include "storage/storage_engine.h"
#include "storage/tablet_reader_params.h"
#include "util/defer_op.h"

namespace starrocks::lake {

Status HorizontalCompactionTask::execute(Progress* progress, CancelFunc cancel_func) {
    if (progress == nullptr) {
        return Status::InvalidArgument("progress is null");
    }

    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker.get());

    ASSIGN_OR_RETURN(auto tablet_schema, _tablet->get_schema());
    int64_t total_num_rows = 0;
    for (auto& rowset : _input_rowsets) {
        total_num_rows += rowset->num_rows();
    }

    ASSIGN_OR_RETURN(auto chunk_size, calculate_chunk_size());

    VLOG(3) << "Start horizontal compaction. tablet: " << _tablet->id() << ", reader chunk size: " << chunk_size;

    Schema schema = ChunkHelper::convert_schema(*tablet_schema);
    TabletReader reader(*_tablet, _version, schema, _input_rowsets);
    RETURN_IF_ERROR(reader.prepare());
    TabletReaderParams reader_params;
    reader_params.reader_type = READER_CUMULATIVE_COMPACTION;
    reader_params.chunk_size = chunk_size;
    reader_params.profile = nullptr;
    reader_params.use_page_cache = false;
    reader_params.fill_data_cache = false;
    RETURN_IF_ERROR(reader.open(reader_params));

    ASSIGN_OR_RETURN(auto writer, _tablet->new_writer(kHorizontal, _txn_id))
    RETURN_IF_ERROR(writer->open());
    DeferOp defer([&]() { writer->close(); });

    auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
    auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);

    while (true) {
        if (UNLIKELY(StorageEngine::instance()->bg_worker_stopped())) {
            return Status::Cancelled("background worker stopped");
        }
        if (cancel_func()) {
            return Status::Cancelled("cancelled");
        }
#ifndef BE_TEST
        RETURN_IF_ERROR(tls_thread_status.mem_tracker()->check_mem_limit("Compaction"));
#endif
        if (auto st = reader.get_next(chunk.get()); st.is_end_of_file()) {
            break;
        } else if (!st.ok()) {
            return st;
        }
        ChunkHelper::padding_char_columns(char_field_indexes, schema, *tablet_schema, chunk.get());
        RETURN_IF_ERROR(writer->write(*chunk));
        chunk->reset();

        progress->update(100 * reader.stats().raw_rows_read / total_num_rows);
        VLOG_EVERY_N(3, 1000) << "Tablet: << " << _tablet->id() << ", compaction progress: " << progress->value();
    }
    // Adjust the progress here for 2 reasons:
    // 1. For primary key, due to the existence of the delete vector, the rows read may be less than "total_num_rows"
    // 2. If the "total_num_rows" is 0, the progress will not be updated above
    progress->update(100);
    RETURN_IF_ERROR(writer->finish());

    auto txn_log = std::make_shared<TxnLog>();
    auto op_compaction = txn_log->mutable_op_compaction();
    txn_log->set_tablet_id(_tablet->id());
    txn_log->set_txn_id(_txn_id);
    for (auto& rowset : _input_rowsets) {
        op_compaction->add_input_rowsets(rowset->id());
    }
    for (auto& file : writer->files()) {
        op_compaction->mutable_output_rowset()->add_segments(file);
    }
    op_compaction->mutable_output_rowset()->set_num_rows(writer->num_rows());
    op_compaction->mutable_output_rowset()->set_data_size(writer->data_size());
    op_compaction->mutable_output_rowset()->set_overlapped(false);
    Status st = _tablet->put_txn_log(std::move(txn_log));
    return st;
}

StatusOr<int32_t> HorizontalCompactionTask::calculate_chunk_size() {
    int64_t total_num_rows = 0;
    int64_t total_input_segs = 0;
    int64_t total_mem_footprint = 0;
    for (auto& rowset : _input_rowsets) {
        total_num_rows += rowset->num_rows();
        total_input_segs += rowset->is_overlapped() ? rowset->num_segments() : 1;
        ASSIGN_OR_RETURN(auto segments, rowset->segments(false));
        for (auto& segment : segments) {
            for (size_t i = 0; i < segment->num_columns(); ++i) {
                const auto* column_reader = segment->column(i);
                if (column_reader == nullptr) {
                    continue;
                }
                total_mem_footprint += column_reader->total_mem_footprint();
            }
        }
    }
    return CompactionUtils::get_read_chunk_size(config::compaction_memory_limit_per_worker, config::vector_chunk_size,
                                                total_num_rows, total_mem_footprint, total_input_segs);
}

} // namespace starrocks::lake
