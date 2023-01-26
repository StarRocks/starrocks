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
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/txn_log.h"
#include "storage/storage_engine.h"
#include "storage/tablet_reader_params.h"
#include "util/defer_op.h"

namespace starrocks::lake {

HorizontalCompactionTask::~HorizontalCompactionTask() = default;

Status HorizontalCompactionTask::execute(Stats* stats) {
    ASSIGN_OR_RETURN(auto tablet_schema, _tablet->get_schema());
    const KeysType keys_type = tablet_schema->keys_type();
    int64_t num_rows = 0;
    int64_t num_size = 0;
    for (auto& rowset : _input_rowsets) {
        num_rows += rowset->num_rows();
        num_size += rowset->data_size();
    }
    int64_t max_input_segs = 0;
    if (keys_type == DUP_KEYS) {
        max_input_segs = 1;
    } else {
        for (auto& rowset : _input_rowsets) {
            max_input_segs += rowset->is_overlapped() ? rowset->num_segments() : 1;
        }
    }
    const int32_t chunk_size = CompactionUtils::get_read_chunk_size(
            config::compaction_memory_limit_per_worker, config::vector_chunk_size, num_rows, num_size, max_input_segs);

    Schema schema = ChunkHelper::convert_schema(*tablet_schema);
    TabletReader reader(*_tablet, _version, schema, _input_rowsets);
    RETURN_IF_ERROR(reader.prepare());
    TabletReaderParams reader_params;
    reader_params.reader_type = READER_CUMULATIVE_COMPACTION;
    reader_params.chunk_size = chunk_size;
    reader_params.profile = nullptr;
    reader_params.use_page_cache = false;
    RETURN_IF_ERROR(reader.open(reader_params));

    ASSIGN_OR_RETURN(auto writer, _tablet->new_writer());
    RETURN_IF_ERROR(writer->open());
    DeferOp defer([&]() { writer->close(); });

    auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
    auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);

    while (true) {
        if (UNLIKELY(StorageEngine::instance()->bg_worker_stopped())) {
            return Status::Cancelled("background worker stopped");
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
    }
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
    if (st.ok() && stats != nullptr) {
        stats->input_bytes.fetch_add(num_size, std::memory_order_relaxed);
        stats->input_rows.fetch_add(num_rows, std::memory_order_relaxed);
        stats->output_bytes.fetch_add(writer->data_size(), std::memory_order_relaxed);
        stats->output_rows.fetch_add(writer->num_rows(), std::memory_order_relaxed);
    }
    return st;
}

} // namespace starrocks::lake
