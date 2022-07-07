// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/lake/horizontal_compaction_task.h"

#include <fmt/format.h>

#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/aggregate_iterator.h"
#include "storage/chunk_helper.h"
#include "storage/compaction_utils.h"
#include "storage/empty_iterator.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/txn_log.h"
#include "storage/merge_iterator.h"
#include "storage/rowset/rowset_options.h"
#include "storage/storage_engine.h"
#include "storage/tablet_reader_params.h"

namespace starrocks::lake {

HorizontalCompactionTask::~HorizontalCompactionTask() = default;

Status HorizontalCompactionTask::execute() {
    OlapReaderStatistics statistics;
    ASSIGN_OR_RETURN(auto tablet_schema, _tablet->get_schema());
    const KeysType keys_type = tablet_schema->keys_type();
    if (keys_type == PRIMARY_KEYS) {
        return Status::NotSupported("primary key compaction not supported");
    }
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
            max_input_segs = rowset->is_overlapped() ? rowset->num_segments() : 1;
        }
    }
    const int32_t chunk_size = CompactionUtils::get_read_chunk_size(
            config::compaction_memory_limit_per_worker, config::vector_chunk_size, num_rows, num_size, max_input_segs);

    vectorized::RowsetReadOptions rs_opts;
    rs_opts.sorted = true;
    rs_opts.reader_type = READER_CUMULATIVE_COMPACTION;
    rs_opts.chunk_size = chunk_size;
    rs_opts.stats = &statistics;
    rs_opts.runtime_state = nullptr;
    rs_opts.profile = nullptr;
    rs_opts.use_page_cache = false;
    rs_opts.tablet_schema = tablet_schema.get();

    vectorized::Schema schema = vectorized::ChunkHelper::convert_schema_to_format_v2(*tablet_schema);

    std::vector<ChunkIteratorPtr> iterators;
    for (auto& rowset : _input_rowsets) {
        ASSIGN_OR_RETURN(auto iter, rowset->read(schema, rs_opts));
        iterators.emplace_back(std::move(iter));
    }

    std::shared_ptr<vectorized::ChunkIterator> read_iter;
    if (UNLIKELY(iterators.empty())) {
        read_iter = new_empty_iterator(schema, chunk_size);
    } else if (keys_type == DUP_KEYS) {
        read_iter = new_heap_merge_iterator(iterators);
    } else {
        read_iter = new_aggregate_iterator(new_heap_merge_iterator(iterators), 0);
    }
    RETURN_IF_ERROR(read_iter->init_encoded_schema(vectorized::EMPTY_GLOBAL_DICTMAPS));
    RETURN_IF_ERROR(read_iter->init_output_schema(vectorized::EMPTY_FILTERED_COLUMN_IDS));
    ASSIGN_OR_RETURN(auto writer, _tablet->new_writer());
    RETURN_IF_ERROR(writer->open());

    auto chunk = vectorized::ChunkHelper::new_chunk(schema, chunk_size);
    auto char_field_indexes = vectorized::ChunkHelper::get_char_field_indexes(schema);

    while (true) {
        if (UNLIKELY(ExecEnv::GetInstance()->storage_engine()->bg_worker_stopped())) {
            return Status::Cancelled("background worker stopped");
        }
#ifndef BE_TEST
        RETURN_IF_ERROR(tls_thread_status.mem_tracker()->check_mem_limit("Compaction"));
#endif
        if (auto st = read_iter->get_next(chunk.get()); st.is_end_of_file()) {
            break;
        } else if (!st.ok()) {
            return st;
        }
        vectorized::ChunkHelper::padding_char_columns(char_field_indexes, schema, *tablet_schema, chunk.get());
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
    return _tablet->put_txn_log(std::move(txn_log));
}

} // namespace starrocks::lake
