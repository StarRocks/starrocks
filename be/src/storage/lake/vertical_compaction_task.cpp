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

#include "storage/lake/vertical_compaction_task.h"

#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/compaction_utils.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/tablet_reader.h"
#include "storage/lake/tablet_writer.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/update_manager.h"
#include "storage/row_source_mask.h"
#include "storage/rowset/column_reader.h"
#include "storage/storage_engine.h"
#include "storage/tablet_reader_params.h"
#include "util/defer_op.h"

namespace starrocks::lake {

Status VerticalCompactionTask::execute(Progress* progress, CancelFunc cancel_func) {
    if (progress == nullptr) {
        return Status::InvalidArgument("progress is null");
    }

    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker.get());

    ASSIGN_OR_RETURN(_tablet_schema, _tablet.get_schema());
    for (auto& rowset : _input_rowsets) {
        _total_num_rows += rowset->num_rows();
        _total_data_size += rowset->data_size();
        _total_input_segs += rowset->is_overlapped() ? rowset->num_segments() : 1;
    }

    const auto& store_paths = ExecEnv::GetInstance()->store_paths();
    DCHECK(!store_paths.empty());
    auto mask_buffer = std::make_unique<RowSourceMaskBuffer>(_tablet.id(), store_paths.begin()->path);
    auto source_masks = std::make_unique<std::vector<RowSourceMask>>();

    uint32_t max_rows_per_segment =
            CompactionUtils::get_segment_max_rows(config::max_segment_file_size, _total_num_rows, _total_data_size);
    ASSIGN_OR_RETURN(auto writer, _tablet.new_writer(kVertical, _txn_id, max_rows_per_segment));
    RETURN_IF_ERROR(writer->open());
    DeferOp defer([&]() { writer->close(); });

    std::vector<std::vector<uint32_t>> column_groups;
    CompactionUtils::split_column_into_groups(_tablet_schema->num_columns(), _tablet_schema->sort_key_idxes(),
                                              config::vertical_compaction_max_columns_per_group, &column_groups);
    auto column_group_size = column_groups.size();

    VLOG(3) << "Start vertical compaction. tablet: " << _tablet.id()
            << ", max rows per segment: " << max_rows_per_segment << ", column group size: " << column_group_size;

    for (size_t i = 0; i < column_group_size; ++i) {
        if (UNLIKELY(StorageEngine::instance()->bg_worker_stopped())) {
            return Status::Cancelled("background worker stopped");
        }

        bool is_key = (i == 0);
        if (!is_key) {
            // read mask buffer from the beginning
            RETURN_IF_ERROR(mask_buffer->flip_to_read());
        }
        RETURN_IF_ERROR(compact_column_group(is_key, i, column_group_size, column_groups[i], writer, mask_buffer.get(),
                                             source_masks.get(), progress, cancel_func));
    }
    // Adjust the progress here for 2 reasons:
    // 1. For primary key, due to the existence of the delete vector, the number of rows read may be less than the
    //    number of rows counted in the metadata.
    // 2. If the number of rows is 0, the progress will not be updated
    progress->update(100);

    RETURN_IF_ERROR(writer->finish());

    auto txn_log = std::make_shared<TxnLog>();
    auto op_compaction = txn_log->mutable_op_compaction();
    txn_log->set_tablet_id(_tablet.id());
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
    RETURN_IF_ERROR(_tablet.put_txn_log(txn_log));
    if (_tablet_schema->keys_type() == KeysType::PRIMARY_KEYS) {
        // preload primary key table's compaction state
        _tablet.update_mgr()->preload_compaction_state(*txn_log, _tablet, _tablet_schema);
    }
    return Status::OK();
}

StatusOr<int32_t> VerticalCompactionTask::calculate_chunk_size_for_column_group(
        const std::vector<uint32_t>& column_group) {
    int64_t total_mem_footprint = 0;
    for (auto& rowset : _input_rowsets) {
        // in vertical compaction, there may be a lot of column groups, it will waste a lot of time to
        // load segments (footer and column index) every time if segments are not in the cache.
        //
        // test case: 4k columns, 150 segments, 60w rows
        // compaction task cost: 272s (fill metadata cache) vs 2400s (not fill metadata cache)
        ASSIGN_OR_RETURN(auto segments, rowset->segments(false, true));
        for (auto& segment : segments) {
            for (auto column_index : column_group) {
                const auto* column_reader = segment->column(column_index);
                if (column_reader == nullptr) {
                    continue;
                }
                total_mem_footprint += column_reader->total_mem_footprint();
            }
        }
    }
    return CompactionUtils::get_read_chunk_size(config::compaction_memory_limit_per_worker, config::vector_chunk_size,
                                                _total_num_rows, total_mem_footprint, _total_input_segs);
}

Status VerticalCompactionTask::compact_column_group(bool is_key, int column_group_index, size_t column_group_size,
                                                    const std::vector<uint32_t>& column_group,
                                                    std::unique_ptr<TabletWriter>& writer,
                                                    RowSourceMaskBuffer* mask_buffer,
                                                    std::vector<RowSourceMask>* source_masks, Progress* progress,
                                                    const CancelFunc& cancel_func) {
    ASSIGN_OR_RETURN(auto chunk_size, calculate_chunk_size_for_column_group(column_group));

    Schema schema = column_group_index == 0 ? (_tablet_schema->sort_key_idxes().empty()
                                                       ? ChunkHelper::convert_schema(_tablet_schema, column_group)
                                                       : ChunkHelper::get_sort_key_schema(_tablet_schema))
                                            : ChunkHelper::convert_schema(_tablet_schema, column_group);
    TabletReader reader(_tablet, _version, schema, _input_rowsets, is_key, mask_buffer);
    RETURN_IF_ERROR(reader.prepare());
    TabletReaderParams reader_params;
    reader_params.reader_type = READER_CUMULATIVE_COMPACTION;
    reader_params.chunk_size = chunk_size;
    reader_params.profile = nullptr;
    reader_params.use_page_cache = false;
    reader_params.fill_data_cache = false;
    RETURN_IF_ERROR(reader.open(reader_params));

    auto chunk = ChunkHelper::new_chunk(schema, chunk_size);
    auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);

    VLOG(3) << "Compact column group. tablet: " << _tablet.id() << ", column group: " << column_group_index
            << ", reader chunk size: " << chunk_size;

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
        if (auto st = reader.get_next(chunk.get(), source_masks); st.is_end_of_file()) {
            break;
        } else if (!st.ok()) {
            return st;
        }

        ChunkHelper::padding_char_columns(char_field_indexes, schema, _tablet_schema, chunk.get());
        RETURN_IF_ERROR(writer->write_columns(*chunk, column_group, is_key));
        chunk->reset();

        if (!source_masks->empty()) {
            if (is_key) {
                RETURN_IF_ERROR(mask_buffer->write(*source_masks));
            }
            source_masks->clear();
        }

        progress->update((100 * column_group_index + 100 * reader.stats().raw_rows_read / _total_num_rows) /
                         column_group_size);
        VLOG_EVERY_N(3, 1000) << "Tablet: " << _tablet.id() << ", compaction progress: " << progress->value();
    }
    RETURN_IF_ERROR(writer->flush_columns());

    if (is_key) {
        RETURN_IF_ERROR(mask_buffer->flush());
    }
    return Status::OK();
}

} // namespace starrocks::lake
