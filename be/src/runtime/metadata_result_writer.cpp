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

#include "runtime/metadata_result_writer.h"

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/expr.h"
#include "runtime/buffer_control_block.h"
#include "util/thrift_util.h"

namespace starrocks {

MetadataResultWriter::MetadataResultWriter(BufferControlBlock* sinker,
                                           const std::vector<ExprContext*>& output_expr_ctxs,
                                           RuntimeProfile* parent_profile, TResultSinkType::type sink_type)
        : BufferControlResultWriter(sinker, parent_profile),
          _output_expr_ctxs(output_expr_ctxs),
          _sink_type(sink_type) {}

MetadataResultWriter::~MetadataResultWriter() = default;

Status MetadataResultWriter::init(RuntimeState* state) {
    _init_profile();
    if (nullptr == _sinker) {
        return Status::InternalError("sinker is nullptr.");
    }
    return Status::OK();
}

Status MetadataResultWriter::append_chunk(Chunk* chunk) {
    SCOPED_TIMER(_append_chunk_timer);
    auto process_status = _process_chunk(chunk);
    if (!process_status.ok() || process_status.value() == nullptr) {
        return process_status.status();
    }
    auto result = std::move(process_status.value());

    const size_t num_rows = result->result_batch.rows.size();
    Status status = _sinker->add_batch(result);

    if (status.ok()) {
        _written_rows += num_rows;
        return status;
    }

    LOG(WARNING) << "Append metadata result to sink failed.";
    return status;
}

StatusOr<TFetchDataResultPtrs> MetadataResultWriter::process_chunk(Chunk* chunk) {
    SCOPED_TIMER(_append_chunk_timer);
    TFetchDataResultPtrs results;
    auto process_status = _process_chunk(chunk);
    if (!process_status.ok()) {
        return process_status.status();
    }
    if (process_status.value() != nullptr) {
        results.push_back(std::move(process_status.value()));
    }
    return results;
}

StatusOr<TFetchDataResultPtr> MetadataResultWriter::_process_chunk(Chunk* chunk) {
    if (nullptr == chunk || 0 == chunk->num_rows()) {
        return nullptr;
    }

    const int num_columns = _output_expr_ctxs.size();

    Columns result_columns;
    result_columns.reserve(num_columns);

    for (int i = 0; i < num_columns; ++i) {
        ASSIGN_OR_RETURN(auto col, _output_expr_ctxs[i]->evaluate(chunk));
        result_columns.emplace_back(std::move(col));
    }

    std::unique_ptr<TFetchDataResult> result(new (std::nothrow) TFetchDataResult());
    if (!result) {
        return Status::MemoryAllocFailed("memory allocate failed");
    }

    if (_sink_type == TResultSinkType::METADATA_ICEBERG) {
        RETURN_IF_ERROR(_fill_iceberg_metadata(result_columns, chunk, result.get()));
    }

    return result;
}

// Columns is fixed in the first version of logical iceberg metadata table.
// In principle, there will be no changes in the future.
// Only new columns are allowed if necessary. The newly added columns need to check if nullable.
// The first version position -> column_name
// 0 -> "content"
// 1 -> "file_path"
// 2 -> "file_format"
// 3 -> "spec_id"
// 4 -> "partition_data"
// 5 -> "record_count"
// 6 -> "file_size_in_bytes"
// 7 -> "split_offsets"
// 8 -> "sort_id"
// 9 -> "equality_ids"
// 10 -> "file_sequence_number"
// 11 -> "data_sequence_number"
// 12 -> "column_stats"
// 13 -> "key_metadata"
Status MetadataResultWriter::_fill_iceberg_metadata(const Columns& columns, const Chunk* chunk,
                                                    TFetchDataResult* result) const {
    SCOPED_TIMER(_convert_tuple_timer);

    const auto* content = down_cast<const Int32Column*>(ColumnHelper::get_data_column(columns[0].get()));
    const auto* file_path = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[1].get()));
    const auto* file_format = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[2].get()));
    const auto* spec_id = down_cast<const Int32Column*>(ColumnHelper::get_data_column(columns[3].get()));
    const auto* partition_data = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[4].get()));
    const auto* record_count = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[5].get()));
    const auto* file_size_in_bytes = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[6].get()));
    const auto* split_offsets = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(columns[7].get()));

    const auto* sort_id = down_cast<const Int32Column*>(ColumnHelper::get_data_column(columns[8].get()));
    const auto* equality_ids = down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(columns[9].get()));
    const auto* file_sequence_number = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[10].get()));
    const auto* data_sequence_number = down_cast<const Int64Column*>(ColumnHelper::get_data_column(columns[11].get()));
    const auto* iceberg_metrics = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[12].get()));
    const auto* key_metadata = down_cast<const BinaryColumn*>(ColumnHelper::get_data_column(columns[13].get()));

    std::vector<TMetadataEntry> meta_entries;
    int num_rows = chunk->num_rows();

    meta_entries.resize(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        TIcebergMetadata iceberg_metadata;
        meta_entries[i].__set_iceberg_metadata(iceberg_metadata);
        auto& iceberg_meta = meta_entries[i].iceberg_metadata;

        iceberg_meta.__set_content(content->get(i).get_int32());
        iceberg_meta.__set_file_path(file_path->get_slice(i).to_string());
        iceberg_meta.__set_file_format(file_format->get_slice(i).to_string());
        iceberg_meta.__set_spec_id(spec_id->get(i).get_int32());

        if (!columns[4]->is_null(i)) {
            iceberg_meta.__set_partition_data(partition_data->get_slice(i).to_string());
        }

        iceberg_meta.__set_record_count(record_count->get(i).get_int64());
        iceberg_meta.__set_file_size_in_bytes(file_size_in_bytes->get(i).get_int64());

        std::vector<int64_t> offsets;
        const auto split_array = split_offsets->get(i).get_array();
        for (auto& split_offset : split_array) {
            offsets.emplace_back(split_offset.get_int64());
        }
        iceberg_meta.__set_split_offsets(offsets);

        if (!columns[8]->is_null(i)) {
            iceberg_meta.__set_sort_id(sort_id->get(i).get_int32());
        }
        if (!columns[9]->is_null(i)) {
            std::vector<int32_t> ids;
            const auto eq_id_array = equality_ids->get(i).get_array();
            for (auto& eq_id : eq_id_array) {
                ids.emplace_back(eq_id.get_int32());
            }
            iceberg_meta.__set_equality_ids(ids);
        }
        if (!columns[10]->is_null(i)) {
            iceberg_meta.__set_file_sequence_number(file_sequence_number->get(i).get_int64());
        }
        if (!columns[11]->is_null(i)) {
            iceberg_meta.__set_data_sequence_number(data_sequence_number->get(i).get_int64());
        }
        if (!columns[12]->is_null(i)) {
            iceberg_meta.__set_column_stats(iceberg_metrics->get_slice(i).to_string());
        }
        if (!columns[13]->is_null(i)) {
            iceberg_meta.__set_key_metadata(key_metadata->get_slice(i).to_string());
        }
    }

    result->result_batch.rows.resize(num_rows);

    ThriftSerializer serializer(false, chunk->memory_usage());
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(serializer.serialize(&meta_entries[i], &result->result_batch.rows[i]));
    }

    return Status::OK();
}
} // namespace starrocks