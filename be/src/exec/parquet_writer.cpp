
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

#include "exec/parquet_writer.h"

#include <fmt/format.h>

#include "runtime/exec_env.h"
#include "util/priority_thread_pool.hpp"
#include "util/uid_util.h"

namespace starrocks {

RollingAsyncParquetWriter::RollingAsyncParquetWriter(const TableInfo& tableInfo, const PartitionInfo& partitionInfo,
                                                     const std::vector<ExprContext*>& output_expr_ctxs,
                                                     RuntimeProfile* parent_profile)
        : _output_expr_ctxs(output_expr_ctxs), _parent_profile(parent_profile) {
    init_rolling_writer(tableInfo, partitionInfo);
}

Status RollingAsyncParquetWriter::init_rolling_writer(const TableInfo& tableInfo, const PartitionInfo& partitionInfo) {
    ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(tableInfo._table_location));
    _schema = tableInfo._schema;
    ::parquet::WriterProperties::Builder builder;
    if (tableInfo._enable_dictionary) {
        builder.enable_dictionary();
    } else {
        builder.disable_dictionary();
    }
    builder.version(::parquet::ParquetVersion::PARQUET_2_0);
    starrocks::parquet::ParquetBuildHelper::build_compression_type(builder, tableInfo._compress_type);
    _properties = builder.build();
    if (partitionInfo._column_names.size() != partitionInfo._column_values.size()) {
        return Status::InvalidArgument("columns and values are not matched in partitionInfo");
    }
    std::stringstream ss;
    ss << tableInfo._table_location;
    ss << "/data/";
    ss << partitionInfo.partition_dir();
    _partition_dir = ss.str();
    return Status::OK();
}

std::string RollingAsyncParquetWriter::get_new_file_name() {
    _cnt += 1;
    _location = _partition_dir + fmt::format("{}_{}.parquet", _cnt, generate_uuid_string());
    return _location;
}

Status RollingAsyncParquetWriter::new_file_writer() {
    std::string file_name = get_new_file_name();
    WritableFileOptions options{.sync_on_close = false, .mode = FileSystem::CREATE_OR_OPEN_WITH_TRUNCATE};
    ASSIGN_OR_RETURN(auto writable_file, _fs->new_writable_file(options, file_name));
    _writer = std::make_shared<starrocks::parquet::AsyncFileWriter>(
            std::move(writable_file), file_name, _partition_dir, _properties, _schema, _output_expr_ctxs,
            ExecEnv::GetInstance()->pipeline_sink_io_pool(), _parent_profile);
    auto st = _writer->init();
    return st;
}

Status RollingAsyncParquetWriter::append_chunk(Chunk* chunk, RuntimeState* state) {
    if (_writer == nullptr) {
        auto status = new_file_writer();
        if (!status.ok()) {
            return status;
        }
    }
    // exceed file size
    if (_writer->file_size() > _max_file_size) {
        auto st = close_current_writer(state);
        if (st.ok()) {
            new_file_writer();
        }
    }
    auto st = _writer->write(chunk);
    return st;
}

Status RollingAsyncParquetWriter::close_current_writer(RuntimeState* state) {
    Status st = _writer->close(state, RollingAsyncParquetWriter::add_iceberg_commit_info);
    if (st.ok()) {
        _pending_commits.emplace_back(_writer);
        return Status::OK();
    } else {
        LOG(WARNING) << "close file error: " << _location;
        return Status::IOError("close file error!");
    }
}

Status RollingAsyncParquetWriter::close(RuntimeState* state) {
    if (_writer != nullptr) {
        auto st = close_current_writer(state);
        if (!st.ok()) {
            return st;
        }
    }
    return Status::OK();
}

bool RollingAsyncParquetWriter::closed() {
    for (auto& writer : _pending_commits) {
        if (writer != nullptr && writer->closed()) {
            writer = nullptr;
        }
        if (writer != nullptr && (!writer->closed())) {
            return false;
        }
    }

    if (_writer != nullptr) {
        return _writer->closed();
    }

    return true;
}

#define MERGE_STATS_CASE(ParquetType)                                                                              \
    case ParquetType: {                                                                                            \
        auto typed_left_stat =                                                                                     \
                std::static_pointer_cast<::parquet::TypedStatistics<::parquet::PhysicalType<ParquetType>>>(left);  \
        auto typed_right_stat =                                                                                    \
                std::static_pointer_cast<::parquet::TypedStatistics<::parquet::PhysicalType<ParquetType>>>(right); \
        typed_left_stat->Merge(*typed_left_stat);                                                                  \
        return;                                                                                                    \
    }

void merge_stats(const std::shared_ptr<::parquet::Statistics>& left,
                 const std::shared_ptr<::parquet::Statistics>& right) {
    DCHECK(left->physical_type() == right->physical_type());
    switch (left->physical_type()) {
        MERGE_STATS_CASE(::parquet::Type::BOOLEAN);
        MERGE_STATS_CASE(::parquet::Type::INT32);
        MERGE_STATS_CASE(::parquet::Type::INT64);
        MERGE_STATS_CASE(::parquet::Type::INT96);
        MERGE_STATS_CASE(::parquet::Type::FLOAT);
        MERGE_STATS_CASE(::parquet::Type::DOUBLE);
        MERGE_STATS_CASE(::parquet::Type::BYTE_ARRAY);
        MERGE_STATS_CASE(::parquet::Type::FIXED_LEN_BYTE_ARRAY);
    default: {
    }
    }
}

void RollingAsyncParquetWriter::add_iceberg_commit_info(starrocks::parquet::AsyncFileWriter* writer,
                                                        RuntimeState* state) {
    TIcebergDataFile dataFile;
    dataFile.partition_path = writer->file_dir();
    dataFile.path = writer->file_name();
    dataFile.format = "parquet";
    dataFile.record_count = writer->metadata()->num_rows();
    dataFile.file_size_in_bytes = writer->file_size();
    std::vector<int64_t> split_offsets;
    writer->split_offsets(split_offsets);
    dataFile.split_offsets = split_offsets;

    // field_id -> column_stat
    std::unordered_map<int32_t, int64_t> column_sizes;
    std::unordered_map<int32_t, std::shared_ptr<::parquet::Statistics>> column_stats;

    const auto& meta = writer->metadata();
    auto* schema = meta->schema();

    for (int col_idx = 0; col_idx < meta->num_columns(); col_idx++) {
        auto field_id = schema->Column(col_idx)->schema_node()->field_id();

        // traverse meta of column chunk in each row group
        for (int rg_idx = 0; rg_idx < meta->num_row_groups(); rg_idx++) {
            auto column_meta = meta->RowGroup(rg_idx)->ColumnChunk(col_idx);
            column_sizes[field_id] += column_meta->total_compressed_size();

            auto column_stat = column_meta->statistics();
            if (rg_idx == 0) {
                column_stats[field_id] = column_stat;
            } else {
                merge_stats(column_stats[field_id], column_stat);
            }
        }
    }

    TIcebergColumnStats iceberg_stats;
    for (auto& [field_id, column_size] : column_sizes) {
        iceberg_stats.column_sizes[field_id] = column_size;
    }

    for (auto& [field_id, column_stat] : column_stats) {
        iceberg_stats.value_counts[field_id] = column_stat->num_values();
        if (column_stat->HasNullCount()) {
            iceberg_stats.null_value_counts[field_id] = column_stat->null_count();
        }
        if (column_stat->HasMinMax()) {
            iceberg_stats.lower_bounds[field_id] = column_stat->EncodeMin();
            iceberg_stats.upper_bounds[field_id] = column_stat->EncodeMax();
        }
    }

    dataFile.column_stats = iceberg_stats;

    state->add_iceberg_data_file(dataFile);
}

} // namespace starrocks
