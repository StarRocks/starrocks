
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

    std::unordered_map<int32_t, int64_t> column_sizes;
    std::unordered_map<int32_t, int64_t> value_counts;
    std::unordered_map<int32_t, int64_t> null_value_counts;
    std::unordered_map<int32_t, std::string> min_values;
    std::unordered_map<int32_t, std::string> max_values;

    const auto& metadata = writer->metadata();

    for (int i = 0; i < metadata->num_row_groups(); ++i) {
        auto block = metadata->RowGroup(i);
        for (int j = 0; j < block->num_columns(); j++) {
            auto column_meta = block->ColumnChunk(j);
            int field_id = j + 1;
            if (null_value_counts.find(field_id) == null_value_counts.end()) {
                null_value_counts.insert({field_id, column_meta->statistics()->null_count()});
            } else {
                null_value_counts[field_id] += column_meta->statistics()->null_count();
            }

            if (column_sizes.find(field_id) == column_sizes.end()) {
                column_sizes.insert({field_id, column_meta->total_compressed_size()});
            } else {
                column_sizes[field_id] += column_meta->total_compressed_size();
            }

            if (value_counts.find(field_id) == value_counts.end()) {
                value_counts.insert({field_id, column_meta->num_values()});
            } else {
                value_counts[field_id] += column_meta->num_values();
            }

            min_values[field_id] = column_meta->statistics()->EncodeMin();
            max_values[field_id] = column_meta->statistics()->EncodeMax();
        }
    }

    TIcebergColumnStats stats;
    for (auto& i : column_sizes) {
        stats.columnSizes.insert({i.first, i.second});
    }
    for (auto& i : value_counts) {
        stats.valueCounts.insert({i.first, i.second});
    }
    for (auto& i : null_value_counts) {
        stats.nullValueCounts.insert({i.first, i.second});
    }
    for (auto& i : min_values) {
        stats.lowerBounds.insert({i.first, i.second});
    }
    for (auto& i : max_values) {
        stats.upperBounds.insert({i.first, i.second});
    }

    dataFile.column_stats = stats;

    state->add_iceberg_data_file(dataFile);
}

} // namespace starrocks