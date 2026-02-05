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

#include "formats/parquet/parquet_file_writer.h"

#include <fmt/core.h>
#include <glog/logging.h>
#include <parquet/exception.h>
#include <parquet/file_writer.h>
#include <parquet/metadata.h>
#include <parquet/parquet_version.h>
#include <parquet/properties.h>
#include <parquet/statistics.h>
#include <runtime/current_thread.h>

#include <future>
#include <ostream>
#include <utility>

#include "column/column_helper.h"
#include "common/http/content_type.h"
#include "formats/file_writer.h"
#include "formats/parquet/arrow_memory_pool.h"
#include "formats/parquet/chunk_writer.h"
#include "formats/parquet/file_writer.h"
#include "formats/parquet/utils.h"
#include "formats/utils.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"
#include "util/debug_util.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks {
class Chunk;
class ColumnHelper;
} // namespace starrocks

namespace starrocks::formats {

Status ParquetFileWriter::write(Chunk* chunk) {
    if (_rowgroup_writer == nullptr) {
        _rowgroup_writer = std::make_unique<parquet::ChunkWriter>(
                _writer->AppendBufferedRowGroup(), _type_descs, _schema, _eval_func, _writer_options->time_zone,
                _writer_options->use_legacy_decimal_encoding, _writer_options->use_int96_timestamp_encoding);
    }

    RETURN_IF_ERROR(_rowgroup_writer->write(chunk));

    if (_rowgroup_writer->estimated_buffered_bytes() >= _writer_options->rowgroup_size) {
        return _flush_row_group();
    }

    return Status::OK();
}

FileWriter::CommitResult ParquetFileWriter::commit() {
    FileWriter::CommitResult result{
            .io_status = Status::OK(), .format = PARQUET, .location = _location, .rollback_action = _rollback_action};
    try {
        if (_writer != nullptr) {
            _writer->Close();
        }
    } catch (const ::parquet::ParquetStatusException& e) {
        result.io_status.update(Status::IOError(fmt::format("{}: {}", "close file error", e.what())));
    }

    if (auto status = _output_stream->Close(); !status.ok()) {
        result.io_status.update(Status::IOError(fmt::format("{}: {}", "close output stream error", status.message())));
    }

    if (result.io_status.ok()) {
        result.file_statistics = _statistics(_writer->metadata().get(), _writer_options->column_ids.has_value());
        result.file_statistics.file_size = _output_stream->Tell().MoveValueUnsafe();
    }

    _writer = nullptr;
    return result;
}

int64_t ParquetFileWriter::get_written_bytes() {
    int n = _output_stream->Tell().MoveValueUnsafe();
    if (_rowgroup_writer != nullptr) {
        n += _rowgroup_writer->estimated_buffered_bytes();
    }
    return n;
}

int64_t ParquetFileWriter::get_allocated_bytes() {
    return _memory_pool.bytes_allocated();
}

int64_t ParquetFileWriter::get_flush_batch_size() {
    return _writer_options->rowgroup_size;
}

Status ParquetFileWriter::_flush_row_group() {
    DCHECK(_rowgroup_writer != nullptr);
    try {
        _rowgroup_writer->close();
    } catch (const ::parquet::ParquetStatusException& e) {
        Status exception = Status::IOError(fmt::format("{}: {}", "flush rowgroup error", e.what()));
        LOG(WARNING) << exception;
        return exception;
    }

    _rowgroup_writer = nullptr;
    return Status::OK();
}

#define MERGE_STATS_CASE(ParquetType)                                                                              \
    case ParquetType: {                                                                                            \
        auto typed_left_stat =                                                                                     \
                std::static_pointer_cast<::parquet::TypedStatistics<::parquet::PhysicalType<ParquetType>>>(left);  \
        auto typed_right_stat =                                                                                    \
                std::static_pointer_cast<::parquet::TypedStatistics<::parquet::PhysicalType<ParquetType>>>(right); \
        typed_left_stat->Merge(*typed_right_stat);                                                                 \
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

FileWriter::FileStatistics ParquetFileWriter::_statistics(const ::parquet::FileMetaData* meta_data, bool has_field_id) {
    DCHECK(meta_data != nullptr);
    FileWriter::FileStatistics file_statistics;
    file_statistics.record_count = meta_data->num_rows();

    if (!has_field_id) {
        return file_statistics;
    }

    // rowgroup split offsets
    file_statistics.split_offsets = parquet::ParquetUtils::collect_split_offsets(*meta_data);

    // field_id -> column_stat
    std::map<int32_t, std::shared_ptr<::parquet::Statistics>> column_stats;
    std::map<int32_t, int64_t> column_sizes;
    std::map<int32_t, int64_t> value_counts;
    std::map<int32_t, int64_t> null_value_counts;
    std::map<int32_t, std::string> lower_bounds;
    std::map<int32_t, std::string> upper_bounds;
    bool has_null_count = false;
    bool has_min_max = false;

    // traverse stat of column chunk in each row group
    for (int col_idx = 0; col_idx < meta_data->num_columns(); col_idx++) {
        auto field_id = meta_data->schema()->Column(col_idx)->schema_node()->field_id();

        for (int rg_idx = 0; rg_idx < meta_data->num_row_groups(); rg_idx++) {
            auto column_chunk_meta = meta_data->RowGroup(rg_idx)->ColumnChunk(col_idx);
            column_sizes[field_id] += column_chunk_meta->total_compressed_size();

            if (column_chunk_meta->is_stats_set()) {
                auto column_stat = column_chunk_meta->statistics();
                if (!column_stats.count(field_id)) {
                    column_stats[field_id] = column_stat;
                } else {
                    merge_stats(column_stats[field_id], column_stat);
                }
            }
        }
    }

    for (auto& [field_id, column_stat] : column_stats) {
        value_counts[field_id] = column_stat->num_values();
        if (column_stat->HasNullCount()) {
            has_null_count = true;
            null_value_counts[field_id] = column_stat->null_count();
            value_counts[field_id] += column_stat->null_count();
        }
        if (column_stat->HasMinMax()) {
            has_min_max = true;
            lower_bounds[field_id] = column_stat->EncodeMin();
            upper_bounds[field_id] = column_stat->EncodeMax();
        }
    }

    file_statistics.column_sizes = std::move(column_sizes);
    file_statistics.value_counts = std::move(value_counts);
    if (has_null_count) {
        file_statistics.null_value_counts = std::move(null_value_counts);
    }
    if (has_min_max) {
        file_statistics.lower_bounds = std::move(lower_bounds);
        file_statistics.upper_bounds = std::move(upper_bounds);
    }

    return file_statistics;
}

ParquetFileWriter::ParquetFileWriter(std::string location, std::shared_ptr<arrow::io::OutputStream> output_stream,
                                     std::vector<std::string> column_names, std::vector<TypeDescriptor> type_descs,
                                     std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                                     TCompressionType::type compression_type,
                                     std::shared_ptr<ParquetWriterOptions> writer_options,
                                     std::function<void()> rollback_action, std::vector<bool> nullable)
        : _location(std::move(location)),
          _output_stream(std::move(output_stream)),
          _column_names(std::move(column_names)),
          _type_descs(std::move(type_descs)),
          _column_evaluators(std::move(column_evaluators)),
          _compression_type(compression_type),
          _writer_options(std::move(writer_options)),
          _nullable(std::move(nullable)),
          _rollback_action(std::move(rollback_action)) {}

arrow::Result<std::shared_ptr<::parquet::schema::GroupNode>> ParquetFileWriter::_make_schema(
        const std::vector<std::string>& column_names, const std::vector<TypeDescriptor>& type_descs,
        const std::vector<FileColumnId>& file_column_ids, const std::vector<bool>& nullable) {
    ::parquet::schema::NodeVector fields;
    parquet::ParquetSchemaOptions schema_options{
            .use_legacy_decimal_encoding = _writer_options->use_legacy_decimal_encoding,
            .use_int96_timestamp_encoding = _writer_options->use_int96_timestamp_encoding,
    };
    for (int i = 0; i < type_descs.size(); i++) {
        ::parquet::Repetition::type repetition =
                (nullable.empty() || nullable[i]) ? ::parquet::Repetition::OPTIONAL : ::parquet::Repetition::REQUIRED;
        ARROW_ASSIGN_OR_RAISE(auto node,
                              parquet::ParquetBuildHelper::make_schema_node(column_names[i], type_descs[i], repetition,
                                                                            file_column_ids[i], schema_options))
        DCHECK(node != nullptr);
        fields.push_back(std::move(node));
    }
    return std::static_pointer_cast<::parquet::schema::GroupNode>(
            ::parquet::schema::GroupNode::Make("table", ::parquet::Repetition::REQUIRED, std::move(fields)));
}

Status ParquetFileWriter::init() {
    for (auto& e : _column_evaluators) {
        RETURN_IF_ERROR(e->init());
    }
    _eval_func = [&](Chunk* chunk, size_t col_idx) { return _column_evaluators[col_idx]->evaluate(chunk); };

    auto status = [&]() {
        if (_writer_options->column_ids.has_value()) {
            ARROW_ASSIGN_OR_RAISE(
                    _schema, _make_schema(_column_names, _type_descs, _writer_options->column_ids.value(), _nullable));
        } else {
            std::vector<FileColumnId> column_ids(_type_descs.size());
            ARROW_ASSIGN_OR_RAISE(_schema, _make_schema(_column_names, _type_descs, column_ids, _nullable));
        }
        return arrow::Status::OK();
    }();

    if (!status.ok()) {
        return Status::NotSupported(status.message());
    }

    ASSIGN_OR_RETURN(auto compression, parquet::ParquetBuildHelper::convert_compression_type(_compression_type));
    _properties = std::make_unique<::parquet::WriterProperties::Builder>()
                          ->version(_writer_options->version)
                          ->enable_write_page_index()
                          ->data_pagesize(_writer_options->page_size)
                          ->write_batch_size(_writer_options->write_batch_size)
                          ->dictionary_pagesize_limit(_writer_options->dictionary_pagesize)
                          ->compression(compression)
                          ->created_by(fmt::format("{} starrocks-{}", CREATED_BY_VERSION, get_short_version()))
                          ->memory_pool(&_memory_pool)
                          ->build();

    _writer = ::parquet::ParquetFileWriter::Open(_output_stream, _schema, _properties);
    return Status::OK();
}

ParquetFileWriter::~ParquetFileWriter() = default;

ParquetFileWriterFactory::ParquetFileWriterFactory(
        std::shared_ptr<FileSystem> fs, TCompressionType::type compression_type,
        std::map<std::string, std::string> options, std::vector<std::string> column_names,
        std::shared_ptr<std::vector<std::unique_ptr<ColumnEvaluator>>> column_evaluators,
        std::optional<std::vector<formats::FileColumnId>> field_ids, PriorityThreadPool* executors,
        RuntimeState* runtime_state, std::vector<bool> nullable)
        : _fs(std::move(fs)),
          _compression_type(compression_type),
          _field_ids(std::move(field_ids)),
          _options(std::move(options)),
          _column_names(std::move(column_names)),
          _column_evaluators(std::move(column_evaluators)),
          _executors(executors),
          _runtime_state(runtime_state),
          _nullable(std::move(nullable)) {}

Status ParquetFileWriterFactory::init() {
    RETURN_IF_ERROR(ColumnEvaluator::init(*_column_evaluators));
    _parsed_options = std::make_shared<ParquetWriterOptions>();
    _parsed_options->column_ids = _field_ids;
    if (_options.contains(ParquetWriterOptions::USE_LEGACY_DECIMAL_ENCODING)) {
        _parsed_options->use_legacy_decimal_encoding =
                boost::iequals(_options[ParquetWriterOptions::USE_LEGACY_DECIMAL_ENCODING], "true");
    }
    if (_options.contains(ParquetWriterOptions::USE_INT96_TIMESTAMP_ENCODING)) {
        _parsed_options->use_int96_timestamp_encoding =
                boost::iequals(_options[ParquetWriterOptions::USE_INT96_TIMESTAMP_ENCODING], "true");
    }
    if (_options.contains(ParquetWriterOptions::VERSION)) {
        const std::string& version = _options[ParquetWriterOptions::VERSION];
        if (boost::iequals(version, "1.0")) {
            _parsed_options->version = ::parquet::ParquetVersion::PARQUET_1_0;
        } else if (boost::iequals(version, "2.4")) {
            _parsed_options->version = ::parquet::ParquetVersion::PARQUET_2_4;
        } else if (boost::iequals(version, "2.6")) {
            _parsed_options->version = ::parquet::ParquetVersion::PARQUET_2_6;
        } else {
            return Status::NotSupported(fmt::format("parquet version {} is not supported", version));
        }
    }
#ifndef BE_TEST
    _parsed_options->time_zone = _runtime_state->timezone();
#endif
    return Status::OK();
}

StatusOr<WriterAndStream> ParquetFileWriterFactory::create(const std::string& path) const {
    ASSIGN_OR_RETURN(auto file, _fs->new_writable_file(WritableFileOptions{.direct_write = true,
                                                                           .content_type = http::ContentType::PARQUET},
                                                       path));
    VLOG(3) << "create parquet file, path=" << path;
    auto rollback_action = [fs = _fs, path = path]() {
        WARN_IF_ERROR(ignore_not_found(fs->delete_file(path)), "fail to delete file");
    };
    auto column_evaluators = ColumnEvaluator::clone(*_column_evaluators);
    auto types = ColumnEvaluator::types(*_column_evaluators);
    auto async_output_stream =
            std::make_unique<io::AsyncFlushOutputStream>(std::move(file), _executors, _runtime_state);
    auto parquet_output_stream = std::make_shared<parquet::AsyncParquetOutputStream>(async_output_stream.get());
    auto writer = std::make_unique<ParquetFileWriter>(path, parquet_output_stream, _column_names, types,
                                                      std::move(column_evaluators), _compression_type, _parsed_options,
                                                      rollback_action, _nullable);
    return WriterAndStream{
            .writer = std::move(writer),
            .stream = std::move(async_output_stream),
    };
}

} // namespace starrocks::formats
