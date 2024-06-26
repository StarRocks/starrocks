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

#include "column/vectorized_fwd.h"
#include "formats/file_writer.h"
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
} // namespace starrocks

namespace starrocks::formats {

std::future<Status> ParquetFileWriter::write(ChunkPtr chunk) {
    if (_rowgroup_writer == nullptr) {
        _rowgroup_writer = std::make_unique<parquet::ChunkWriter>(
                _writer->AppendBufferedRowGroup(), _type_descs, _schema, _eval_func, _writer_options->time_zone,
                _writer_options->use_legacy_decimal_encoding, _writer_options->use_int96_timestamp_encoding);
    }
    if (auto status = _rowgroup_writer->write(chunk.get()); !status.ok()) {
        return make_ready_future(std::move(status));
    }
    if (_rowgroup_writer->estimated_buffered_bytes() >= _writer_options->rowgroup_size) {
        return _flush_row_group();
    }
    return make_ready_future(Status::OK());
}

std::future<FileWriter::CommitResult> ParquetFileWriter::commit() {
    auto promise = std::make_shared<std::promise<FileWriter::CommitResult>>();
    std::future<FileWriter::CommitResult> future = promise->get_future();

    auto task = [writer = _writer, output_stream = _output_stream, p = promise,
                 has_field_id = _writer_options->column_ids.has_value(), rollback = _rollback_action,
                 location = _location, state = _runtime_state, execution_state = _execution_state] {
#ifndef BE_TEST
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state->instance_mem_tracker());
        CurrentThread::current().set_query_id(state->query_id());
        CurrentThread::current().set_fragment_instance_id(state->fragment_instance_id());
#endif
        {
            // commit until all rowgroup flushing tasks have been done
            std::unique_lock lock(execution_state->mu);
            execution_state->cv.wait(lock, [&]() { return !execution_state->has_unfinished_task; });
        }

        FileWriter::CommitResult result{
                .io_status = Status::OK(), .format = PARQUET, .location = location, .rollback_action = rollback};
        try {
            writer->Close();
        } catch (const ::parquet::ParquetStatusException& e) {
            result.io_status.update(Status::IOError(fmt::format("{}: {}", "close file error", e.what())));
        }

        if (auto status = output_stream->Close(); !status.ok()) {
            result.io_status.update(
                    Status::IOError(fmt::format("{}: {}", "close output stream error", status.message())));
        }

        if (result.io_status.ok()) {
            result.file_statistics = _statistics(writer->metadata().get(), has_field_id);
            result.file_statistics.file_size = output_stream->Tell().MoveValueUnsafe();
        }

        p->set_value(result);
    };

    if (_executors) {
        bool ok = _executors->try_offer(task);
        if (!ok) {
            Status exception = Status::ResourceBusy("submit close file task fails");
            LOG(WARNING) << exception;
            promise->set_value(FileWriter::CommitResult{.io_status = exception, .rollback_action = _rollback_action});
        }
    } else {
        task();
    }

    _writer = nullptr;
    return future;
}

int64_t ParquetFileWriter::get_written_bytes() {
    int n = _output_stream->Tell().MoveValueUnsafe();
    if (_rowgroup_writer != nullptr) {
        n += _rowgroup_writer->estimated_buffered_bytes();
    }
    return n;
}

std::future<Status> ParquetFileWriter::_flush_row_group() {
    DCHECK(_rowgroup_writer != nullptr);
    auto promise = std::make_shared<std::promise<Status>>();
    std::future<Status> future = promise->get_future();

    auto task = [rowgroup_writer = _rowgroup_writer, p = promise, state = _runtime_state,
                 execution_state = _execution_state] {
#ifndef BE_TEST
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(state->instance_mem_tracker());
        CurrentThread::current().set_query_id(state->query_id());
        CurrentThread::current().set_fragment_instance_id(state->fragment_instance_id());
#endif
        try {
            rowgroup_writer->close();
            p->set_value(Status::OK());
        } catch (const ::parquet::ParquetStatusException& e) {
            Status exception = Status::IOError(fmt::format("{}: {}", "flush rowgroup error", e.what()));
            LOG(WARNING) << exception;
            p->set_value(exception);
        }

        {
            std::lock_guard lock(execution_state->mu);
            execution_state->has_unfinished_task = false;
            execution_state->cv.notify_one();
        }
    };

    {
        std::lock_guard lock(_execution_state->mu);
        _execution_state->has_unfinished_task = true;
    }

    if (_executors) {
        bool ok = _executors->try_offer(task);
        if (!ok) {
            Status exception = Status::ResourceBusy("submit close file task fails");
            LOG(WARNING) << exception;
            promise->set_value(exception);
        }
    } else {
        task();
    }

    _rowgroup_writer = nullptr;
    return future;
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
    std::vector<int64_t> split_offsets;
    for (int i = 0; i < meta_data->num_row_groups(); i++) {
        auto first_column_meta = meta_data->RowGroup(i)->ColumnChunk(0);
        int64_t dict_page_offset = first_column_meta->dictionary_page_offset();
        int64_t first_data_page_offset = first_column_meta->data_page_offset();
        int64_t split_offset = dict_page_offset > 0 && dict_page_offset < first_data_page_offset
                                       ? dict_page_offset
                                       : first_data_page_offset;
        split_offsets.push_back(split_offset);
    }
    file_statistics.split_offsets = split_offsets;

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

ParquetFileWriter::ParquetFileWriter(
        const std::string& location, std::unique_ptr<parquet::ParquetOutputStream> output_stream,
        const std::vector<std::string>& column_names, const std::vector<TypeDescriptor>& type_descs,
        std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators, TCompressionType::type compression_type,
        const std::shared_ptr<ParquetWriterOptions>& writer_options, const std::function<void()>& rollback_action,
        PriorityThreadPool* executors, RuntimeState* runtime_state)
        : _location(location),
          _output_stream(std::move(output_stream)),
          _column_names(column_names),
          _type_descs(type_descs),
          _column_evaluators(std::move(column_evaluators)),
          _compression_type(compression_type),
          _writer_options(writer_options),
          _rollback_action(std::move(rollback_action)),
          _executors(executors),
          _runtime_state(runtime_state) {}

StatusOr<::parquet::Compression::type> ParquetFileWriter::_convert_compression_type(TCompressionType::type type) {
    ::parquet::Compression::type converted_type;
    switch (type) {
    case TCompressionType::NO_COMPRESSION: {
        converted_type = ::parquet::Compression::UNCOMPRESSED;
        break;
    }
    case TCompressionType::SNAPPY: {
        converted_type = ::parquet::Compression::SNAPPY;
        break;
    }
    case TCompressionType::GZIP: {
        converted_type = ::parquet::Compression::GZIP;
        break;
    }
    case TCompressionType::ZSTD: {
        converted_type = ::parquet::Compression::ZSTD;
        break;
    }
    case TCompressionType::LZ4: {
        converted_type = ::parquet::Compression::LZ4;
        break;
    }
    default: {
        return Status::NotSupported(fmt::format("not supported compression type {}", to_string(type)));
    }
    }

    // Check if arrow supports indicated compression type
    if (!::parquet::IsCodecSupported(converted_type)) {
        return Status::NotSupported(fmt::format("not supported compression codec {}", converted_type));
    }

    return converted_type;
}

arrow::Result<std::shared_ptr<::parquet::schema::GroupNode>> ParquetFileWriter::_make_schema(
        const std::vector<std::string>& column_names, const std::vector<TypeDescriptor>& type_descs,
        const std::vector<FileColumnId>& file_column_ids) {
    ::parquet::schema::NodeVector fields;
    for (int i = 0; i < type_descs.size(); i++) {
        ARROW_ASSIGN_OR_RAISE(auto node, _make_schema_node(column_names[i], type_descs[i],
                                                           ::parquet::Repetition::OPTIONAL, file_column_ids[i]))
        DCHECK(node != nullptr);
        fields.push_back(std::move(node));
    }
    return std::static_pointer_cast<::parquet::schema::GroupNode>(
            ::parquet::schema::GroupNode::Make("table", ::parquet::Repetition::REQUIRED, std::move(fields)));
}

arrow::Result<::parquet::schema::NodePtr> ParquetFileWriter::_make_schema_node(const std::string& name,
                                                                               const TypeDescriptor& type_desc,
                                                                               ::parquet::Repetition::type rep_type,
                                                                               FileColumnId file_column_id) {
    if (file_column_id.children.size() != type_desc.children.size()) {
        file_column_id.children = std::vector<FileColumnId>(type_desc.children.size());
    }

    switch (type_desc.type) {
    case TYPE_BOOLEAN: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::None(),
                                                      ::parquet::Type::BOOLEAN, -1, file_column_id.field_id);
    }
    case TYPE_TINYINT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(8, true),
                                                      ::parquet::Type::INT32, -1, file_column_id.field_id);
    }
    case TYPE_SMALLINT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(16, true),
                                                      ::parquet::Type::INT32, -1, file_column_id.field_id);
    }
    case TYPE_INT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(32, true),
                                                      ::parquet::Type::INT32, -1, file_column_id.field_id);
    }
    case TYPE_BIGINT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Int(64, true),
                                                      ::parquet::Type::INT64, -1, file_column_id.field_id);
    }
    case TYPE_FLOAT: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::None(),
                                                      ::parquet::Type::FLOAT, -1, file_column_id.field_id);
    }
    case TYPE_DOUBLE: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::None(),
                                                      ::parquet::Type::DOUBLE, -1, file_column_id.field_id);
    }
    case TYPE_BINARY:
    case TYPE_VARBINARY:
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::None(),
                                                      ::parquet::Type::BYTE_ARRAY, -1, file_column_id.field_id);
    case TYPE_CHAR:
    case TYPE_VARCHAR: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::String(),
                                                      ::parquet::Type::BYTE_ARRAY, -1, file_column_id.field_id);
    }
    case TYPE_DATE: {
        return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::LogicalType::Date(),
                                                      ::parquet::Type::INT32, -1, file_column_id.field_id);
    }
    case TYPE_DATETIME: {
        // Apache Hive version 3 or lower does not support reading timestamps encoded as INT64
        if (_writer_options->use_int96_timestamp_encoding) {
            return ::parquet::schema::PrimitiveNode::Make(name, rep_type, ::parquet::Type::INT96,
                                                          ::parquet::ConvertedType::NONE, -1, file_column_id.field_id);
        } else {
            return ::parquet::schema::PrimitiveNode::Make(
                    name, rep_type,
                    ::parquet::LogicalType::Timestamp(true, ::parquet::LogicalType::TimeUnit::unit::MICROS),
                    ::parquet::Type::INT64, -1, file_column_id.field_id);
        }
    }
    case TYPE_DECIMAL32:
    case TYPE_DECIMAL64:
    case TYPE_DECIMAL128: {
        // Apache Hive version 3 or lower does not support reading decimals encoded as INT32/INT64
        if (!_writer_options->use_legacy_decimal_encoding) {
            if (type_desc.type == TYPE_DECIMAL32) {
                return ::parquet::schema::PrimitiveNode::Make(
                        name, rep_type, ::parquet::LogicalType::Decimal(type_desc.precision, type_desc.scale),
                        ::parquet::Type::INT32, -1, file_column_id.field_id);
            }
            if (type_desc.type == TYPE_DECIMAL64) {
                return ::parquet::schema::PrimitiveNode::Make(
                        name, rep_type, ::parquet::LogicalType::Decimal(type_desc.precision, type_desc.scale),
                        ::parquet::Type::INT64, -1, file_column_id.field_id);
            }
        }
        return ::parquet::schema::PrimitiveNode::Make(
                name, rep_type, ::parquet::LogicalType::Decimal(type_desc.precision, type_desc.scale),
                ::parquet::Type::FIXED_LEN_BYTE_ARRAY, parquet::decimal_precision_to_byte_count(type_desc.precision),
                file_column_id.field_id);
    }
    case TYPE_STRUCT: {
        DCHECK(type_desc.children.size() == type_desc.field_names.size());
        ::parquet::schema::NodeVector fields;
        for (size_t i = 0; i < type_desc.children.size(); i++) {
            ARROW_ASSIGN_OR_RAISE(auto child, _make_schema_node(type_desc.field_names[i], type_desc.children[i],
                                                                ::parquet::Repetition::OPTIONAL,
                                                                file_column_id.children[i])); // use optional as default
            fields.push_back(std::move(child));
        }
        return ::parquet::schema::GroupNode::Make(name, rep_type, fields, ::parquet::ConvertedType::NONE,
                                                  file_column_id.field_id);
    }
    case TYPE_ARRAY: {
        DCHECK(type_desc.children.size() == 1);
        ARROW_ASSIGN_OR_RAISE(auto element,
                              _make_schema_node("element", type_desc.children[0], ::parquet::Repetition::OPTIONAL,
                                                file_column_id.children[0])); // use optional as default
        auto list = ::parquet::schema::GroupNode::Make("list", ::parquet::Repetition::REPEATED, {element});
        return ::parquet::schema::GroupNode::Make(name, rep_type, {list}, ::parquet::LogicalType::List(),
                                                  file_column_id.field_id);
    }
    case TYPE_MAP: {
        DCHECK(type_desc.children.size() == 2);
        ARROW_ASSIGN_OR_RAISE(auto key, _make_schema_node("key", type_desc.children[0], ::parquet::Repetition::REQUIRED,
                                                          file_column_id.children[0]))
        ARROW_ASSIGN_OR_RAISE(auto value,
                              _make_schema_node("value", type_desc.children[1], ::parquet::Repetition::OPTIONAL,
                                                file_column_id.children[1]));
        auto key_value = ::parquet::schema::GroupNode::Make("key_value", ::parquet::Repetition::REPEATED, {key, value});
        return ::parquet::schema::GroupNode::Make(name, rep_type, {key_value}, ::parquet::LogicalType::Map(),
                                                  file_column_id.field_id);
    }
    case TYPE_TIME: {
        return ::parquet::schema::PrimitiveNode::Make(
                name, rep_type, ::parquet::LogicalType::Time(false, ::parquet::LogicalType::TimeUnit::MICROS),
                ::parquet::Type::INT64, -1, file_column_id.field_id);
    }
    default: {
        return arrow::Status::TypeError(fmt::format("Doesn't support to write {} type data", type_desc.debug_string()));
    }
    }
}

Status ParquetFileWriter::init() {
    for (auto& e : _column_evaluators) {
        RETURN_IF_ERROR(e->init());
    }
    _eval_func = [&](Chunk* chunk, size_t col_idx) { return _column_evaluators[col_idx]->evaluate(chunk); };

    auto status = [&]() {
        if (_writer_options->column_ids.has_value()) {
            ARROW_ASSIGN_OR_RAISE(_schema,
                                  _make_schema(_column_names, _type_descs, _writer_options->column_ids.value()));
        } else {
            std::vector<FileColumnId> column_ids(_type_descs.size());
            ARROW_ASSIGN_OR_RAISE(_schema, _make_schema(_column_names, _type_descs, column_ids));
        }
        return arrow::Status::OK();
    }();

    if (!status.ok()) {
        return Status::NotSupported(status.message());
    }

    ASSIGN_OR_RETURN(auto compression, _convert_compression_type(_compression_type));
    _properties = std::make_unique<::parquet::WriterProperties::Builder>()
                          ->version(::parquet::ParquetVersion::PARQUET_2_6)
                          ->enable_write_page_index()
                          ->data_pagesize(_writer_options->page_size)
                          ->write_batch_size(_writer_options->write_batch_size)
                          ->dictionary_pagesize_limit(_writer_options->dictionary_pagesize)
                          ->compression(compression)
                          ->created_by(fmt::format("{} starrocks-{}", CREATED_BY_VERSION, get_short_version()))
                          ->build();

    _writer = ::parquet::ParquetFileWriter::Open(_output_stream, _schema, _properties);
    return Status::OK();
}

ParquetFileWriter::~ParquetFileWriter() = default;

ParquetFileWriterFactory::ParquetFileWriterFactory(std::shared_ptr<FileSystem> fs,
                                                   TCompressionType::type compression_type,
                                                   const std::map<std::string, std::string>& options,
                                                   const std::vector<std::string>& column_names,
                                                   std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                                                   std::optional<std::vector<formats::FileColumnId>> field_ids,
                                                   PriorityThreadPool* executors, RuntimeState* runtime_state)
        : _fs(std::move(fs)),
          _compression_type(compression_type),
          _field_ids(std::move(field_ids)),
          _options(options),
          _column_names(column_names),
          _column_evaluators(std::move(column_evaluators)),
          _executors(executors),
          _runtime_state(runtime_state) {}

Status ParquetFileWriterFactory::init() {
    RETURN_IF_ERROR(ColumnEvaluator::init(_column_evaluators));
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
#ifndef BE_TEST
    _parsed_options->time_zone = _runtime_state->timezone();
#endif
    return Status::OK();
}

StatusOr<std::shared_ptr<FileWriter>> ParquetFileWriterFactory::create(const std::string& path) const {
    ASSIGN_OR_RETURN(auto file, _fs->new_writable_file(path));
    auto rollback_action = [fs = _fs, path = path]() {
        WARN_IF_ERROR(ignore_not_found(fs->delete_file(path)), "fail to delete file");
    };
    auto column_evaluators = ColumnEvaluator::clone(_column_evaluators);
    auto types = ColumnEvaluator::types(_column_evaluators);
    auto output_stream = std::make_unique<parquet::ParquetOutputStream>(std::move(file));
    return std::make_shared<ParquetFileWriter>(path, std::move(output_stream), _column_names, types,
                                               std::move(column_evaluators), _compression_type, _parsed_options,
                                               rollback_action, _executors, _runtime_state);
}

} // namespace starrocks::formats
