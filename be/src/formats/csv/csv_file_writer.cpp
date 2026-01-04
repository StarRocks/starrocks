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

#include "csv_file_writer.h"

#include <boost/algorithm/string.hpp>
#include <utility>

#include "common/http/content_type.h"
#include "csv_escape.h"
#include "exec/hdfs_scanner_text.h"
#include "formats/utils.h"
#include "output_stream_file.h"
#include "runtime/current_thread.h"
#include "util/compression/compression_utils.h"
#include "util/defer_op.h"

namespace starrocks::formats {

CSVFileWriter::CSVFileWriter(std::string location, std::shared_ptr<csv::OutputStream> output_stream,
                             std::vector<std::string> column_names, std::vector<TypeDescriptor> types,
                             std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                             std::shared_ptr<CSVWriterOptions> writer_options, std::function<void()> rollback_action)
        : _location(std::move(location)),
          _output_stream(std::move(output_stream)),
          _column_names(std::move(column_names)),
          _types(std::move(types)),
          _column_evaluators(std::move(column_evaluators)),
          _writer_options(std::move(writer_options)),
          _rollback_action(std::move(rollback_action)) {}

CSVFileWriter::~CSVFileWriter() = default;

Status CSVFileWriter::init() {
    RETURN_IF_ERROR(ColumnEvaluator::init(_column_evaluators));
    _column_converters.reserve(_types.size());
    for (auto& type : _types) {
        // ARRAY type is supported, other complex types (STRUCT, MAP) are not supported yet
        if (type.is_complex_type() && !type.is_array_type()) {
            return Status::InternalError(fmt::format("Type {} is not supported yet", type.debug_string()));
        }
        auto nullable_conv = csv::get_converter(type, true);
        if (nullable_conv == nullptr) {
            return Status::InternalError(fmt::format("No CSV converter for type: {}", type.debug_string()));
        }
        _column_converters.emplace_back(std::move(nullable_conv), csv::get_converter(type, false));
    }
    _converter_options = std::make_shared<csv::Converter::Options>();
    if (_writer_options->is_hive) {
        _converter_options->is_hive = true;
        _converter_options->array_format_type = csv::ArrayFormatType::kHive;
        _converter_options->array_hive_collection_delimiter = _writer_options->collection_delim.empty()
                                                                      ? DEFAULT_COLLECTION_DELIM.front()
                                                                      : _writer_options->collection_delim.front();
        _converter_options->array_hive_mapkey_delimiter = _writer_options->mapkey_delim.empty()
                                                                  ? DEFAULT_MAPKEY_DELIM.front()
                                                                  : _writer_options->mapkey_delim.front();
    }
    return Status::OK();
}

Status CSVFileWriter::_write_header() {
    if (_header_written) {
        return Status::OK();
    }

    // Write header row if include_header is enabled
    if (_writer_options->include_header) {
        if (_column_names.empty()) {
            LOG(WARNING)
                    << "include_header is enabled but column_names is empty, this may indicate an upstream logic issue";
        } else {
            for (size_t i = 0; i < _column_names.size(); i++) {
                // Escape column names that contain special characters (delimiter, quotes, newlines)
                std::string escaped_name =
                        csv::escape_csv_field(_column_names[i], _writer_options->column_terminated_by);
                RETURN_IF_ERROR(_output_stream->write(escaped_name));
                if (i + 1 != _column_names.size()) {
                    RETURN_IF_ERROR(_output_stream->write(_writer_options->column_terminated_by));
                }
            }
            RETURN_IF_ERROR(_output_stream->write(_writer_options->line_terminated_by));
        }
    }

    // Mark header as written only after all operations succeed
    _header_written = true;
    return Status::OK();
}

int64_t CSVFileWriter::get_written_bytes() {
    return _output_stream->size();
}

int64_t CSVFileWriter::get_allocated_bytes() {
    return _output_stream->buffer_size();
}

int64_t CSVFileWriter::get_flush_batch_size() {
    return 0;
}

Status CSVFileWriter::write(Chunk* chunk) {
    // Write header on first write
    RETURN_IF_ERROR(_write_header());

    _num_rows += chunk->num_rows();

    auto columns = Columns();
    for (auto& e : _column_evaluators) {
        ASSIGN_OR_RETURN(auto column, e->evaluate(chunk));
        columns.push_back(std::move(column));
    }

    for (size_t r = 0; r < chunk->num_rows(); r++) {
        for (size_t c = 0; c < columns.size(); c++) {
            csv::Converter* converter;
            if (columns[c]->is_nullable()) {
                converter = _column_converters[c].first.get();
            } else {
                converter = _column_converters[c].second.get();
            }
            RETURN_IF_ERROR(converter->write_string(_output_stream.get(), *columns[c], r, *_converter_options));
            if (c + 1 != columns.size()) {
                RETURN_IF_ERROR(_output_stream->write(_writer_options->column_terminated_by));
            }
        }
        RETURN_IF_ERROR(_output_stream->write(_writer_options->line_terminated_by));
    }

    return Status::OK();
}

FileWriter::CommitResult CSVFileWriter::commit() {
    FileWriter::CommitResult result{
            .io_status = Status::OK(), .format = CSV, .location = _location, .rollback_action = _rollback_action};

    // Ensure header is written even if no data was written
    if (auto st = _write_header(); !st.ok()) {
        result.io_status.update(st);
    }

    if (auto st = _output_stream->finalize(); !st.ok()) {
        result.io_status.update(st);
    }

    if (result.io_status.ok()) {
        result.file_statistics.record_count = _num_rows;
        result.file_statistics.file_size = _output_stream->size();
    }

    return result;
}

CSVFileWriterFactory::CSVFileWriterFactory(std::shared_ptr<FileSystem> fs, TCompressionType::type compression_type,
                                           std::map<std::string, std::string> options,
                                           std::vector<std::string> column_names,
                                           std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                                           PriorityThreadPool* executors, RuntimeState* runtime_state)
        : _fs(std::move(fs)),
          _compression_type(compression_type),
          _options(std::move(options)),
          _column_names(std::move(column_names)),
          _column_evaluators(std::move(column_evaluators)),
          _executors(executors),
          _runtime_state(runtime_state) {}

Status CSVFileWriterFactory::init() {
    for (auto& e : _column_evaluators) {
        RETURN_IF_ERROR(e->init());
    }
    _parsed_options = std::make_shared<CSVWriterOptions>();
    if (_options.contains(CSVWriterOptions::COLUMN_TERMINATED_BY)) {
        _parsed_options->column_terminated_by = _options[CSVWriterOptions::COLUMN_TERMINATED_BY];
    }
    if (_options.contains(CSVWriterOptions::LINE_TERMINATED_BY)) {
        _parsed_options->line_terminated_by = _options[CSVWriterOptions::LINE_TERMINATED_BY];
    }
    if (_options.contains(CSVWriterOptions::INCLUDE_HEADER)) {
        _parsed_options->include_header = boost::iequals(_options[CSVWriterOptions::INCLUDE_HEADER], "true");
    }
    if (_options.contains(CSVWriterOptions::COLLECTION_DELIM)) {
        _parsed_options->collection_delim = _options[CSVWriterOptions::COLLECTION_DELIM];
    }
    if (_options.contains(CSVWriterOptions::MAPKEY_DELIM)) {
        _parsed_options->mapkey_delim = _options[CSVWriterOptions::MAPKEY_DELIM];
    }
    if (_options.contains(CSVWriterOptions::IS_HIVE)) {
        _parsed_options->is_hive = _options[CSVWriterOptions::IS_HIVE] == "true";
    }
    return Status::OK();
}

StatusOr<WriterAndStream> CSVFileWriterFactory::create(const std::string& path) const {
    ASSIGN_OR_RETURN(auto file,
                     _fs->new_writable_file(
                             WritableFileOptions{.direct_write = true, .content_type = http::ContentType::CSV}, path));
    auto rollback_action = [fs = _fs, path = path]() {
        WARN_IF_ERROR(ignore_not_found(fs->delete_file(path)), "fail to delete file");
    };
    // Use CancelableDefer to ensure cleanup on any failure after file creation
    CancelableDefer cleanup_on_failure([&rollback_action]() { rollback_action(); });

    auto column_evaluators = ColumnEvaluator::clone(_column_evaluators);
    auto types = ColumnEvaluator::types(_column_evaluators);
    auto async_output_stream =
            std::make_unique<io::AsyncFlushOutputStream>(std::move(file), _executors, _runtime_state);

    // Create base async output stream
    auto base_stream = std::make_shared<csv::AsyncOutputStreamFile>(async_output_stream.get(), 1024 * 1024);

    // Wrap with compression if enabled (decorator pattern)
    std::shared_ptr<csv::OutputStream> csv_output_stream;
    CompressionTypePB compression_pb = CompressionUtils::to_compression_pb(_compression_type);
    // Only use compression if it's a valid, recognized compression type
    // (not UNKNOWN_COMPRESSION which is returned for AUTO, DEFAULT_COMPRESSION, etc.)
    if (compression_pb != CompressionTypePB::NO_COMPRESSION &&
        compression_pb != CompressionTypePB::UNKNOWN_COMPRESSION) {
        ASSIGN_OR_RETURN(csv_output_stream,
                         csv::CompressedOutputStream::create(base_stream, compression_pb, 1024 * 1024));
    } else {
        csv_output_stream = base_stream;
    }

    auto writer = std::make_unique<CSVFileWriter>(path, csv_output_stream, _column_names, types,
                                                  std::move(column_evaluators), _parsed_options, rollback_action);
    cleanup_on_failure.cancel(); // Prevent cleanup on success
    return WriterAndStream{
            .writer = std::move(writer),
            .stream = std::move(async_output_stream),
    };
}

} // namespace starrocks::formats
