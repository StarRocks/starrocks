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

#include "formats/utils.h"
#include "output_stream_file.h"

namespace starrocks::formats {

CSVFileWriter::CSVFileWriter(std::string location, std::unique_ptr<csv::OutputStream> output_stream,
                             const std::vector<std::string>& column_names, const std::vector<TypeDescriptor>& types,
                             std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                             const std::shared_ptr<CSVWriterOptions>& writer_options,
                             const std::function<void()> rollback_action, PriorityThreadPool* executors)
        : _location(std::move(location)),
          _output_stream(std::move(output_stream)),
          _column_names(column_names),
          _types(types),
          _column_evaluators(std::move(column_evaluators)),
          _writer_options(writer_options),
          _rollback_action(rollback_action),
          _executors(executors) {}

CSVFileWriter::~CSVFileWriter() = default;

Status CSVFileWriter::init() {
    RETURN_IF_ERROR(ColumnEvaluator::init(_column_evaluators));
    _column_converters.reserve(_types.size());
    for (auto& type : _types) {
        auto nullable_conv = csv::get_converter(type, true);
        if (nullable_conv == nullptr) {
            return Status::InternalError(fmt::format("No CSV converter for type: {}", type.debug_string()));
        }
        _column_converters.emplace_back(std::move(nullable_conv), csv::get_converter(type, false));
    }
    return Status::OK();
}

int64_t CSVFileWriter::get_written_bytes() {
    return _output_stream->size();
}

std::future<Status> CSVFileWriter::write(ChunkPtr chunk) {
    _num_rows += chunk->num_rows();

    auto columns = std::vector<ColumnPtr>();
    for (auto& e : _column_evaluators) {
        auto maybe_column = e->evaluate(chunk.get());
        if (!maybe_column.ok()) {
            return make_ready_future(maybe_column.status());
        }
        columns.push_back(maybe_column.value());
    }

    for (size_t r = 0; r < chunk->num_rows(); r++) {
        for (size_t c = 0; c < columns.size(); c++) {
            csv::Converter* converter;
            if (columns[c]->is_nullable()) {
                converter = _column_converters[c].first.get();
            } else {
                converter = _column_converters[c].second.get();
            }

            if (auto st = converter->write_string(_output_stream.get(), *columns[c], r, {}); !st.ok()) {
                return make_ready_future(std::move(st));
            }

            if (c != columns.size() - 1) {
                if (auto st = _output_stream->write(_writer_options->column_terminated_by); !st.ok()) {
                    return make_ready_future(std::move(st));
                }
            }
        }

        if (auto st = _output_stream->write(_writer_options->line_terminated_by); !st.ok()) {
            return make_ready_future(std::move(st));
        }
    }

    return make_ready_future(Status::OK());
}

std::future<FileWriter::CommitResult> CSVFileWriter::commit() {
    auto promise = std::make_shared<std::promise<FileWriter::CommitResult>>();
    std::future<FileWriter::CommitResult> future = promise->get_future();

    auto task = [output_stream = _output_stream, p = promise, rollback = _rollback_action, row_counter = _num_rows,
                 location = _location] {
        FileWriter::CommitResult result{
                .io_status = Status::OK(), .format = CSV, .location = location, .rollback_action = rollback};

        if (auto st = output_stream->finalize(); !st.ok()) {
            result.io_status.update(st);
        }

        if (result.io_status.ok()) {
            result.file_statistics.record_count = row_counter;
            result.file_statistics.file_size = output_stream->size();
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

    return future;
}

CSVFileWriterFactory::CSVFileWriterFactory(std::shared_ptr<FileSystem> fs,
                                           const std::map<std::string, std::string>& options,
                                           const std::vector<std::string>& column_names,
                                           std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                                           PriorityThreadPool* executors)
        : _fs(std::move(fs)),
          _options(options),
          _column_names(column_names),
          _column_evaluators(std::move(column_evaluators)),
          _executors(executors) {}

Status CSVFileWriterFactory::init() {
    for (auto& e : _column_evaluators) {
        RETURN_IF_ERROR(e->init());
    }
    _parsed_options = std::make_shared<CSVWriterOptions>();
    return Status::OK();
}

StatusOr<std::shared_ptr<FileWriter>> CSVFileWriterFactory::create(const std::string& path) {
    ASSIGN_OR_RETURN(auto file, _fs->new_writable_file(path));
    auto rollback_action = [fs = _fs, path = path]() {
        WARN_IF_ERROR(ignore_not_found(fs->delete_file(path)), "fail to delete file");
    };
    auto column_evaluators = ColumnEvaluator::clone(_column_evaluators);
    auto types = ColumnEvaluator::types(_column_evaluators);
    auto output_stream = std::make_unique<csv::OutputStreamFile>(std::move(file), 1024);
    return std::make_shared<CSVFileWriter>(path, std::move(output_stream), _column_names, types,
                                           std::move(column_evaluators), _parsed_options, rollback_action, _executors);
}

} // namespace starrocks::formats
