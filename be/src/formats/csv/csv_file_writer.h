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

#pragma once

#include "formats/csv/converter.h"
#include "formats/csv/output_stream.h"
#include "formats/file_writer.h"

namespace starrocks::formats {

struct CSVWriterOptions : FileWriterOptions {
    std::string column_terminated_by = ",";
    std::string line_terminated_by = "\n";

    inline static std::string COLUMN_TERMINATED_BY = "column_terminated_by";
    inline static std::string LINE_TERMINATED_BY = "line_terminated_by";
};

// The primary purpose of this class is to support hive + csv. Use with caution in other cases.
// TODO(letian-jiang): support escaping
class CSVFileWriter final : public FileWriter {
public:
    CSVFileWriter(std::string location, std::shared_ptr<csv::OutputStream> output_stream,
                  std::vector<std::string> column_names, std::vector<TypeDescriptor> types,
                  std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                  TCompressionType::type compression_type, std::shared_ptr<CSVWriterOptions> writer_options,
                  std::function<void()> rollback_action);

    ~CSVFileWriter() override;

    Status init() override;

    int64_t get_written_bytes() override;

    int64_t get_allocated_bytes() override;

    Status write(Chunk* chunk) override;

    CommitResult commit() override;

private:
    const std::string _location;
    std::shared_ptr<csv::OutputStream> _output_stream;
    const std::vector<std::string> _column_names;
    const std::vector<TypeDescriptor> _types;
    std::vector<std::unique_ptr<ColumnEvaluator>> _column_evaluators;
    TCompressionType::type _compression_type = TCompressionType::UNKNOWN_COMPRESSION;
    std::shared_ptr<CSVWriterOptions> _writer_options;
    const std::function<void()> _rollback_action;

    int64_t _num_rows = 0;
    // (nullable converter, not-null converter)
    std::vector<std::pair<std::unique_ptr<csv::Converter>, std::unique_ptr<csv::Converter>>> _column_converters;
};

class CSVFileWriterFactory : public FileWriterFactory {
public:
    CSVFileWriterFactory(std::shared_ptr<FileSystem> fs, TCompressionType::type compression_type,
                         std::map<std::string, std::string> options, std::vector<std::string> column_names,
                         std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                         PriorityThreadPool* executors, RuntimeState* runtime_state);

    Status init() override;

    StatusOr<WriterAndStream> create(const std::string& path) const override;

private:
    std::shared_ptr<FileSystem> _fs;
    TCompressionType::type _compression_type = TCompressionType::UNKNOWN_COMPRESSION;
    std::map<std::string, std::string> _options;
    std::shared_ptr<CSVWriterOptions> _parsed_options;

    std::vector<std::string> _column_names;
    std::vector<std::unique_ptr<ColumnEvaluator>> _column_evaluators;
    PriorityThreadPool* _executors = nullptr;
    RuntimeState* _runtime_state = nullptr;
};

} // namespace starrocks::formats
