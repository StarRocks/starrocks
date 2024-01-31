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

#include <orc/Writer.hh>
#include <util/priority_thread_pool.hpp>

#include "formats/file_writer.h"
#include "orc_chunk_writer.h"

namespace starrocks::formats {

class ORCWriterOptions : public FileWriterOptions {
    int64_t stripe_size = 1 << 27; // 128MB
};

class ORCFileWriter final : public FileWriter {
public:
    ORCFileWriter(const std::string& location, std::unique_ptr<OrcOutputStream> output_stream,
                  const std::vector<std::string>& column_names, const std::vector<TypeDescriptor>& type_descs,
                  std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                  const std::shared_ptr<ORCWriterOptions>& writer_options, const std::function<void()> rollback_action,
                  PriorityThreadPool* executors);

    ~ORCFileWriter() override = default;

    Status init() override;

    int64_t get_written_bytes() override;

    std::future<Status> write(ChunkPtr chunk) override;

    std::future<CommitResult> commit() override;

private:
    static StatusOr<std::unique_ptr<orc::Type>> _make_schema(const std::vector<std::string>& column_names,
                                                             const std::vector<TypeDescriptor>& type_descs);

    static StatusOr<std::unique_ptr<orc::Type>> _make_schema_node(const TypeDescriptor& type_desc);

    StatusOr<std::unique_ptr<orc::ColumnVectorBatch>> _convert(ChunkPtr chunk);

    void _write_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, const TypeDescriptor& type_desc);

    template <LogicalType Type, typename VectorBatchType>
    void _write_number(orc::ColumnVectorBatch& orc_column, ColumnPtr& column);

    void _write_string(orc::ColumnVectorBatch& orc_column, ColumnPtr& column);

    void _write_decimal(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, int precision, int scale);

    template <LogicalType DecimalType, typename VectorBatchType, typename T>
    void _write_decimal32or64or128(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, int precision, int scale);

    void _write_date(orc::ColumnVectorBatch& orc_column, ColumnPtr& column);

    void _write_datetime(orc::ColumnVectorBatch& orc_column, ColumnPtr& column);

    void _write_array_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, const TypeDescriptor& type);

    void _write_struct_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, const TypeDescriptor& type);

    void _write_map_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, const TypeDescriptor& type);

    const std::string _location;
    std::shared_ptr<OrcOutputStream> _output_stream;
    const std::vector<std::string> _column_names;
    const std::vector<TypeDescriptor> _type_descs;
    std::vector<std::unique_ptr<ColumnEvaluator>> _column_evaluators;

    std::unique_ptr<orc::Type> _schema;
    std::shared_ptr<orc::Writer> _writer;
    std::shared_ptr<ORCWriterOptions> _writer_options;
    int64_t _row_counter{0};

    std::function<void()> _rollback_action;
    // If provided, submit task to executors and return future to the caller. Otherwise execute synchronously.
    PriorityThreadPool* _executors;
};

class ORCFileWriterFactory : public FileWriterFactory {
public:
    ORCFileWriterFactory(std::shared_ptr<FileSystem> fs, const std::string& format,
                         const std::map<std::string, std::string>& options,
                         const std::vector<std::string>& column_names,
                         std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                         PriorityThreadPool* executors = nullptr);

    StatusOr<std::shared_ptr<FileWriter>> create(const std::string& path) override;

private:
    Status _init();

    std::shared_ptr<FileSystem> _fs;
    std::string _format;
    std::map<std::string, std::string> _options;
    std::shared_ptr<ORCWriterOptions> _parsed_options;

    std::vector<std::string> _column_names;
    std::vector<std::unique_ptr<ColumnEvaluator>> _column_evaluators;
    PriorityThreadPool* _executors;
};

} // namespace starrocks::formats
