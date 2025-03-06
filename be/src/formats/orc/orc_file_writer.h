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

#include <orc/OrcFile.hh>
#include <orc/Writer.hh>
#include <util/priority_thread_pool.hpp>

#include "formats/file_writer.h"
#include "orc_memory_pool.h"

namespace starrocks::formats {

class OrcOutputStream : public orc::OutputStream {
public:
    OrcOutputStream(std::unique_ptr<starrocks::WritableFile> wfile);

    ~OrcOutputStream() override;

    uint64_t getLength() const override;

    uint64_t getNaturalWriteSize() const override;

    void write(const void* buf, size_t length) override;

    void close() override;

    const std::string& getName() const override;

private:
    std::unique_ptr<starrocks::WritableFile> _wfile;
    bool _is_closed = false;
};

class AsyncOrcOutputStream : public orc::OutputStream {
public:
    AsyncOrcOutputStream(io::AsyncFlushOutputStream* _stream);

    ~AsyncOrcOutputStream() override = default;

    uint64_t getLength() const override;

    uint64_t getNaturalWriteSize() const override;

    void write(const void* buf, size_t length) override;

    void close() override;

    const std::string& getName() const override;

private:
    io::AsyncFlushOutputStream* _stream;
    bool _is_closed = false;
};

struct ORCWriterOptions : public FileWriterOptions {};

class ORCFileWriter final : public FileWriter {
public:
    ORCFileWriter(std::string location, std::shared_ptr<orc::OutputStream> output_stream,
                  std::vector<std::string> column_names, std::vector<TypeDescriptor> type_descs,
                  std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                  TCompressionType::type compression_type, std::shared_ptr<ORCWriterOptions> writer_options,
                  std::function<void()> rollback_action);

    ~ORCFileWriter() override = default;

    Status init() override;

    int64_t get_written_bytes() override;

    int64_t get_allocated_bytes() override;

    Status write(Chunk* chunk) override;

    CommitResult commit() override;

private:
    static StatusOr<orc::CompressionKind> _convert_compression_type(TCompressionType::type type);

    static StatusOr<std::unique_ptr<orc::Type>> _make_schema(const std::vector<std::string>& column_names,
                                                             const std::vector<TypeDescriptor>& type_descs);

    static StatusOr<std::unique_ptr<orc::Type>> _make_schema_node(const TypeDescriptor& type_desc);

    static void _populate_orc_notnull(orc::ColumnVectorBatch& orc_column, const uint8_t* null_column,
                                      size_t column_size);

    StatusOr<std::unique_ptr<orc::ColumnVectorBatch>> _convert(Chunk* chunk);

    Status _write_column(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, const TypeDescriptor& type_desc);

    template <LogicalType Type, typename VectorBatchType>
    Status _write_number(orc::ColumnVectorBatch& orc_column, ColumnPtr& column);

    Status _write_string(orc::ColumnVectorBatch& orc_column, ColumnPtr& column);

    template <LogicalType DecimalType, typename VectorBatchType, typename T>
    Status _write_decimal32or64or128(orc::ColumnVectorBatch& orc_column, ColumnPtr& column, int precision, int scale);

    Status _write_date(orc::ColumnVectorBatch& orc_column, ColumnPtr& column);

    Status _write_datetime(orc::ColumnVectorBatch& orc_column, ColumnPtr& column);

    inline static const std::string STARROCKS_ORC_WRITER_VERSION_KEY = "starrocks.writer.version";

    const std::string _location;
    std::shared_ptr<orc::OutputStream> _output_stream;
    const std::vector<std::string> _column_names;
    const std::vector<TypeDescriptor> _type_descs;
    std::vector<std::unique_ptr<ColumnEvaluator>> _column_evaluators;

    std::unique_ptr<orc::Type> _schema;
    std::shared_ptr<orc::Writer> _writer;
    OrcMemoryPool _memory_pool;
    TCompressionType::type _compression_type = TCompressionType::UNKNOWN_COMPRESSION;
    std::shared_ptr<ORCWriterOptions> _writer_options;
    int64_t _row_counter{0};
    std::function<void()> _rollback_action;
};

class ORCFileWriterFactory : public FileWriterFactory {
public:
    ORCFileWriterFactory(std::shared_ptr<FileSystem> fs, TCompressionType::type compression_type,
                         std::map<std::string, std::string> options, std::vector<std::string> column_names,
                         std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                         PriorityThreadPool* executors, RuntimeState* runtime_state);

    Status init() override;

    StatusOr<WriterAndStream> create(const std::string& path) const override;

private:
    std::shared_ptr<FileSystem> _fs;
    TCompressionType::type _compression_type = TCompressionType::UNKNOWN_COMPRESSION;
    std::map<std::string, std::string> _options;
    std::shared_ptr<ORCWriterOptions> _parsed_options;

    std::vector<std::string> _column_names;
    std::vector<std::unique_ptr<ColumnEvaluator>> _column_evaluators;
    PriorityThreadPool* _executors = nullptr;
    RuntimeState* _runtime_state = nullptr;
};

} // namespace starrocks::formats
