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

#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <gen_cpp/DataSinks_types.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <parquet/platform.h>
#include <parquet/schema.h>
#include <parquet/types.h>
#include <stddef.h>
#include <stdint.h>

#include <condition_variable>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow_memory_pool.h"
#include "column/chunk.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/function_context.h"
#include "formats/column_evaluator.h"
#include "formats/file_writer.h"
#include "formats/parquet/arrow_memory_pool.h"
#include "formats/parquet/chunk_writer.h"
#include "formats/parquet/file_writer.h"
#include "formats/utils.h"
#include "fs/fs.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/priority_thread_pool.hpp"

namespace parquet {
class FileMetaData;
class ParquetFileWriter;
class WriterProperties;
} // namespace parquet
namespace starrocks {
class Chunk;
class FileSystem;
class PriorityThreadPool;
class RuntimeState;

namespace parquet {
class ChunkWriter;
class ParquetOutputStream;
} // namespace parquet
} // namespace starrocks

namespace starrocks::formats {

struct FileColumnId {
    int32_t field_id = -1;
    std::vector<FileColumnId> children;
};

struct ParquetWriterOptions : FileWriterOptions {
    int64_t dictionary_pagesize = 1024 * 1024; // 1MB
    int64_t page_size = 1024 * 1024;           // 1MB
    int64_t write_batch_size = 4096;
    int64_t rowgroup_size = 128L * 1024 * 1024; // 128MB
    std::optional<std::vector<FileColumnId>> column_ids = std::nullopt;
    std::string time_zone = TimezoneUtils::default_time_zone;
    bool use_legacy_decimal_encoding = false;
    bool use_int96_timestamp_encoding = false;

    inline static std::string USE_LEGACY_DECIMAL_ENCODING = "use_legacy_decimal_encoding";
    inline static std::string USE_INT96_TIMESTAMP_ENCODING = "use_int96_timestamp_encoding";
};

class ParquetFileWriter final : public FileWriter {
public:
    ParquetFileWriter(std::string location, std::shared_ptr<arrow::io::OutputStream> output_stream,
                      std::vector<std::string> column_names, std::vector<TypeDescriptor> type_descs,
                      std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                      TCompressionType::type compression_type, std::shared_ptr<ParquetWriterOptions> writer_options,
                      const std::function<void()>& rollback_action);

    ~ParquetFileWriter() override;

    Status init() override;

    int64_t get_written_bytes() override;

    int64_t get_allocated_bytes() override;

    Status write(Chunk* chunk) override;

    CommitResult commit() override;

private:
    static StatusOr<::parquet::Compression::type> _convert_compression_type(TCompressionType::type type);

    arrow::Result<std::shared_ptr<::parquet::schema::GroupNode>> _make_schema(
            const std::vector<std::string>& file_column_names, const std::vector<TypeDescriptor>& type_descs,
            const std::vector<FileColumnId>& file_column_ids);

    arrow::Result<::parquet::schema::NodePtr> _make_schema_node(const std::string& name,
                                                                const TypeDescriptor& type_desc,
                                                                ::parquet::Repetition::type rep_type,
                                                                FileColumnId file_column_id);

    static FileStatistics _statistics(const ::parquet::FileMetaData* meta_data, bool has_field_id);

    Status _flush_row_group();

    std::shared_ptr<::parquet::WriterProperties> _properties;
    std::shared_ptr<::parquet::schema::GroupNode> _schema;

    const std::string _location;
    std::shared_ptr<arrow::io::OutputStream> _output_stream;
    ArrowMemoryPool _memory_pool;
    const std::vector<std::string> _column_names;
    const std::vector<TypeDescriptor> _type_descs;
    std::vector<std::unique_ptr<ColumnEvaluator>> _column_evaluators;
    TCompressionType::type _compression_type = TCompressionType::UNKNOWN_COMPRESSION;
    std::shared_ptr<ParquetWriterOptions> _writer_options;
    std::function<StatusOr<ColumnPtr>(Chunk*, size_t)> _eval_func;

    std::shared_ptr<::parquet::ParquetFileWriter> _writer;
    std::shared_ptr<parquet::ChunkWriter> _rowgroup_writer;
    const std::function<void()> _rollback_action;
};

class ParquetFileWriterFactory : public FileWriterFactory {
public:
    ParquetFileWriterFactory(std::shared_ptr<FileSystem> fs, TCompressionType::type compression_type,
                             std::map<std::string, std::string> options, std::vector<std::string> column_names,
                             std::vector<std::unique_ptr<ColumnEvaluator>>&& column_evaluators,
                             std::optional<std::vector<formats::FileColumnId>> field_ids, PriorityThreadPool* executors,
                             RuntimeState* runtime_state);

    Status init() override;

    StatusOr<WriterAndStream> create(const std::string& path) const override;

private:
    std::shared_ptr<FileSystem> _fs;
    TCompressionType::type _compression_type = TCompressionType::UNKNOWN_COMPRESSION;
    std::optional<std::vector<formats::FileColumnId>> _field_ids;
    std::map<std::string, std::string> _options;
    std::shared_ptr<ParquetWriterOptions> _parsed_options;

    std::vector<std::string> _column_names;
    std::vector<std::unique_ptr<ColumnEvaluator>> _column_evaluators;
    PriorityThreadPool* _executors = nullptr;
    RuntimeState* _runtime_state = nullptr;
};

} // namespace starrocks::formats
