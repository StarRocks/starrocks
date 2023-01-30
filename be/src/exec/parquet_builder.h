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

#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>

#include <cstdint>
#include <map>
#include <string>

#include "common/status.h"
#include "exec/file_builder.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/parquet_types.h"

namespace starrocks {

class ExprContext;
class FileWriter;

struct ParquetBuilderOptions {
    TCompressionType::type compression_type{TCompressionType::SNAPPY};
    bool use_dict{true};
    int64_t row_group_max_size{128 * 1024 * 1024};
};

class ParquetOutputStream : public arrow::io::OutputStream {
public:
    ParquetOutputStream(std::unique_ptr<WritableFile> _writable_file);
    ~ParquetOutputStream() override;
    arrow::Status Write(const void* data, int64_t nbytes) override;
    arrow::Status Write(const std::shared_ptr<arrow::Buffer>& data) override;
    arrow::Result<int64_t> Tell() const override;
    arrow::Status Close() override;

    bool closed() const override { return _is_closed; }

private:
    std::unique_ptr<WritableFile> _writable_file;
    bool _is_closed = false;
};

class ParquetBuildHelper {
public:
    static void build_file_data_type(parquet::Type::type& parquet_data_type, const LogicalType& column_data_type);

    static void build_parquet_repetition_type(parquet::Repetition::type& parquet_repetition_type,
                                              const bool is_nullable);

    static void build_compression_type(parquet::WriterProperties::Builder& builder,
                                       const TCompressionType::type& compression_type);
};

class ParquetBuilder : public FileBuilder {
public:
    ParquetBuilder(std::unique_ptr<WritableFile> writable_file, const std::vector<ExprContext*>& output_expr_ctxs,
                   const ParquetBuilderOptions& options, const std::vector<std::string>& file_column_names);

    ~ParquetBuilder() override = default;

    Status add_chunk(Chunk* chunk) override;

    std::size_t file_size() override;

    Status finish() override;

private:
    Status _init(const ParquetBuilderOptions& options, const std::vector<std::string>& file_column_names);
    void _init_properties(const ParquetBuilderOptions& options);
    Status _init_schema(const std::vector<std::string>& file_column_names);
    void _generate_rg_writer();
    void _flush_row_group();
    size_t _get_rg_written_bytes();
    void _check_size();

    std::unique_ptr<WritableFile> _writable_file;
    std::shared_ptr<ParquetOutputStream> _output_stream;
    const std::vector<ExprContext*>& _output_expr_ctxs;
    std::shared_ptr<::parquet::schema::GroupNode> _schema;
    std::shared_ptr<::parquet::WriterProperties> _properties;
    std::unique_ptr<::parquet::ParquetFileWriter> _file_writer;
    ::parquet::RowGroupWriter* _rg_writer = nullptr;
    std::vector<int64_t> _buffered_values_estimate;
    const int64_t _row_group_max_size;
    bool _closed = false;
    int64_t _total_row_group_writen_bytes{0};
};

} // namespace starrocks
