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

#include <gen_cpp/DataSinks_types.h>
#include <parquet/arrow/writer.h>

#include <utility>

#include "common/logging.h"
#include "exec/parquet_writer.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"
#include "fs/fs.h"

namespace starrocks::pipeline {

StatusOr<std::string> column_to_string(const TypeDescriptor& type_desc, const ColumnPtr& column);

class TableFunctionTableSinkOperator final : public Operator {
public:
    TableFunctionTableSinkOperator(OperatorFactory* factory, const int32_t id, const int32_t plan_node_id,
                                   const int32_t driver_sequence, const string& path, const string& file_format,
                                   const TCompressionType::type& compression_type,
                                   const std::vector<ExprContext*>& output_exprs,
                                   const std::vector<ExprContext*>& partition_exprs,
                                   const std::vector<std::string>& partition_column_names, bool write_single_file,
                                   const TCloudConfiguration& cloud_conf, FragmentContext* fragment_ctx,
                                   std::shared_ptr<::parquet::schema::GroupNode> parquet_file_schema)
            : Operator(factory, id, "table_function_table_sink", plan_node_id, false, driver_sequence),
              _path(path),
              _file_format(file_format),
              _compression_type(compression_type),
              _output_exprs(output_exprs),
              _partition_exprs(partition_exprs),
              _partition_column_names(partition_column_names),
              _write_single_file(write_single_file),
              _cloud_conf(cloud_conf),
              _fragment_ctx(fragment_ctx),
              _parquet_file_schema(std::move(parquet_file_schema)) {}

    ~TableFunctionTableSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override;

    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;

    bool pending_finish() const override;

    Status set_cancelled(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    TableInfo _make_table_info(const string& partition_location) const;

    static void add_commit_info(starrocks::parquet::AsyncFileWriter* writer, RuntimeState* state) {
        if (writer->metadata()) {
            state->update_num_rows_load_sink(writer->metadata()->num_rows());
        }
    }

private:
    const std::string _path;
    const std::string _file_format;
    const TCompressionType::type _compression_type;
    const std::vector<ExprContext*> _output_exprs;
    const std::vector<ExprContext*> _partition_exprs;
    const std::vector<std::string> _partition_column_names;
    const bool _write_single_file;
    const TCloudConfiguration _cloud_conf;
    mutable FragmentContext* _fragment_ctx;

    const std::shared_ptr<::parquet::schema::GroupNode> _parquet_file_schema;
    std::unordered_map<std::string, std::unique_ptr<starrocks::RollingAsyncParquetWriter>> _partition_writers;
    std::atomic<bool> _is_finished = false;

    std::unique_ptr<FileWriter> _file_writer;
    mutable std::queue<std::future<Status>> _blocking_futures;
};

class TableFunctionTableSinkOperatorFactory final : public OperatorFactory {
public:
    TableFunctionTableSinkOperatorFactory(const int32_t id, const string& path, const string& file_format,
                                          const TCompressionType::type& compression_type,
                                          const std::vector<ExprContext*>& output_exprs,
                                          const std::vector<ExprContext*>& partition_exprs,
                                          const std::vector<std::string>& column_names,
                                          const std::vector<std::string>& partition_column_names,
                                          bool write_single_file, const TCloudConfiguration& cloud_conf,
                                          FragmentContext* fragment_ctx);

    ~TableFunctionTableSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    const std::string _path;
    const std::string _file_format;
    const TCompressionType::type _compression_type;
    const std::vector<ExprContext*> _output_exprs;
    const std::vector<ExprContext*> _partition_exprs;
    const std::vector<std::string> _column_names;
    const std::vector<std::string> _partition_column_names;
    const bool _write_single_file;
    const TCloudConfiguration _cloud_conf;
    FragmentContext* _fragment_ctx;

    std::shared_ptr<::parquet::schema::GroupNode> _parquet_file_schema;
};

} // namespace starrocks::pipeline
