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

namespace starrocks {
namespace pipeline {

class HiveTableSinkOperator final : public Operator {
public:
    HiveTableSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                          std::string location, std::string file_format, TCompressionType::type compression_codec,
                          const TCloudConfiguration& cloud_conf, FragmentContext* fragment_ctx,
                          const std::shared_ptr<::parquet::schema::GroupNode>& schema,
                          const std::vector<ExprContext*>& output_expr_ctxs,
                          const vector<ExprContext*>& partition_output_expr,
                          const vector<std::string>& partition_column_names,
                          const vector<std::string>& data_column_names, bool is_static_partition_insert)
            : Operator(factory, id, "hive_table_sink", plan_node_id, false, driver_sequence),
              _location(std::move(location)),
              _file_format(std::move(file_format)),
              _compression_codec(std::move(compression_codec)),
              _cloud_conf(cloud_conf),
              _parquet_file_schema(std::move(schema)),
              _output_expr(output_expr_ctxs),
              _partition_expr(partition_output_expr),
              _is_static_partition_insert(is_static_partition_insert),
              _partition_column_names(partition_column_names),
              _data_column_names(data_column_names) {}

    ~HiveTableSinkOperator() override = default;

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

    static void add_hive_commit_info(starrocks::parquet::AsyncFileWriter* writer, RuntimeState* state);

private:
    std::string _get_partition_location(const std::vector<std::string>& values);

    std::string _location;
    std::string _file_format;
    TCompressionType::type _compression_codec;
    TCloudConfiguration _cloud_conf;

    std::shared_ptr<::parquet::schema::GroupNode> _parquet_file_schema;
    std::vector<ExprContext*> _output_expr;
    std::vector<ExprContext*> _partition_expr;
    std::unordered_map<std::string, std::unique_ptr<starrocks::RollingAsyncParquetWriter>> _partition_writers;
    std::atomic<bool> _is_finished = false;
    bool _is_static_partition_insert = false;
    std::vector<std::string> _partition_column_names;
    std::vector<std::string> _data_column_names;
};

class HiveTableSinkOperatorFactory final : public OperatorFactory {
public:
    HiveTableSinkOperatorFactory(int32_t id, FragmentContext* fragment_ctx, const THiveTableSink& t_hive_table_sink,
                                 vector<TExpr> t_output_expr, std::vector<ExprContext*> partition_expr_ctxs);

    ~HiveTableSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<HiveTableSinkOperator>(
                this, _id, _plan_node_id, driver_sequence, _location, _file_format, _compression_codec, _cloud_conf,
                _fragment_ctx, _parquet_file_schema,
                std::vector<ExprContext*>(_output_expr_ctxs.begin(),
                                          _output_expr_ctxs.begin() + _data_column_names.size()),
                _partition_expr_ctxs, _partition_column_names, _data_column_names, is_static_partition_insert);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

private:
    std::vector<TExpr> _t_output_expr;
    std::vector<ExprContext*> _output_expr_ctxs;
    std::vector<ExprContext*> _partition_expr_ctxs;

    std::vector<std::string> _data_column_names;
    std::vector<std::string> _partition_column_names;

    FragmentContext* _fragment_ctx = nullptr;
    std::string _location;
    std::string _file_format;
    TCompressionType::type _compression_codec;
    TCloudConfiguration _cloud_conf;
    std::shared_ptr<::parquet::schema::GroupNode> _parquet_file_schema;
    bool is_static_partition_insert = false;
};

} // namespace pipeline
} // namespace starrocks
