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
#include <exec/pipeline/scan/connector_scan_operator.h>

#include "connector_chunk_sink.h"
#include "common/logging.h"
#include "exec/parquet_writer.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"
#include "fs/fs.h"

namespace starrocks::pipeline {

class ConnectorSinkOperator final : public Operator {
public:
    ConnectorSinkOperator(OperatorFactory* factory, const int32_t id, const int32_t plan_node_id,
                          const int32_t driver_sequence, std::unique_ptr<ConnectorChunkSink> connector_chunk_sink, FragmentContext* fragment_context)
            : Operator(factory, id, "connector_sink_operator", plan_node_id, false, driver_sequence),
              _connector_chunk_sink(std::move(connector_chunk_sink)), _fragment_context(fragment_context) {}

    ~ConnectorSinkOperator() override = default;

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
    Status enqueue_futures(ConnectorChunkSink::Futures future);

    std::unique_ptr<ConnectorChunkSink> _connector_chunk_sink;

    mutable std::queue<std::future<Status>> _add_chunk_future_queue;
    mutable std::queue<std::future<FileWriter::CommitResult>> _commit_file_future_queue;
    mutable std::queue<std::function<void()>> _rollback_actions;

    bool _no_more_input = false;
    bool _is_cancelled = false;
    FragmentContext* _fragment_context;
};

class ConnectorSinkOperatorFactory final : public OperatorFactory {
public:
    ConnectorSinkOperatorFactory(const int32_t id, const string& path, const string& file_format,
                                          const TCompressionType::type& compression_type,
                                          const std::vector<ExprContext*>& output_exprs,
                                          const std::vector<ExprContext*>& partition_exprs,
                                          const std::vector<std::string>& column_names,
                                          const std::vector<std::string>& partition_column_names,
                                          bool write_single_file, const TCloudConfiguration& cloud_conf,
                                          FragmentContext* fragment_ctx);

    ~ConnectorSinkOperatorFactory() override = default;

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
