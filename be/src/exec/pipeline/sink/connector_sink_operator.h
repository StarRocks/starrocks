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

#include <exec/pipeline/scan/connector_scan_operator.h>
#include <gen_cpp/DataSinks_types.h>
#include <parquet/arrow/writer.h>

#include <utility>

#include "common/logging.h"
#include "connector_sink/connector_chunk_sink.h"
#include "exec/parquet_writer.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/operator.h"
#include "fs/fs.h"

namespace starrocks::pipeline {

// TODO(letian-jiang): inherit Operator, and deprecate all external table sink operators
class ConnectorSinkOperator final : public Operator {
public:
    ConnectorSinkOperator(OperatorFactory* factory, const int32_t id, const int32_t plan_node_id,
                          const int32_t driver_sequence,
                          std::unique_ptr<connector::ConnectorChunkSink> connector_chunk_sink,
                          FragmentContext* fragment_context)
            : Operator(factory, id, "connector_sink_operator", plan_node_id, false, driver_sequence),
              _connector_chunk_sink(std::move(connector_chunk_sink)),
              _fragment_context(fragment_context) {}

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
    Status _enqueue_futures(connector::ConnectorChunkSink::Futures future);

    std::unique_ptr<connector::ConnectorChunkSink> _connector_chunk_sink;

    mutable std::queue<std::future<Status>> _add_chunk_future_queue;
    mutable std::queue<std::future<formats::FileWriter::CommitResult>> _commit_file_future_queue;
    mutable std::queue<std::function<void()>> _rollback_actions; // TODO: file system

    bool _no_more_input = false;
    bool _is_cancelled = false;
    FragmentContext* _fragment_context;
};

// TODO(letian-jiang): inherit OperatorFactory, and deprecate all external table sink operators
class ConnectorSinkOperatorFactory final : public OperatorFactory {
public:
    ConnectorSinkOperatorFactory(int32_t id, std::unique_ptr<connector::ConnectorChunkSinkProvider> data_sink_provider,
                                 std::shared_ptr<connector::ConnectorChunkSinkContext> context,
                                 FragmentContext* fragment_context);

    ~ConnectorSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    std::unique_ptr<connector::ConnectorChunkSinkProvider> _data_sink_provider;
    std::shared_ptr<connector::ConnectorChunkSinkContext> _context;
    FragmentContext* _fragment_context;
};

} // namespace starrocks::pipeline
