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

#include <utility>

#include "common/logging.h"
#include "connector_primitive/connector_sink.h"
#include "connector_primitive/sink_memory_manager.h"
#include "exec/pipeline/fragment_context.h"
#include "exec_primitive/pipeline/operator_factory.h"
#include "formats/io/async_flush_stream_poller.h"
#include "fs/fs.h"

namespace starrocks::pipeline {

class ConnectorSinkOperator final : public Operator {
public:
    ConnectorSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                          std::unique_ptr<connector::ConnectorSink> connector_sink,
                          std::shared_ptr<connector::SinkMemoryManager> sink_mem_mgr, FragmentContext* fragment_context,
                          std::atomic<int32_t>& num_sinkers);

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
    std::unique_ptr<connector::ConnectorSink> _connector_sink;
    std::unique_ptr<formats::AsyncFlushStreamPoller> _io_poller;
    std::shared_ptr<connector::SinkMemoryManager> _sink_mem_mgr;

    bool _no_more_input = false;
    bool _is_cancelled = false;
    FragmentContext* _fragment_context;
    std::atomic<int32_t>& _num_sinkers;
};

class ConnectorSinkOperatorFactory final : public OperatorFactory {
public:
    ConnectorSinkOperatorFactory(int32_t id, std::unique_ptr<connector::ConnectorSinkProvider> data_sink_provider,
                                 FragmentContext* fragment_context);

    ~ConnectorSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    void _increment_num_sinkers_no_barrier() { _num_sinkers.fetch_add(1, std::memory_order_relaxed); }

    std::unique_ptr<connector::ConnectorSinkProvider> _data_sink_provider;
    std::shared_ptr<connector::SinkMemoryManager> _sink_mem_mgr;
    FragmentContext* _fragment_context;
    std::atomic<int32_t> _num_sinkers = 0;
};

} // namespace starrocks::pipeline
