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

#include "connector_sink_operator.h"

#include <tuple>
#include <utility>

#include "connector/async_flush_stream_poller.h"
#include "formats/utils.h"
#include "glog/logging.h"

namespace starrocks::pipeline {

ConnectorSinkOperator::ConnectorSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                             int32_t driver_sequence,
                                             std::unique_ptr<connector::ConnectorChunkSink> connector_chunk_sink,
                                             std::unique_ptr<connector::AsyncFlushStreamPoller> io_poller,
                                             std::shared_ptr<connector::SinkMemoryManager> sink_mem_mgr,
                                             connector::SinkOperatorMemoryManager* op_mem_mgr,
                                             FragmentContext* fragment_context)
        : Operator(factory, id, "connector_sink", plan_node_id, false, driver_sequence),
          _connector_chunk_sink(std::move(connector_chunk_sink)),
          _io_poller(std::move(io_poller)),
          _sink_mem_mgr(std::move(sink_mem_mgr)),
          _op_mem_mgr(op_mem_mgr),
          _fragment_context(fragment_context) {}

Status ConnectorSinkOperator::prepare(RuntimeState* state) {
#ifndef BE_TEST
    RETURN_IF_ERROR(Operator::prepare(state));
#endif
    RETURN_IF_ERROR(_connector_chunk_sink->init());
    return Status::OK();
}

void ConnectorSinkOperator::close(RuntimeState* state) {
    if (_is_cancelled) {
        _connector_chunk_sink->rollback();
    }
#ifndef BE_TEST
    Operator::close(state);
#endif
}

bool ConnectorSinkOperator::need_input() const {
    if (_no_more_input) {
        return false;
    }

    auto [status, _] = _io_poller->poll();
    if (!status.ok()) {
        LOG(WARNING) << "cancel fragment: " << status;
        _fragment_context->cancel(status);
    }

    return _sink_mem_mgr->can_accept_more_input(_op_mem_mgr);
}

bool ConnectorSinkOperator::is_finished() const {
    if (!_no_more_input) {
        return false;
    }

    auto [status, finished] = _io_poller->poll();
    if (!status.ok()) {
        LOG(WARNING) << "cancel fragment: " << status;
        _fragment_context->cancel(status);
    }

    return finished;
}

Status ConnectorSinkOperator::set_finishing(RuntimeState* state) {
    _no_more_input = true;
    RETURN_IF_ERROR(_connector_chunk_sink->finish());
    return Status::OK();
}

bool ConnectorSinkOperator::pending_finish() const {
    return !is_finished();
}

Status ConnectorSinkOperator::set_cancelled(RuntimeState* state) {
    _is_cancelled = true;
    return Status::OK();
}

StatusOr<ChunkPtr> ConnectorSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::NotSupported("ConnectorSinkOperator::pull_chunk");
}

Status ConnectorSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    RETURN_IF_ERROR(_connector_chunk_sink->add(chunk.get()));
    return Status::OK();
}

ConnectorSinkOperatorFactory::ConnectorSinkOperatorFactory(
        int32_t id, std::unique_ptr<connector::ConnectorChunkSinkProvider> data_sink_provider,
        std::shared_ptr<connector::ConnectorChunkSinkContext> sink_context, FragmentContext* fragment_context)
        : OperatorFactory(id, "connector_sink", Operator::s_pseudo_plan_node_id_for_final_sink),
          _data_sink_provider(std::move(data_sink_provider)),
          _sink_context(std::move(sink_context)),
          _fragment_context(fragment_context) {
    MemTracker* query_pool_tracker = GlobalEnv::GetInstance()->query_pool_mem_tracker();
    MemTracker* query_tracker = _fragment_context->runtime_state()->query_mem_tracker_ptr().get();
    _sink_mem_mgr = std::make_shared<connector::SinkMemoryManager>(query_pool_tracker, query_tracker);
}

OperatorPtr ConnectorSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto chunk_sink = _data_sink_provider->create_chunk_sink(_sink_context, driver_sequence).value();
    auto io_poller = std::make_unique<connector::AsyncFlushStreamPoller>();
    chunk_sink->set_io_poller(io_poller.get());
    auto op_mem_mgr = _sink_mem_mgr->create_child_manager();
    chunk_sink->set_operator_mem_mgr(op_mem_mgr);
    return std::make_shared<ConnectorSinkOperator>(this, _id, Operator::s_pseudo_plan_node_id_for_final_sink,
                                                   driver_sequence, std::move(chunk_sink), std::move(io_poller),
                                                   _sink_mem_mgr, op_mem_mgr, _fragment_context);
}

} // namespace starrocks::pipeline
