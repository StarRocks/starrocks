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

#include "formats/parquet/parquet_file_writer.h"
#include "glog/logging.h"
#include "util/url_coding.h"

namespace starrocks::pipeline {

Status ConnectorSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_connector_chunk_sink->init());
    return Status::OK();
}

void ConnectorSinkOperator::close(RuntimeState* state) {
    if (_is_cancelled) {
        while (!_rollback_actions.empty()) {
            _rollback_actions.front()();
            _rollback_actions.pop();
        }
    }
    Operator::close(state);
}

template <typename R>
bool is_ready(std::future<R> const& f) {
    return f.wait_for(std::chrono::seconds(0)) == std::future_status::ready;
}

bool ConnectorSinkOperator::need_input() const {
    if (_no_more_input) {
        return false;
    }

    while (!_add_chunk_future_queue.empty()) {
        // return if any future is not ready, check in order of FIFO
        if (!is_ready(_add_chunk_future_queue.front())) {
            return false;
        }
        if (auto st = _add_chunk_future_queue.front().get(); !st.ok()) {
            LOG(WARNING) << "cancel fragment: " << st;
            _fragment_context->cancel(st);
        }
        _add_chunk_future_queue.pop();
    }

    return true;
}

bool ConnectorSinkOperator::is_finished() const {
    if (!_no_more_input) {
        return false;
    }

    while (!_add_chunk_future_queue.empty()) {
        // return if any future is not ready, check in order of FIFO
        if (!is_ready(_add_chunk_future_queue.front())) {
            return false;
        }

        if (auto st = _add_chunk_future_queue.front().get(); !st.ok()) {
            LOG(WARNING) << "cancel fragment: " << st;
            _fragment_context->cancel(st);
        }
        _add_chunk_future_queue.pop();
    }

    while (!_commit_file_future_queue.empty()) {
        // return if any future is not ready, check in order of FIFO
        if (!is_ready(_commit_file_future_queue.front())) {
            return false;
        }

        auto result = _commit_file_future_queue.front().get();
        _commit_file_future_queue.pop();

        if (auto st = result.io_status; st.ok()) {
            _connector_chunk_sink->callback_on_success()(result);
        } else {
            LOG(WARNING) << "cancel fragment: " << st;
            _fragment_context->cancel(st);
        }
        _rollback_actions.push(std::move(result.rollback_action));
    }

    DCHECK(_add_chunk_future_queue.empty());
    DCHECK(_commit_file_future_queue.empty());
    return true;
}

Status ConnectorSinkOperator::set_finishing(RuntimeState* state) {
    _no_more_input = true;
    LOG(INFO) << "set finishing";
    auto future = _connector_chunk_sink->finish();
    return _enqueue_futures(std::move(future));
}

bool ConnectorSinkOperator::pending_finish() const {
    return !is_finished();
}

Status ConnectorSinkOperator::set_cancelled(RuntimeState* state) {
    _is_cancelled = true;
    return Status::OK();
}

StatusOr<ChunkPtr> ConnectorSinkOperator::pull_chunk(RuntimeState* state) {
    CHECK(false) << "ConnectorSinkOperator::pull_chunk";
    __builtin_unreachable();
}

Status ConnectorSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    ASSIGN_OR_RETURN(auto future, _connector_chunk_sink->add(chunk));
    return _enqueue_futures(std::move(future));
}

Status ConnectorSinkOperator::_enqueue_futures(connector::ConnectorChunkSink::Futures future) {
    for (auto& f : future.add_chunk_future) {
        _add_chunk_future_queue.push(std::move(f));
    }
    for (auto& f : future.commit_file_future) {
        _commit_file_future_queue.push(std::move(f));
    }
    return Status::OK();
}

ConnectorSinkOperatorFactory::ConnectorSinkOperatorFactory(
        int32_t id, std::unique_ptr<connector::ConnectorChunkSinkProvider> data_sink_provider,
        std::shared_ptr<connector::ConnectorChunkSinkContext> sink_context, FragmentContext* fragment_context)
        : OperatorFactory(id, "connector sink operator", Operator::s_pseudo_plan_node_id_for_final_sink),
          _data_sink_provider(std::move(data_sink_provider)),
          _sink_context(sink_context),
          _fragment_context(fragment_context) {}

OperatorPtr ConnectorSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto chunk_sink = _data_sink_provider->create_chunk_sink(_sink_context, driver_sequence);
    return std::make_shared<ConnectorSinkOperator>(this, _id, Operator::s_pseudo_plan_node_id_for_final_sink,
                                                   driver_sequence, std::move(chunk_sink), _fragment_context);
}

} // namespace starrocks::pipeline
