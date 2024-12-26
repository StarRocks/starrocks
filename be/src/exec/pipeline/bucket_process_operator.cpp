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

#include "exec/pipeline/bucket_process_operator.h"

#include <utility>

#include "exec/pipeline/aggregate/spillable_aggregate_blocking_sink_operator.h"
#include "exec/pipeline/aggregate/spillable_aggregate_distinct_blocking_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/spill_process_channel.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {

Status BucketProcessContext::reset_operator_state(RuntimeState* state) {
    RETURN_IF_ERROR(source->reset_state(state, {}));
    RETURN_IF_ERROR(sink->reset_state(state, {}));
    return Status::OK();
}

Status BucketProcessContext::finish_current_sink(RuntimeState* state) {
    RETURN_IF_ERROR(this->sink->set_finishing(state));
    this->current_bucket_sink_finished = true;
    this->sink_complete_version++;
    return Status::OK();
}

Status BucketProcessSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_ctx->sink->prepare(state));
    _ctx->sink->set_runtime_filter_probe_sequence(_runtime_filter_probe_sequence);
    return Status::OK();
}

void BucketProcessSinkOperator::close(RuntimeState* state) {
    _ctx->sink->close(state);
}

bool BucketProcessSinkOperator::need_input() const {
    if (_ctx->current_bucket_sink_finished) {
        return false;
    }
    return _ctx->sink->need_input();
}

bool BucketProcessSinkOperator::is_finished() const {
    return _ctx->finished || (_ctx->all_input_finishing && _ctx->sink->is_finished());
}

Status BucketProcessSinkOperator::set_finishing(RuntimeState* state) {
    ONCE_DETECT(_set_finishing_once);
    auto defer = DeferOp([&]() {
        if (_ctx->spill_channel != nullptr) {
            _ctx->spill_channel->set_finishing();
        }
    });
    _ctx->all_input_finishing = true;
    DCHECK(_ctx->reset_version <= _ctx->sink_complete_version);
    // acquire finish token and never release
    bool token = _ctx->token;
    if (!token && _ctx->token.compare_exchange_strong(token, true)) {
        // In this condition, if reset_version == ctx->sink_version. indicates that the
        // Possibility 1: The BucketSourceOperator got the token first and executed it.
        //
        // Possibility 2: BucketSink did not receive the EOS chunk, possibly short-circuited.
        //
        // At this point we need to re-execute set_finishing on the sub operator to ensure that is_finished() returns true.
        if (_ctx->reset_version == _ctx->sink_complete_version) {
            RETURN_IF_ERROR(_ctx->finish_current_sink(state));
        }
        _ctx->current_bucket_sink_finished = true;
    }
    return Status::OK();
}

Status BucketProcessSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    auto info = chunk->owner_info();
    if (!chunk->is_empty()) {
        RETURN_IF_ERROR(_ctx->sink->push_chunk(state, chunk));
    }
    // short-circuit case. such as group by limit
    if (_ctx->sink->is_finished()) {
        _ctx->all_input_finishing = true;
        return Status::OK();
    }
    if (info.is_last_chunk()) {
        RETURN_IF_ERROR(_ctx->finish_current_sink(state));
    }
    return Status::OK();
}

Status BucketProcessSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_ctx->source->prepare(state));
    _ctx->source->set_runtime_filter_probe_sequence(_runtime_filter_probe_sequence);
    return Status::OK();
}
// case 1: has_output() is true then call pull_chunk to pull chunk
// case 2: has_output() is false (empty bucket) then to reset state
bool BucketProcessSourceOperator::has_output() const {
    return _ctx->current_bucket_sink_finished && (_ctx->source->has_output() || _ctx->source->is_finished());
}
// condition 1 : all input should be finished
// condition 2 : current bucket source finished (There will be no additional output on the source side.)
bool BucketProcessSourceOperator::is_finished() const {
    return _ctx->finished || (_ctx->all_input_finishing && _ctx->source->is_finished());
}
Status BucketProcessSourceOperator::set_finished(RuntimeState* state) {
    _ctx->finished = true;
    RETURN_IF_ERROR(_ctx->source->set_finished(state));
    return Status::OK();
}
void BucketProcessSourceOperator::close(RuntimeState* state) {
    _ctx->source->close(state);
}

StatusOr<ChunkPtr> BucketProcessSourceOperator::pull_chunk(RuntimeState* state) {
    // BucketProcessSink::set_finishing execution timing is uncertain
    ChunkPtr chunk;
    if (_ctx->source->has_output()) {
        ASSIGN_OR_RETURN(chunk, _ctx->source->pull_chunk(state));
    }
    if (!_ctx->all_input_finishing && _ctx->source->is_finished()) {
        bool token = _ctx->token;
        if (!token && _ctx->token.compare_exchange_strong(token, true)) {
            RETURN_IF_ERROR(_ctx->reset_operator_state(state));
            _ctx->reset_version++;
            if (_ctx->all_input_finishing) {
                // BucketSink::set_finishing is called but we have called reset_state().
                // call sub operator set_finishing to make sure the final state sub_sink_operator->is_finished() is true
                RETURN_IF_ERROR(_ctx->finish_current_sink(state));
                DCHECK_EQ(_ctx->sink_complete_version, _ctx->reset_version + 1);
            } else {
                _ctx->current_bucket_sink_finished = false;
            }
            _ctx->token = false;
        }
    }

    return chunk;
}

// TODO: put the spill channel in operator.
SpillProcessChannelPtr get_spill_channel(const OperatorPtr& op) {
    if (auto raw = dynamic_cast<SpillableAggregateBlockingSinkOperator*>(op.get()); raw != nullptr) {
        return raw->spill_channel();
    } else if (auto raw = dynamic_cast<SpillableAggregateDistinctBlockingSinkOperator*>(op.get()); raw != nullptr) {
        return raw->spill_channel();
    }
    return nullptr;
}

BucketProcessSinkOperatorFactory::BucketProcessSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                                                   BucketProcessContextFactoryPtr context_factory,
                                                                   OperatorFactoryPtr factory)
        : OperatorFactory(id, "bucket_process_sink_factory", plan_node_id),
          _factory(std::move(factory)),
          _ctx_factory(std::move(context_factory)) {}

OperatorPtr BucketProcessSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto ctx = _ctx_factory->get_or_create(driver_sequence);
    ctx->sink = _factory->create(degree_of_parallelism, driver_sequence);
    auto spill_channel = get_spill_channel(ctx->sink);
    if (spill_channel != nullptr) {
        spill_channel->set_reuseable(true);
    }
    ctx->spill_channel = std::move(spill_channel);
    auto bucket_source_operator =
            std::make_shared<BucketProcessSinkOperator>(this, _id, _plan_node_id, driver_sequence, ctx);
    return bucket_source_operator;
}

Status BucketProcessSinkOperatorFactory::prepare(RuntimeState* state) {
    return _factory->prepare(state);
}

void BucketProcessSinkOperatorFactory::close(RuntimeState* state) {
    _factory->close(state);
}

BucketProcessSourceOperatorFactory::BucketProcessSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                                                       BucketProcessContextFactoryPtr context_factory,
                                                                       OperatorFactoryPtr factory)
        : SourceOperatorFactory(id, "bucket_process_factory", plan_node_id),
          _factory(std::move(factory)),
          _ctx_factory(std::move(context_factory)) {}

OperatorPtr BucketProcessSourceOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto ctx = _ctx_factory->get_or_create(driver_sequence);
    ctx->source = _factory->create(degree_of_parallelism, driver_sequence);
    auto bucket_source_operator =
            std::make_shared<BucketProcessSourceOperator>(this, _id, _plan_node_id, driver_sequence, ctx);
    return bucket_source_operator;
}

Status BucketProcessSourceOperatorFactory::prepare(RuntimeState* state) {
    return _factory->prepare(state);
}

void BucketProcessSourceOperatorFactory::close(RuntimeState* state) {
    _factory->close(state);
}

} // namespace starrocks::pipeline