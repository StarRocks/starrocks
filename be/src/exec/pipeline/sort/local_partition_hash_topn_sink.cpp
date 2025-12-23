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

#include "exec/pipeline/sort/local_partition_hash_topn_sink.h"

namespace starrocks::pipeline {

Status LocalPartitionHashTopnSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _ctx->observable().attach_sink_observer(state, observer());
    return _ctx->prepare(state, _unique_metrics.get());
}

StatusOr<ChunkPtr> LocalPartitionHashTopnSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("LocalPartitionHashTopnSinkOperator does not support pull_chunk");
}

Status LocalPartitionHashTopnSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    auto notify = _ctx->observable().defer_notify_source();
    return _ctx->push_one_chunk(state, chunk);
}

Status LocalPartitionHashTopnSinkOperator::set_finishing(RuntimeState* state) {
    auto notify = _ctx->observable().defer_notify_source();
    ONCE_DETECT(_set_finishing_once);
    DeferOp defer([&]() {
        _ctx->sink_complete();
        _unique_metrics->add_info_string("IsPassThrough", "No");
        auto* partition_num_counter = ADD_COUNTER(_unique_metrics, "PartitionNum", TUnit::UNIT);
        COUNTER_SET(partition_num_counter, static_cast<int64_t>(_ctx->num_partitions()));
        _is_finished = true;
    });
    if (state->is_cancelled()) {
        return Status::OK();
    }
    return _ctx->finalize(state);
}

OperatorPtr LocalPartitionHashTopnSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<LocalPartitionHashTopnSinkOperator>(this, _id, _plan_node_id, driver_sequence,
                                                                _ctx_factory->create(driver_sequence));
}

Status LocalPartitionHashTopnSinkOperatorFactory::prepare(RuntimeState* state) {
    return _ctx_factory->prepare(state);
}

} // namespace starrocks::pipeline
