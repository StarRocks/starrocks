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

#include "exec/pipeline/group_execution/group_operator.h"

#include "exec/pipeline/group_execution/execution_group.h"
#include "exec/pipeline/operator.h"
#include "gutil/casts.h"

namespace starrocks::pipeline {
Status GroupedExecutionSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _exchanger->incr_sinker();
    _unique_metrics->add_info_string("ShuffleNum", std::to_string(_exchanger->source_dop()));
    _peak_memory_usage_counter = _unique_metrics->AddHighWaterMarkCounter(
            "GroupLocalExchangePeakMemoryUsage", TUnit::BYTES,
            RuntimeProfile::Counter::create_strategy(TUnit::BYTES, TCounterMergeType::SKIP_FIRST_MERGE));
    _exchanger->attach_sink_observer(state, this->observer());
    return Status::OK();
}

bool GroupedExecutionSinkOperator::need_input() const {
    return !_is_finished && _exchanger->need_input();
}

void GroupedExecutionSinkOperator::close(RuntimeState* state) {
    Operator::close(state);
}

Status GroupedExecutionSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    _exchanger->finish(state);
    down_cast<GroupedExecutionSinkFactory*>(_factory)->submit();
    return Status::OK();
}

Status GroupedExecutionSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    auto res = _exchanger->accept(chunk, _driver_sequence);
    _peak_memory_usage_counter->set(_exchanger->get_memory_usage());
    return Status::OK();
}

Status GroupedExecutionSinkFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(_exchanger->prepare(state));
    return Status::OK();
}

OperatorPtr GroupedExecutionSinkFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<GroupedExecutionSinkOperator>(this, _id, _plan_node_id, driver_sequence, _exchanger);
}

void GroupedExecutionSinkFactory::close(RuntimeState* state) {
    _exchanger->close(state);
    OperatorFactory::close(state);
}

void GroupedExecutionSinkFactory::submit() {
    _exec_group->submit_next_driver();
}

} // namespace starrocks::pipeline