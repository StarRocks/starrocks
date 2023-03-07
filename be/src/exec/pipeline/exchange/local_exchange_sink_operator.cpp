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

#include "exec/pipeline/exchange/local_exchange_sink_operator.h"

#include "column/chunk.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

/// LocalExchangeSinkOperator.
Status LocalExchangeSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _exchanger->incr_sinker();
    _unique_metrics->add_info_string("ShuffleNum", std::to_string(_exchanger->source_dop()));
    _peak_memory_usage_counter = _unique_metrics->AddHighWaterMarkCounter(
            "LocalExchangePeakMemoryUsage", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES));
    return Status::OK();
}

bool LocalExchangeSinkOperator::need_input() const {
    return !_is_finished && _exchanger->need_input();
}

StatusOr<ChunkPtr> LocalExchangeSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't call pull_chunk from local exchange sink.");
}

Status LocalExchangeSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    _exchanger->finish(state);
    return Status::OK();
}

Status LocalExchangeSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    auto res = _exchanger->accept(chunk, _driver_sequence);
    _peak_memory_usage_counter->set(_exchanger->get_memory_usage());
    return res;
}

/// LocalExchangeSinkOperatorFactory.
Status LocalExchangeSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(_exchanger->prepare(state));
    return Status::OK();
}
void LocalExchangeSinkOperatorFactory::close(RuntimeState* state) {
    _exchanger->close(state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
