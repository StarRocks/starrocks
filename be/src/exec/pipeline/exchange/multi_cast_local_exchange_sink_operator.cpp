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

#include "exec/pipeline/exchange/multi_cast_local_exchange_sink_operator.h"

namespace starrocks::pipeline {

Status MultiCastLocalExchangeSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    _exchanger->open_sink_operator();
    _peak_memory_usage_counter = _unique_metrics->AddHighWaterMarkCounter(
            "ExchangerPeakMemoryUsage", TUnit::BYTES, RuntimeProfile::Counter::create_strategy(TUnit::BYTES, TCounterMergeType::SKIP_FIRST_MERGE));
    _peak_buffer_row_size_counter = _unique_metrics->AddHighWaterMarkCounter(
            "ExchangerPeakBufferRowSize", TUnit::UNIT, RuntimeProfile::Counter::create_strategy(TUnit::UNIT, TCounterMergeType::SKIP_FIRST_MERGE));
    return Status::OK();
}

bool MultiCastLocalExchangeSinkOperator::need_input() const {
    return _exchanger->can_push_chunk();
}

Status MultiCastLocalExchangeSinkOperator::set_finishing(RuntimeState* state) {
    if (!_is_finished) {
        _is_finished = true;
        _exchanger->close_sink_operator();
    }
    return Status::OK();
}

StatusOr<ChunkPtr> MultiCastLocalExchangeSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Should not pull_chunk in MultiCastLocalExchangeSinkOperator");
}

Status MultiCastLocalExchangeSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return _exchanger->push_chunk(chunk, _driver_sequence, this);
}

void MultiCastLocalExchangeSinkOperator::update_counter(size_t memory_usage, size_t buffer_row_size) {
    _peak_memory_usage_counter->set(memory_usage);
    _peak_buffer_row_size_counter->set(buffer_row_size);
}


}