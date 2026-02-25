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
    RETURN_IF_ERROR(_exchanger->init_metrics(_unique_metrics.get(), _driver_sequence == 0));
    _exchanger->open_sink_operator();
    _exchanger->observable().attach_sink_observer(state, observer());
    return Status::OK();
}

bool MultiCastLocalExchangeSinkOperator::need_input() const {
    return _exchanger->can_push_chunk();
}

Status MultiCastLocalExchangeSinkOperator::set_finishing(RuntimeState* state) {
    auto notify = _exchanger->observable().defer_notify_source();
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
    auto notify = _exchanger->observable().defer_notify_source();
    return _exchanger->push_chunk(chunk, _driver_sequence);
}

Status MultiCastLocalExchangeSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(_exchanger->prepare(state));
    return Status::OK();
}
void MultiCastLocalExchangeSinkOperatorFactory::close(RuntimeState* state) {
    _exchanger->close(state);
    OperatorFactory::close(state);
}
} // namespace starrocks::pipeline