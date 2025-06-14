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

#include "exec/pipeline/exchange/multi_cast_local_exchange_source_operator.h"

namespace starrocks::pipeline {

Status MultiCastLocalExchangeSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    RETURN_IF_ERROR(_exchanger->init_metrics(_unique_metrics.get(), false));
    _exchanger->open_source_operator(_mcast_consumer_index);
    _exchanger->observable().attach_source_observer(state, observer());
    return Status::OK();
}

Status MultiCastLocalExchangeSourceOperator::set_finishing(RuntimeState* state) {
    auto notify = _exchanger->observable().defer_notify_sink();
    if (!_is_finished) {
        _is_finished = true;
        _exchanger->close_source_operator(_mcast_consumer_index);
    }
    return Status::OK();
}

StatusOr<ChunkPtr> MultiCastLocalExchangeSourceOperator::pull_chunk(RuntimeState* state) {
    auto notify = _exchanger->observable().defer_notify_sink();
    auto ret = _exchanger->pull_chunk(state, _mcast_consumer_index);
    if (ret.status().is_end_of_file()) {
        (void)set_finishing(state);
    }
    return ret;
}

bool MultiCastLocalExchangeSourceOperator::has_output() const {
    return _exchanger->can_pull_chunk(_mcast_consumer_index);
}

} // namespace starrocks::pipeline