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

#include "exec/pipeline/fetch_sink_operator.h"

#include "agent/master_info.h"
#include "common/status.h"
#include "exec/pipeline/fetch_processor.h"

namespace starrocks::pipeline {

FetchSinkOperator::FetchSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                     int32_t driver_sequence, std::shared_ptr<FetchProcessor> processor)
        : Operator(factory, id, "fetch_sink", plan_node_id, false, driver_sequence), _processor(std::move(processor)) {}

Status FetchSinkOperator::prepare(RuntimeState* state) {
    DLOG(INFO) << "[GLM] FetchSinkOperator::prepare, processor: " << (void*)_processor.get() << ", " << (void*)this;
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_processor->prepare(state, _unique_metrics.get()));
    return Status::OK();
}

void FetchSinkOperator::close(RuntimeState* state) {
    Operator::close(state);
    DLOG(INFO) << "[GLM] FetchSinkOperator::close, processor: " << (void*)_processor.get() << ", " << (void*)this;
}

bool FetchSinkOperator::need_input() const {
    return _processor->need_input();
}

bool FetchSinkOperator::is_finished() const {
    return _processor->is_sink_complete();
}

Status FetchSinkOperator::set_finishing(RuntimeState* state) {
    DLOG(INFO) << "[GLM] FetchSinkOperator::set_finishing, processor: " << (void*)_processor.get() << ", " << (void*)this;
    return _processor->set_sink_finishing(state);
}

bool FetchSinkOperator::pending_finish() const {
    return !is_finished();
}

Status FetchSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return _processor->push_chunk(state, chunk);
}

OperatorPtr FetchSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto processor = _processor_factory->get_or_create(driver_sequence);
    return std::make_shared<FetchSinkOperator>(this, _id, _plan_node_id, driver_sequence, std::move(processor));
}

} // namespace starrocks::pipeline