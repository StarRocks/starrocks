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

#include "exec/pipeline/fetch_source_operator.h"

#include "exec/pipeline/fetch_processor.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

FetchSourceOperator::FetchSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                         int32_t driver_sequence, std::shared_ptr<FetchProcessor> processor)
        : SourceOperator(factory, id, "fetch_source", plan_node_id, false, driver_sequence),
          _processor(std::move(processor)) {}

Status FetchSourceOperator::prepare(RuntimeState* state) {
    VLOG_ROW << "[GLM] FetchSourceOperator::prepare, processor: " << (void*)_processor.get() << ", " << (void*)this;
    RETURN_IF_ERROR(Operator::prepare(state));
    return Status::OK();
}

void FetchSourceOperator::close(RuntimeState* state) {
    Operator::close(state);
    VLOG_ROW << "[GLM] FetchSourceOperator::close, processor: " << (void*)_processor.get() << ", " << (void*)this;
}

bool FetchSourceOperator::has_output() const {
    return _processor->has_output();
}

bool FetchSourceOperator::is_finished() const {
    return _processor->is_finished();
}
bool FetchSourceOperator::pending_finish() const {
    return !is_finished();
}

Status FetchSourceOperator::set_finishing(RuntimeState* state) {
    VLOG_ROW << "[GLM] FetchSourceOperator::set_finishing, processor: " << (void*)_processor.get() << ", "
             << (void*)this;
    return Status::OK();
}

StatusOr<ChunkPtr> FetchSourceOperator::pull_chunk(RuntimeState* state) {
    return _processor->pull_chunk(state);
}

OperatorPtr FetchSourceOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto processor = _processor_factory->get_or_create(driver_sequence);
    return std::make_shared<FetchSourceOperator>(this, _id, _plan_node_id, driver_sequence, std::move(processor));
}

} // namespace starrocks::pipeline