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

#include "exec/pipeline/sort/local_partition_hash_topn_source.h"

namespace starrocks::pipeline {

Status LocalPartitionHashTopnSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(SourceOperator::prepare(state));
    _ctx->observable().attach_source_observer(state, observer());
    return Status::OK();
}

bool LocalPartitionHashTopnSourceOperator::has_output() const {
    return _ctx->has_output();
}

bool LocalPartitionHashTopnSourceOperator::is_finished() const {
    return _ctx->is_finished();
}

StatusOr<ChunkPtr> LocalPartitionHashTopnSourceOperator::pull_chunk(RuntimeState* state) {
    auto notify = _ctx->observable().defer_notify_sink();
    return _ctx->pull_one_chunk();
}

OperatorPtr LocalPartitionHashTopnSourceOperatorFactory::create(int32_t degree_of_parallelism,
                                                                int32_t driver_sequence) {
    return std::make_shared<LocalPartitionHashTopnSourceOperator>(this, id(), plan_node_id(), driver_sequence,
                                                                  _ctx_factory->create(driver_sequence));
}

} // namespace starrocks::pipeline
