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

#include "hash_partition_sink_operator.h"

namespace starrocks::pipeline {

Status HashPartitionSinkOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);
    return _hash_partition_ctx->prepare(state);
}

StatusOr<ChunkPtr> HashPartitionSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't call pull_chunk from hash partition sink operator.");
}

Status HashPartitionSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return _hash_partition_ctx->push_one_chunk_to_partitioner(state, chunk);
}

Status HashPartitionSinkOperator::set_finishing(RuntimeState* state) {
    _hash_partition_ctx->sink_complete();
    _is_finished = true;
    return Status::OK();
}

HashPartitionSinkOperatorFactory::HashPartitionSinkOperatorFactory(
        int32_t id, int32_t plan_node_id, HashPartitionContextFactoryPtr hash_partition_ctx_factory)
        : OperatorFactory(id, "hash_partition_sink", plan_node_id),
          _hash_partition_ctx_factory(std::move(hash_partition_ctx_factory)) {}

OperatorPtr HashPartitionSinkOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<HashPartitionSinkOperator>(this, _id, _plan_node_id, driver_sequence,
                                                       _hash_partition_ctx_factory->create(driver_sequence));
}
} // namespace starrocks::pipeline
