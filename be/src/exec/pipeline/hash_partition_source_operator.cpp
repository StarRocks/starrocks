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

#include "hash_partition_source_operator.h"

namespace starrocks::pipeline {

bool HashPartitionSourceOperator::has_output() const {
    return _hash_partition_ctx->has_output();
}

bool HashPartitionSourceOperator::is_finished() const {
    return _hash_partition_ctx->is_finished();
}

StatusOr<ChunkPtr> HashPartitionSourceOperator::pull_chunk(RuntimeState* state) {
    return _hash_partition_ctx->pull_one_chunk(state);
}

HashPartitionSourceOperatorFactory::HashPartitionSourceOperatorFactory(
        int32_t id, int32_t plan_node_id, HashPartitionContextFactoryPtr hash_partition_ctx_factory)
        : SourceOperatorFactory(id, "hash_partition_source", plan_node_id),
          _hash_partition_ctx_factory(std::move(hash_partition_ctx_factory)) {}

OperatorPtr HashPartitionSourceOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<HashPartitionSourceOperator>(this, _id, _plan_node_id, driver_sequence,
                                                         _hash_partition_ctx_factory->create(driver_sequence));
}
} // namespace starrocks::pipeline
