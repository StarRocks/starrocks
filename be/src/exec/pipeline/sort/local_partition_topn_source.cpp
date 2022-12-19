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

#include "exec/pipeline/sort/local_partition_topn_source.h"

#include <utility>

namespace starrocks::pipeline {

LocalPartitionTopnSourceOperator::LocalPartitionTopnSourceOperator(OperatorFactory* factory, int32_t id,
                                                                   int32_t plan_node_id, int32_t driver_sequence,
                                                                   LocalPartitionTopnContext* partition_topn_ctx)
        : SourceOperator(factory, id, "local_partition_topn_source", plan_node_id, driver_sequence),
          _partition_topn_ctx(partition_topn_ctx) {}

bool LocalPartitionTopnSourceOperator::has_output() const {
    return _partition_topn_ctx->has_output();
}

bool LocalPartitionTopnSourceOperator::is_finished() const {
    return _partition_topn_ctx->is_finished();
}

StatusOr<ChunkPtr> LocalPartitionTopnSourceOperator::pull_chunk(RuntimeState* state) {
    return _partition_topn_ctx->pull_one_chunk();
}

LocalPartitionTopnSourceOperatorFactory::LocalPartitionTopnSourceOperatorFactory(
        int32_t id, int32_t plan_node_id, LocalPartitionTopnContextFactoryPtr partition_topn_ctx_factory)
        : SourceOperatorFactory(id, "local_partition_topn_source", plan_node_id),
          _partition_topn_ctx_factory(std::move(partition_topn_ctx_factory)) {}
OperatorPtr LocalPartitionTopnSourceOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    return std::make_shared<LocalPartitionTopnSourceOperator>(this, _id, _plan_node_id, driver_sequence,
                                                              _partition_topn_ctx_factory->create(driver_sequence));
}
} // namespace starrocks::pipeline
