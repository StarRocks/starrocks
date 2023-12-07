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

#pragma once

#include "exec/pipeline/hash_partition_context.h"
#include "exec/pipeline/source_operator.h"
#include "exec/vectorized/partition/chunks_partitioner.h"

namespace starrocks::pipeline {
class HashPartitionSourceOperator final : public SourceOperator {
public:
    HashPartitionSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                HashPartitionContext* hash_partition_ctx)
            : SourceOperator(factory, id, "hash_partition_source", plan_node_id, driver_sequence),
              _hash_partition_ctx(hash_partition_ctx) {}

    ~HashPartitionSourceOperator() override = default;

    bool has_output() const override;

    bool is_finished() const override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    HashPartitionContext* _hash_partition_ctx;
};

class HashPartitionSourceOperatorFactory final : public SourceOperatorFactory {
public:
    HashPartitionSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                       HashPartitionContextFactoryPtr hash_partition_ctx_factory);

    ~HashPartitionSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    HashPartitionContextFactoryPtr _hash_partition_ctx_factory;
};
} // namespace starrocks::pipeline
