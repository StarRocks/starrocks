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

#include "exec/partition/chunks_partitioner.h"
#include "exec/pipeline/hash_partition_context.h"
#include "exec/pipeline/operator.h"

/**
 * TODO 
 * 1. disable downgrade
 */
namespace starrocks::pipeline {
class HashPartitionSinkOperator final : public Operator {
public:
    HashPartitionSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                              HashPartitionContext* hash_partition_ctx)
            : Operator(factory, id, "hash_partition_sink", plan_node_id, driver_sequence),
              _hash_partition_ctx(hash_partition_ctx) {}

    Status prepare(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override { return true; }

    bool is_finished() const override { return _is_finished; }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    Status set_finishing(RuntimeState* state) override;

private:
    bool _is_finished = false;
    HashPartitionContext* _hash_partition_ctx;
};

class HashPartitionSinkOperatorFactory final : public OperatorFactory {
public:
    HashPartitionSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                     HashPartitionContextFactoryPtr hash_partition_ctx_factory);

    ~HashPartitionSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    HashPartitionContextFactoryPtr _hash_partition_ctx_factory;
};
} // namespace starrocks::pipeline
