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

#include "exec/pipeline/operator.h"
#include "exec/pipeline/sort/local_partition_topn_context.h"

namespace starrocks::pipeline {

// The purpose of LocalPartitionTopn{Sink/Source}Operator is to reduce the amount of data,
// so the output chunks are still remain unordered
//                                   ┌────► topn─────┐
//                                   │               │
// (unordered)                       │               │                  (unordered)
// inputChunks ───────► partitioner ─┼────► topn ────┼─► gather ─────► outputChunks
//                                   │               │
//                                   │               │
//                                   └────► topn ────┘
class LocalPartitionTopnSinkOperator : public Operator {
public:
    LocalPartitionTopnSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                   LocalPartitionTopnContext* partition_topn_ctx);

    ~LocalPartitionTopnSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;

    bool has_output() const override { return false; }

    bool need_input() const override { return true; }

    bool is_finished() const override { return _is_finished; }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    Status set_finishing(RuntimeState* state) override;

private:
    bool _is_finished = false;

    LocalPartitionTopnContext* _partition_topn_ctx;
};

class LocalPartitionTopnSinkOperatorFactory final : public OperatorFactory {
public:
    template <class... Args>
    LocalPartitionTopnSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                          LocalPartitionTopnContextFactoryPtr partition_topn_ctx_factory,
                                          Args&&... unused)
            : OperatorFactory(id, "local_partition_topn_sink", plan_node_id),
              _partition_topn_ctx_factory(std::move(partition_topn_ctx_factory)) {}
    ~LocalPartitionTopnSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    Status prepare(RuntimeState* state) override;

private:
    LocalPartitionTopnContextFactoryPtr _partition_topn_ctx_factory;
};
} // namespace starrocks::pipeline
