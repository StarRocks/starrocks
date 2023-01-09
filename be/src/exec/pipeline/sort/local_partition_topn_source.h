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

#include "exec/pipeline/sort/local_partition_topn_context.h"
#include "exec/pipeline/source_operator.h"

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
class LocalPartitionTopnSourceOperator : public SourceOperator {
public:
    LocalPartitionTopnSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                     int32_t driver_sequence, LocalPartitionTopnContext* partition_topn_ctx);

    ~LocalPartitionTopnSourceOperator() override = default;

    bool has_output() const override;

    bool is_finished() const override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    LocalPartitionTopnContext* _partition_topn_ctx;
};

class LocalPartitionTopnSourceOperatorFactory final : public SourceOperatorFactory {
public:
    LocalPartitionTopnSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                            LocalPartitionTopnContextFactoryPtr partition_topn_ctx_factory);

    ~LocalPartitionTopnSourceOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    LocalPartitionTopnContextFactoryPtr _partition_topn_ctx_factory;
};
} // namespace starrocks::pipeline
