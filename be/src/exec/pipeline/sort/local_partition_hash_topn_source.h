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
#include "exec/pipeline/sort/local_partition_hash_topn_context.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks::pipeline {

class LocalPartitionHashTopnSourceOperator final : public SourceOperator {
public:
    LocalPartitionHashTopnSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                         int32_t driver_sequence, LocalPartitionHashTopnContext* ctx)
            : SourceOperator(factory, id, "local_partition_hash_topn_source", plan_node_id, false, driver_sequence),
              _ctx(ctx) {}

    Status prepare(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    LocalPartitionHashTopnContext* _ctx;
};

class LocalPartitionHashTopnSourceOperatorFactory final : public SourceOperatorFactory {
public:
    LocalPartitionHashTopnSourceOperatorFactory(int32_t id, int32_t plan_node_id,
                                                LocalPartitionHashTopnContextFactoryPtr ctx_factory)
            : SourceOperatorFactory(id, "local_partition_hash_topn_source", plan_node_id),
              _ctx_factory(std::move(ctx_factory)) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    LocalPartitionHashTopnContextFactoryPtr _ctx_factory;
};

} // namespace starrocks::pipeline
