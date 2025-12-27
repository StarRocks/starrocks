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

namespace starrocks::pipeline {

class LocalPartitionHashTopnSinkOperator final : public Operator {
public:
    LocalPartitionHashTopnSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id,
                                       int32_t driver_sequence, LocalPartitionHashTopnContext* ctx)
            : Operator(factory, id, "local_partition_hash_topn_sink", plan_node_id, false, driver_sequence),
              _ctx(ctx) {}

    Status prepare(RuntimeState* state) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    Status set_finishing(RuntimeState* state) override;

    void set_execute_mode(int /*performance_level*/) override { /* do nothing */
    }

    bool has_output() const override { return false; }

    bool need_input() const override { return true; }

    bool is_finished() const override { return _is_finished; }

private:
    bool _is_finished = false;
    LocalPartitionHashTopnContext* _ctx;
    DECLARE_ONCE_DETECTOR(_set_finishing_once);
};

class LocalPartitionHashTopnSinkOperatorFactory final : public OperatorFactory {
public:
    template <class... Args>
    LocalPartitionHashTopnSinkOperatorFactory(int32_t id, int32_t plan_node_id,
                                              LocalPartitionHashTopnContextFactoryPtr ctx_factory, Args&&... args)
            : OperatorFactory(id, "local_partition_hash_topn_sink", plan_node_id),
              _ctx_factory(std::move(ctx_factory)) {}

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    Status prepare(RuntimeState* state) override;

private:
    LocalPartitionHashTopnContextFactoryPtr _ctx_factory;
};

} // namespace starrocks::pipeline
