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

#include "exec/pipeline/adaptive/adaptive_fwd.h"
#include "exec/pipeline/operator.h"

namespace starrocks::pipeline {

class CollectStatsSinkOperator final : public Operator {
public:
    CollectStatsSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, const int32_t driver_sequence,
                             CollectStatsContextRawPtr ctx);
    ~CollectStatsSinkOperator() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool has_output() const override;
    bool need_input() const override;
    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

private:
    CollectStatsContextRawPtr _ctx;
    bool _is_finishing = false;
};

class CollectStatsSinkOperatorFactory final : public OperatorFactory {
public:
    CollectStatsSinkOperatorFactory(int32_t id, int32_t plan_node_id, CollectStatsContextPtr ctx);
    ~CollectStatsSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<CollectStatsSinkOperator>(this, _id, _plan_node_id, driver_sequence, _ctx.get());
    }

private:
    CollectStatsContextPtr _ctx;
};

} // namespace starrocks::pipeline
