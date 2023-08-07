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

#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/nljoin/nljoin_context.h"
#include "exec/pipeline/operator.h"

namespace starrocks::pipeline {

// NLJoinBuildOperator
// Collect data of right table into the cross-join-context
class NLJoinBuildOperator : public Operator {
public:
    NLJoinBuildOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, const int32_t driver_sequence,
                        const std::shared_ptr<NLJoinContext>& cross_join_context,
                        const char* name = "nestloop_join_build")
            : Operator(factory, id, name, plan_node_id, driver_sequence), _cross_join_context(cross_join_context) {
        _cross_join_context->ref();
    }

    ~NLJoinBuildOperator() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool has_output() const override { return false; }
    bool need_input() const override { return !is_finished(); }
    bool is_finished() const override { return _is_finished || _cross_join_context->is_finished(); }

    Status set_finishing(RuntimeState* state) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    OutputAmplificationType intra_pipeline_amplification_type() const override;
    size_t output_amplification_factor() const override;

private:
    std::atomic<bool> _is_finished = false;

protected:
    const std::shared_ptr<NLJoinContext>& _cross_join_context;
};

class NLJoinBuildOperatorFactory final : public OperatorFactory {
public:
    NLJoinBuildOperatorFactory(int32_t id, int32_t plan_node_id, std::shared_ptr<NLJoinContext> cross_join_context)
            : OperatorFactory(id, "nljoin_build", plan_node_id), _cross_join_context(std::move(cross_join_context)) {}

    ~NLJoinBuildOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<NLJoinBuildOperator>(this, _id, _plan_node_id, driver_sequence, _cross_join_context);
    }

private:
    std::shared_ptr<NLJoinContext> _cross_join_context;
};

} // namespace starrocks::pipeline
