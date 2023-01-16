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

#include "column/vectorized_fwd.h"
#include "exec/pipeline/operator.h"
#include "runtime/descriptors.h"

namespace starrocks {
class ExprContext;

namespace pipeline {
class SelectOperator final : public Operator {
public:
    SelectOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                   const std::vector<ExprContext*>& conjunct_ctxs)
            : Operator(factory, id, "select", plan_node_id, driver_sequence), _conjunct_ctxs(conjunct_ctxs) {}

    ~SelectOperator() override = default;
    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    bool has_output() const override { return _curr_chunk != nullptr || _pre_output_chunk != nullptr; }
    bool need_input() const override;
    bool is_finished() const override { return _is_finished && !_curr_chunk && !_pre_output_chunk; }

    Status set_finishing(RuntimeState* state) override {
        _is_finished = true;
        return Status::OK();
    }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    Status reset_state(starrocks::RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

private:
    // _curr_chunk used to receive input chunks, and apply predicate filtering.
    ChunkPtr _curr_chunk = nullptr;
    // _pre_output_chunk used to merge small _curr_chunk until it's big enough, then return as output.
    ChunkPtr _pre_output_chunk = nullptr;

    const std::vector<ExprContext*>& _conjunct_ctxs;

    bool _is_finished = false;
};

class SelectOperatorFactory final : public OperatorFactory {
public:
    SelectOperatorFactory(int32_t id, int32_t plan_node_id, std::vector<ExprContext*>&& conjunct_ctxs)
            : OperatorFactory(id, "select", plan_node_id), _conjunct_ctxs(std::move(conjunct_ctxs)) {}

    ~SelectOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<SelectOperator>(this, _id, _plan_node_id, driver_sequence, _conjunct_ctxs);
    }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    std::vector<ExprContext*> _conjunct_ctxs;
};

} // namespace pipeline
} // namespace starrocks
