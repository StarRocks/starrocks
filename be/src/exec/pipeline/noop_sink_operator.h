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

namespace starrocks::pipeline {

// NoopSinkOperator is just a placeholder sink operator.
// It can receive infinite chunks from the previous operator and do nothing.
// It will be finished, when the previous operator is finished.
class NoopSinkOperator final : public Operator {
public:
    NoopSinkOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : Operator(factory, id, "noop_sink", plan_node_id, driver_sequence) {}

    ~NoopSinkOperator() override = default;

    bool need_input() const override { return !_is_finished; }

    bool has_output() const override { return false; }

    bool is_finished() const override { return _is_finished; }

    Status set_finishing(RuntimeState* state) override {
        _is_finished = true;
        return Status::OK();
    }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override {
        return Status::InternalError("Shouldn't pull chunk from sink operator");
    }

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override { return Status::OK(); }

private:
    bool _is_finished{false};
};

class NoopSinkOperatorFactory final : public OperatorFactory {
public:
    NoopSinkOperatorFactory(int32_t id, int32_t plan_node_id) : OperatorFactory(id, "noop_sink", plan_node_id) {}

    ~NoopSinkOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<NoopSinkOperator>(this, _id, _plan_node_id, driver_sequence);
    }
};

} // namespace starrocks::pipeline
