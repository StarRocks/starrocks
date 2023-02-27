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

#include "exec/pipeline/source_operator.h"

namespace starrocks::pipeline {

// EmptySetOperator returns an empty result set.
class EmptySetOperator final : public SourceOperator {
public:
    EmptySetOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : SourceOperator(factory, id, "empty_set", plan_node_id, driver_sequence) {}

    ~EmptySetOperator() override = default;

    bool has_output() const override { return false; }

    bool is_finished() const override { return true; }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override {
        return Status::InternalError("Shouldn't pull chunk from empty set operator");
    }
};

class EmptySetOperatorFactory final : public SourceOperatorFactory {
public:
    EmptySetOperatorFactory(int32_t id, int32_t plan_node_id) : SourceOperatorFactory(id, "empty_set", plan_node_id) {}

    ~EmptySetOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<EmptySetOperator>(this, _id, _plan_node_id, driver_sequence);
    }

    SourceOperatorFactory::AdaptiveState adaptive_state() const override { return AdaptiveState::ACTIVE; }
};

} // namespace starrocks::pipeline
