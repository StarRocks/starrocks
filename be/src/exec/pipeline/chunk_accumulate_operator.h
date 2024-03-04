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
#include "storage/chunk_helper.h"

namespace starrocks {

class RuntimeState;

namespace pipeline {

// Accumulate chunks and output a chunk, until the number of rows of the input chunks is large enough.
class ChunkAccumulateOperator final : public Operator {
public:
    ChunkAccumulateOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence)
            : Operator(factory, id, "chunk_accumulate", plan_node_id, true, driver_sequence) {}

    ~ChunkAccumulateOperator() override = default;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    bool has_output() const override { return _acc.has_output(); }
    bool need_input() const override { return _acc.need_input(); }
    bool is_finished() const override { return _acc.is_finished(); }

    bool ignore_empty_eos() const override { return false; }

    Status set_finishing(RuntimeState* state) override;
    Status set_finished(RuntimeState* state) override;

    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) override;

private:
    ChunkPipelineAccumulator _acc;
};

class ChunkAccumulateOperatorFactory final : public OperatorFactory {
public:
    ChunkAccumulateOperatorFactory(int32_t id, int32_t plan_node_id)
            : OperatorFactory(id, "chunk_accumulate", plan_node_id) {}

    ~ChunkAccumulateOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<ChunkAccumulateOperator>(this, _id, _plan_node_id, driver_sequence);
    }
};

} // namespace pipeline
} // namespace starrocks
