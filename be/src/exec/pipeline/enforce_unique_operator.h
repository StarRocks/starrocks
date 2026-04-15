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
#include <vector>

#include "base/phmap/phmap.h"
#include "column/chunk.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/operator_factory.h"

namespace starrocks::pipeline {

// EnforceUniqueOperator verifies that the key columns (file_path, row_position)
// contain no duplicate values across all input chunks. If a duplicate is found,
// push_chunk returns a RuntimeError. Otherwise, the chunk is passed through
// unchanged. This is used by the MERGE INTO planner to ensure that each target
// row is matched by at most one source row.
class EnforceUniqueOperator final : public Operator {
public:
    EnforceUniqueOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                          const std::vector<int32_t>& unique_key_col_indices)
            : Operator(factory, id, "enforce_unique", plan_node_id, false, driver_sequence),
              _unique_key_col_indices(unique_key_col_indices) {}

    ~EnforceUniqueOperator() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool has_output() const override;
    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    bool need_input() const override;
    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    Status set_finishing(RuntimeState* state) override;
    bool is_finished() const override;

private:
    struct PairHash {
        std::size_t operator()(const std::pair<std::string, int64_t>& p) const {
            auto h1 = std::hash<std::string>{}(p.first);
            auto h2 = std::hash<int64_t>{}(p.second);
            return h1 ^ (h2 * 2654435761u);
        }
    };

    const std::vector<int32_t> _unique_key_col_indices;
    phmap::flat_hash_set<std::pair<std::string, int64_t>, PairHash> _seen;
    ChunkPtr _output_chunk;
    bool _input_finished = false;
};

class EnforceUniqueOperatorFactory final : public OperatorFactory {
public:
    EnforceUniqueOperatorFactory(int32_t id, int32_t plan_node_id, std::vector<int32_t> unique_key_col_indices)
            : OperatorFactory(id, "enforce_unique", plan_node_id),
              _unique_key_col_indices(std::move(unique_key_col_indices)) {}

    ~EnforceUniqueOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<EnforceUniqueOperator>(this, _id, _plan_node_id, driver_sequence,
                                                       _unique_key_col_indices);
    }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    std::vector<int32_t> _unique_key_col_indices;
};

} // namespace starrocks::pipeline
