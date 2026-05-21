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

#include <string>
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
//
// File paths are interned to avoid per-row string copies: a side map assigns
// a compact integer ID to each unique path, and the seen-set stores (path_id, pos)
// pairs instead of (string, int64). This reduces memory from ~40 bytes/entry to
// 12 bytes/entry for the common case of many rows per file.
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
        std::size_t operator()(const std::pair<int32_t, int64_t>& p) const {
            // Combine two integers with good avalanche properties (boost hash_combine pattern)
            std::size_t seed = std::hash<int32_t>{}(p.first);
            seed ^= std::hash<int64_t>{}(p.second) + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
            return seed;
        }
    };

    // Estimated heap footprint of the seen-set and the path intern table,
    // exposed via the SeenSetMemoryUsage profile counter.
    int64_t _seen_memory_usage() const;

    const std::vector<int32_t> _unique_key_col_indices;
    // Intern table: file path string -> compact integer ID
    phmap::flat_hash_map<std::string, int32_t> _path_ids;
    int32_t _next_path_id = 0;
    // Seen set: (path_id, row_position) — 12 bytes per entry instead of ~40+
    phmap::flat_hash_set<std::pair<int32_t, int64_t>, PairHash> _seen;
    ChunkPtr _output_chunk;
    bool _input_finished = false;
    RuntimeProfile::Counter* _seen_rows_counter = nullptr;
    RuntimeProfile::Counter* _seen_memory_counter = nullptr;
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
