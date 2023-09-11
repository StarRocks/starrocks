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

#include <cstddef>
#include <unordered_map>
#include <utility>
#include <vector>

namespace starrocks::parquet {

class ColumnReadOrderCtx {
public:
    ColumnReadOrderCtx(std::vector<int> col_idxs, size_t all_cost, std::unordered_map<int, size_t> col_cost)
            : _column_indices(std::move(col_idxs)), _min_round_cost(all_cost), _column_cost_map(std::move(col_cost)) {}

    ~ColumnReadOrderCtx() = default;

    const std::vector<int>& get_column_read_order();

    int get_column_cost(int col_idx) const { return _column_cost_map.at(col_idx); }

    void update_ctx(size_t round_cost);

    size_t get_min_round_cost() const { return _min_round_cost; }

private:
    // least cost order
    std::vector<int> _column_indices;
    // using column order
    std::vector<int> _trying_column_indices;
    // cost
    size_t _min_round_cost = 0;
    // rand round order index, select the min round_cost from 10 random order
    size_t _rand_round_order_index = 10;
    // column cost map index -> cost
    std::unordered_map<int, size_t> _column_cost_map;
};

} // namespace starrocks::parquet
