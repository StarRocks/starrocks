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

#include "formats/parquet/column_read_order_ctx.h"

#include <algorithm>
#include <random>

namespace starrocks::parquet {

const std::vector<int>& ColumnReadOrderCtx::get_column_read_order() {
    // we will choose min round cost from ten random round order
    if (_rand_round_order_index > 0) {
        _trying_column_indices = _column_indices;
        std::shuffle(_trying_column_indices.begin(), _trying_column_indices.end(),
                     std::mt19937(std::random_device()()));
        return _trying_column_indices;
    } else {
        return _column_indices;
    }
}

void ColumnReadOrderCtx::update_ctx(size_t round_cost) {
    if (_rand_round_order_index > 0) {
        if (round_cost < _min_round_cost) {
            _column_indices = _trying_column_indices;
            _min_round_cost = round_cost;
        }
        _trying_column_indices.clear();
        _rand_round_order_index--;
    }
}

} // namespace starrocks::parquet