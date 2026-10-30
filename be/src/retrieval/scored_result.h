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

#include <cstdint>
#include <vector>

namespace starrocks {

// ============================================================
// ScoredResult — unified row-level scored output shared by every
// retrieval modality. Just (row_id, score) pairs in best-first
// (non-increasing relevance) order. A vector ANN search and a
// full-text BM25 search both fill the same shape.
// ============================================================
struct ScoredResult {
    std::vector<int64_t> row_ids;
    std::vector<float> scores;
    int32_t result_count = 0;

    void reserve(int32_t n) {
        row_ids.reserve(n);
        scores.reserve(n);
    }

    void clear() {
        row_ids.clear();
        scores.clear();
        result_count = 0;
    }

    void add(int64_t row_id, float score) {
        row_ids.push_back(row_id);
        scores.push_back(score);
        ++result_count;
    }
};

} // namespace starrocks
