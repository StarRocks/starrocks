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
#include <utility>
#include <vector>

#include "common/status.h"

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

// ============================================================
// ScoredRowIterator — stateful, lazy (row_id, score) stream
//                      (Milvus/Knowhere IndexIterator style)
//
// Query-agnostic: each index implementation returns its own concrete
// subclass, holding the algorithm-specific traversal state (HNSW
// visited set + candidate heap, DiskANN beam, full-text posting-list
// cursor, ...). Results are produced one at a time in best-first
// (non-increasing relevance) order.
//
// When a row filter is supplied at creation time, the iterator
// validates each candidate against it lazily and keeps advancing the
// underlying traversal until a matching result is found — this is the
// native "iterative filter" path.
//
// Lifetime: an iterator borrows the index it was created from and
// must not outlive it.
// ============================================================
class ScoredRowIterator {
public:
    virtual ~ScoredRowIterator() = default;

    // Whether at least one more result can be produced. May lazily
    // advance the underlying traversal (expand ef_search / beam, fetch
    // the next page), hence non-const.
    virtual bool has_next() = 0;

    // Produce the next (row_id, score) in best-first order.
    // Precondition: has_next() == true.
    virtual StatusOr<std::pair<int64_t, float>> next() = 0;
};

} // namespace starrocks
