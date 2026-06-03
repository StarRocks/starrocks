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
#include <map>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "common/status.h"
#include "retrieval/index.h"
#include "retrieval/row_id_filter.h"
#include "retrieval/scored_result.h"

namespace starrocks {

// ============================================================
// Vector-specific enums
// ============================================================

enum class VectorIndexType {
    HNSW,
    DISKANN,
    IVFPQ,
};

enum class VectorDistanceType {
    COSINE,
    L2,
    IP,
};

// ============================================================
// VectorQuery — pure ANN search parameters
// ============================================================

struct VectorQuery {
    std::vector<float> query_vector;
    int32_t top_k = 0;
    VectorDistanceType distance_type = VectorDistanceType::L2;

    // Algorithm-specific search options.
    // HNSW: {"ef_search": "200"}
    // DiskANN: {"beam_width": "4", "list_size": "1536"}
    std::map<std::string, std::string> search_options;
};

// ============================================================
// VectorAnnIndex — vector ANN search interface, one modality of
// BaseIndex.
//
// Each implementation handles a single ANN algorithm and its own
// index file I/O. The interface is stateless per one-shot search:
// search() may be invoked multiple times. For lazy / iterative
// retrieval, make_iterator() returns a stateful ScoredRowIterator.
//
// Lifecycle: create -> init(path, meta) -> search/filtered_search
//            / make_iterator -> close
// ============================================================
class VectorAnnIndex : public BaseIndex {
public:
    struct IndexMeta {
        int32_t dim = 0;
        VectorIndexType index_type = VectorIndexType::HNSW;
        std::map<std::string, std::string> index_params;
    };

    ~VectorAnnIndex() override = default;

    virtual Status init(const std::string& path, const IndexMeta& meta) = 0;

    virtual Status search(const VectorQuery& query, ScoredResult* result) = 0;

    // Filtered search: the index skips rows not in `filter` during traversal.
    // Default implementation falls back to unfiltered search + post-filter.
    virtual Status filtered_search(const VectorQuery& query, const RowIdFilter& filter, ScoredResult* result);

    // Create a stateful, lazy result iterator. `filter` may be nullptr (no
    // filtering). The returned iterator borrows *this and must not outlive it.
    // In iterator semantics, query.top_k is an initial search-window hint
    // rather than a hard cap — the caller pulls as many results as it wants.
    virtual StatusOr<std::unique_ptr<ScoredRowIterator>> make_iterator(const VectorQuery& query,
                                                                       const RowIdFilter* filter) = 0;

    // BaseIndex contract. All vector indexes are the VECTOR category;
    // close()/mem_usage() stay pure and are implemented per algorithm.
    IndexType type() const override { return IndexType::VECTOR; }

    // The concrete ANN algorithm backing this index (HNSW / DiskANN / IVFPQ).
    virtual VectorIndexType algo_type() const = 0;

    // True if the index can efficiently skip filtered rows during traversal
    // (e.g., HNSW graph walk, DiskANN beam search). Callers may use this to
    // decide between in-search filtering and oversample + post-filter.
    virtual bool supports_efficient_filtered_search() const { return false; }
};

} // namespace starrocks
