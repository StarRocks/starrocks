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
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <variant>
#include <vector>

#include "common/status.h"

namespace detail {
class Roaring64Map;
}

namespace starrocks {

// ============================================================
// Enums (shared with legacy vector_index_reader.h)
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
// VectorAnnResult — unified row-level search output
// ============================================================

struct VectorAnnResult {
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
// RowIdFilter — abstract row filter (bitmap or callback)
// ============================================================

class RowIdFilter {
public:
    using Callback = std::function<bool(int64_t)>;

    RowIdFilter() = default;

    static RowIdFilter from_bitmap(std::shared_ptr<detail::Roaring64Map> bitmap) {
        RowIdFilter f;
        f._impl = std::move(bitmap);
        return f;
    }

    static RowIdFilter from_callback(Callback fn) {
        RowIdFilter f;
        f._impl = std::move(fn);
        return f;
    }

    bool contains(int64_t row_id) const;

    // Returns the number of set bits, or -1 if unknown (callback mode).
    int64_t count() const;

    bool empty() const { return std::holds_alternative<std::monostate>(_impl); }

private:
    std::variant<std::monostate, std::shared_ptr<detail::Roaring64Map>, Callback> _impl;
};

// ============================================================
// VectorAnnIndex — pure ANN search interface (Layer 1)
//
// Each implementation handles a single ANN algorithm and its
// own index file I/O. The interface is stateless per-search:
// the caller may invoke search() multiple times (important for
// iterative search strategies).
//
// Lifecycle: create -> init(path, meta) -> search/filtered_search -> close
// ============================================================

class VectorAnnIndex {
public:
    struct IndexMeta {
        int32_t dim = 0;
        VectorIndexType index_type = VectorIndexType::HNSW;
        std::map<std::string, std::string> index_params;
    };

    virtual ~VectorAnnIndex() = default;

    virtual Status init(const std::string& path, const IndexMeta& meta) = 0;

    virtual Status search(const VectorQuery& query, VectorAnnResult* result) = 0;

    // Filtered search: the index skips rows not in `filter` during traversal.
    // Default implementation falls back to unfiltered search + post-filter.
    virtual Status filtered_search(const VectorQuery& query, const RowIdFilter& filter, VectorAnnResult* result);

    virtual Status close() = 0;

    virtual int64_t mem_usage() const = 0;

    virtual VectorIndexType type() const = 0;

    // True if the index can efficiently skip filtered rows during traversal
    // (e.g., HNSW graph walk, DiskANN beam search).
    // When false, FilterStrategy will prefer post-filter over pre-filter.
    virtual bool supports_efficient_filtered_search() const { return false; }
};

} // namespace starrocks
