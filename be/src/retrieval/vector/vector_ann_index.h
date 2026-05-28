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
// RowIdFilter — abstract row filter (faiss IDSelector-inspired)
//
// Hot-path interface: a single virtual is_member(). ANN graph
// traversal may invoke this hundreds of millions of times per
// search, so the base interface is intentionally minimal — a
// single vtable indirection per call, no variant tag check, no
// std::function indirection.
//
// Open for extension: callers may add new subclasses; ANN
// implementations may dynamic_cast to a known concrete subclass
// to take a specialized fast path.
// ============================================================
class RowIdFilter {
public:
    virtual ~RowIdFilter() = default;
    virtual bool is_member(int64_t row_id) const = 0;
};

// Roaring bitmap-backed filter. Owns the bitmap.
//
// Uses 32-bit roaring::Roaring: segment-local row ids fit in uint32,
// matching the convention used by storage's DelIdFilter.
class BitmapRowIdFilter final : public RowIdFilter {
public:
    explicit BitmapRowIdFilter(roaring::Roaring bitmap) : _bitmap(std::move(bitmap)) {}

    bool is_member(int64_t row_id) const override {
        return row_id >= 0 && row_id <= UINT32_MAX && _bitmap.contains(static_cast<uint32_t>(row_id));
    }

    uint64_t cardinality() const { return _bitmap.cardinality(); }

    const roaring::Roaring& bitmap() const { return _bitmap; }

private:
    roaring::Roaring _bitmap;
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
