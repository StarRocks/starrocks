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

#include "storage/index/vector/hnsw_ann_index.h"

#include <fmt/format.h>

namespace starrocks {

Status HnswAnnIndex::init(const std::string& path, const IndexMeta& meta) {
    _index_path = path;
    _dim = meta.dim;

    ASSIGN_OR_RETURN(_fs, FileSystem::CreateSharedFromString(path));

    if (auto it = meta.index_params.find("M"); it != meta.index_params.end()) {
        _M = std::stoi(it->second);
    }
    if (auto it = meta.index_params.find("ef_construction"); it != meta.index_params.end()) {
        _ef_construction = std::stoi(it->second);
    }

    // TODO: read HNSW index file via _fs and build the in-memory graph structure.
    // The graph consists of multiple layers; each node stores a vector and a
    // neighbor list per layer. The entry point is stored in the index header.

    _initialized = true;
    return Status::OK();
}

Status HnswAnnIndex::search(const VectorQuery& query, VectorAnnResult* result) {
    if (!_initialized) {
        return Status::InternalError("HnswAnnIndex not initialized");
    }

    [[maybe_unused]] int32_t ef_search = get_ef_search(query);

    // TODO: implement HNSW greedy beam search
    // 1. Start from the entry point at the top layer
    // 2. Greedily descend to layer 0 using beam search with ef=1
    // 3. At layer 0, do beam search with ef=ef_search
    // 4. Return the top_k closest neighbors from the candidate set
    // 5. Populate result->row_ids and result->scores

    return Status::NotSupported("HNSW ANN search not yet implemented");
}

Status HnswAnnIndex::filtered_search(const VectorQuery& query, const RowIdFilter& filter, VectorAnnResult* result) {
    if (!_initialized) {
        return Status::InternalError("HnswAnnIndex not initialized");
    }

    if (filter.empty()) {
        return search(query, result);
    }

    [[maybe_unused]] int32_t ef_search = get_ef_search(query);

    // TODO: implement filtered HNSW search
    // Same as search() but during graph traversal at layer 0, skip nodes
    // where !filter.contains(node_id). This naturally integrates pre-filter
    // into the ANN search without separate bitmap intersection.
    //
    // If the filter is too sparse (filter.count() / total_nodes < threshold),
    // increase ef_search proportionally to maintain recall.

    return Status::NotSupported("HNSW filtered ANN search not yet implemented");
}

Status HnswAnnIndex::close() {
    // TODO: release in-memory graph structure
    _initialized = false;
    return Status::OK();
}

int64_t HnswAnnIndex::mem_usage() const {
    // TODO: return actual memory usage of the loaded HNSW graph
    return 0;
}

int32_t HnswAnnIndex::get_ef_search(const VectorQuery& query) const {
    if (auto it = query.search_options.find("ef_search"); it != query.search_options.end()) {
        return std::stoi(it->second);
    }
    return 200; // sensible default
}

} // namespace starrocks
