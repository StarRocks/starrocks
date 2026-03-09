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

#include "fs/fs.h"
#include "storage/index/vector/vector_ann_index.h"

namespace starrocks {

// HNSW (Hierarchical Navigable Small World) ANN index.
//
// Index params (in IndexMeta::index_params):
//   "M"                - max connections per layer (default 16)
//   "ef_construction"  - size of dynamic candidate list during build (default 200)
//
// Search options (in VectorQuery::search_options):
//   "ef_search"        - size of dynamic candidate list during search (default 200)
class HnswAnnIndex final : public VectorAnnIndex {
public:
    HnswAnnIndex() = default;
    ~HnswAnnIndex() override = default;

    Status init(const std::string& path, const IndexMeta& meta) override;
    Status search(const VectorQuery& query, VectorAnnResult* result) override;
    Status filtered_search(const VectorQuery& query, const RowIdFilter& filter, VectorAnnResult* result) override;
    Status close() override;
    int64_t mem_usage() const override;
    VectorIndexType type() const override { return VectorIndexType::HNSW; }
    bool supports_efficient_filtered_search() const override { return true; }

private:
    int32_t get_ef_search(const VectorQuery& query) const;

    std::shared_ptr<FileSystem> _fs;
    std::string _index_path;
    int32_t _dim = 0;
    int32_t _M = 16;
    int32_t _ef_construction = 200;
    bool _initialized = false;
};

} // namespace starrocks
