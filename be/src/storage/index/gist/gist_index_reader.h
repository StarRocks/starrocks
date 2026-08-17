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
#include <vector>

#include "common/status.h"
#include "storage/index/gist/rtree.h"

namespace starrocks {

/**
 * GiSTIndexReader loads a .gst file and evaluates spatial predicate queries
 * against its packed R-Tree, returning candidate row ordinals.
 *
 * The caller must verify the candidates with exact geometry predicates after
 * using the index to filter (the index uses MBRs, not exact geometry).
 *
 * Lifecycle:  init() → search_*()
 */
class GiSTIndexReader {
public:
    /// Load the index file into memory.
    Status init(const std::string& index_file_path);

    /// Returns row_ids whose MBR intersects query_mbr.
    /// Use for ST_Intersects predicate candidates.
    Status search_intersects(const MBR& query_mbr, std::vector<uint32_t>* result_row_ids) const;

    /// Returns row_ids whose MBR is fully contained within query_mbr.
    /// Use for ST_Within predicate candidates (tighter than intersects).
    Status search_within(const MBR& query_mbr, std::vector<uint32_t>* result_row_ids) const;

    /// Returns row_ids whose MBR fully contains query_mbr.
    /// Use for ST_Contains predicate candidates.
    Status search_contains(const MBR& query_mbr, std::vector<uint32_t>* result_row_ids) const;

    bool is_loaded() const { return !_index_data.empty(); }

private:
    std::string _index_data;
};

} // namespace starrocks
