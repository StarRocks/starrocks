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
#include <string>
#include <vector>

namespace starrocks {

// ----------------------------------------------------------------------------
// Minimum Bounding Rectangle (MBR) — axis-aligned bounding box in 2D.
// ----------------------------------------------------------------------------
struct MBR {
    double min_x{0}, min_y{0}, max_x{0}, max_y{0};
};

inline bool mbr_intersects(const MBR& a, const MBR& b) {
    return a.min_x <= b.max_x && a.max_x >= b.min_x &&
           a.min_y <= b.max_y && a.max_y >= b.min_y;
}

// a fully contains b
inline bool mbr_contains(const MBR& a, const MBR& b) {
    return a.min_x <= b.min_x && a.max_x >= b.max_x &&
           a.min_y <= b.min_y && a.max_y >= b.max_y;
}

inline MBR mbr_union(const MBR& a, const MBR& b) {
    return {std::min(a.min_x, b.min_x), std::min(a.min_y, b.min_y),
            std::max(a.max_x, b.max_x), std::max(a.max_y, b.max_y)};
}

// ----------------------------------------------------------------------------
// Packed R-Tree file format (version 1, little-endian)
//
// [Header — 40 bytes]
//   magic[8]          : "GSTROCKS"
//   version[4]        : uint32 = 1
//   node_capacity[4]  : uint32
//   num_leaves[8]     : uint64
//   root_offset[8]    : uint64  (byte offset within file of root node)
//   _reserved[8]      : padding
//
// [Node block — variable]
//   is_leaf[1]        : uint8  (1=leaf, 0=internal)
//   num_entries[4]    : uint32
//   entries[N]:
//     leaf entry (36 bytes)  : row_id[4] + min_x[8] + min_y[8] + max_x[8] + max_y[8]
//     inner entry (40 bytes) : child_offset[8] + min_x[8] + min_y[8] + max_x[8] + max_y[8]
// ----------------------------------------------------------------------------

static constexpr uint32_t RTREE_VERSION = 1;
static constexpr char     RTREE_MAGIC[8] = {'G','S','T','R','O','C','K','S'};
static constexpr size_t   RTREE_HEADER_SIZE = 40;

// ----------------------------------------------------------------------------
// Leaf entry: one geometry row
// ----------------------------------------------------------------------------
struct RTreeLeafEntry {
    uint32_t row_id;
    MBR mbr;
};

// ----------------------------------------------------------------------------
// Build a packed STR R-Tree from leaf entries.
// Entries are sorted in-place during build (STR algorithm).
// Returns the serialised tree as a binary string.
// node_capacity: max entries per node (4–1024)
// ----------------------------------------------------------------------------
std::string rtree_build_str(std::vector<RTreeLeafEntry>& entries, int node_capacity);

// ----------------------------------------------------------------------------
// Search the serialised R-Tree.
// node_predicate: called on each node MBR — return false to prune subtree.
// leaf_predicate: called on each leaf MBR — return true to include row_id.
// result_row_ids is appended (not cleared) with matching row ordinals.
// ----------------------------------------------------------------------------
void rtree_search(const char* data, size_t size,
                  std::function<bool(const MBR&)> node_predicate,
                  std::function<bool(const MBR&)> leaf_predicate,
                  std::vector<uint32_t>* result_row_ids);

// Convenience wrappers for the three common spatial predicates ---------------

// Returns rows whose MBR intersects query_mbr (candidate set for ST_Intersects)
void rtree_search_intersects(const char* data, size_t size,
                             const MBR& query_mbr,
                             std::vector<uint32_t>* result_row_ids);

// Returns rows whose MBR is contained within query_mbr (candidate for ST_Within)
void rtree_search_within(const char* data, size_t size,
                         const MBR& query_mbr,
                         std::vector<uint32_t>* result_row_ids);

// Returns rows whose MBR contains query_mbr (candidate for ST_Contains)
void rtree_search_contains(const char* data, size_t size,
                           const MBR& query_mbr,
                           std::vector<uint32_t>* result_row_ids);

} // namespace starrocks
