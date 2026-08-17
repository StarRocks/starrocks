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

#include "storage/index/gist/rtree.h"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <limits>
#include <stack>

namespace starrocks {

// ============================================================================
// Low-level serialisation helpers (little-endian)
// ============================================================================

static void write_u8(std::string& buf, uint8_t v) {
    buf.push_back(static_cast<char>(v));
}

static void write_u32(std::string& buf, uint32_t v) {
    buf.append(reinterpret_cast<const char*>(&v), 4);
}

static void write_u64(std::string& buf, uint64_t v) {
    buf.append(reinterpret_cast<const char*>(&v), 8);
}

static void write_f64(std::string& buf, double v) {
    buf.append(reinterpret_cast<const char*>(&v), 8);
}

static void write_mbr(std::string& buf, const MBR& m) {
    write_f64(buf, m.min_x);
    write_f64(buf, m.min_y);
    write_f64(buf, m.max_x);
    write_f64(buf, m.max_y);
}

static void patch_u64(std::string& buf, size_t offset, uint64_t v) {
    memcpy(&buf[offset], &v, 8);
}

// ============================================================================
// Read helpers (little-endian, bounds-checked)
// ============================================================================

static inline uint8_t  read_u8 (const char* p) { return static_cast<uint8_t>(*p); }
static inline uint32_t read_u32(const char* p) { uint32_t v; memcpy(&v, p, 4); return v; }
static inline uint64_t read_u64(const char* p) { uint64_t v; memcpy(&v, p, 8); return v; }
static inline double   read_f64(const char* p) { double   v; memcpy(&v, p, 8); return v; }

static inline MBR read_mbr(const char* p) {
    return {read_f64(p), read_f64(p+8), read_f64(p+16), read_f64(p+24)};
}

// ============================================================================
// STR (Sort-Tile-Recursive) bulk-load
//
// Algorithm:
//   1. Sort entries by x-centre.
//   2. Divide into S = ceil(sqrt(n/M)) vertical slices.
//   3. Within each slice sort by y-centre and pack into nodes of M entries.
//   4. Recurse on parent-level internal entries until a single root.
// ============================================================================

struct InternalEntry {
    uint64_t child_offset;
    MBR mbr;
};

static MBR compute_union(const std::vector<RTreeLeafEntry>& entries, size_t from, size_t to) {
    MBR u = entries[from].mbr;
    for (size_t i = from + 1; i < to; ++i) u = mbr_union(u, entries[i].mbr);
    return u;
}

static MBR compute_union_internal(const std::vector<InternalEntry>& entries, size_t from, size_t to) {
    MBR u = entries[from].mbr;
    for (size_t i = from + 1; i < to; ++i) u = mbr_union(u, entries[i].mbr);
    return u;
}

// Write leaf node; return its start offset in buf.
static uint64_t write_leaf_node(std::string& buf,
                                const std::vector<RTreeLeafEntry>& entries,
                                size_t from, size_t to) {
    uint64_t offset = buf.size();
    write_u8(buf, 1u);                              // is_leaf
    write_u32(buf, static_cast<uint32_t>(to - from)); // num_entries
    for (size_t i = from; i < to; ++i) {
        write_u32(buf, entries[i].row_id);
        write_mbr(buf, entries[i].mbr);
    }
    return offset;
}

// Write internal node; return its start offset in buf.
static uint64_t write_internal_node(std::string& buf,
                                    const std::vector<InternalEntry>& entries,
                                    size_t from, size_t to) {
    uint64_t offset = buf.size();
    write_u8(buf, 0u);
    write_u32(buf, static_cast<uint32_t>(to - from));
    for (size_t i = from; i < to; ++i) {
        write_u64(buf, entries[i].child_offset);
        write_mbr(buf, entries[i].mbr);
    }
    return offset;
}

// Build leaf level: sort + pack into leaf nodes; return internal entries pointing to them.
static std::vector<InternalEntry> build_leaf_level(
        std::string& buf,
        std::vector<RTreeLeafEntry>& entries,
        int M) {
    const size_t n = entries.size();
    const int S = static_cast<int>(std::ceil(std::sqrt(static_cast<double>(n) / M)));
    const size_t slice_size = static_cast<size_t>(std::ceil(static_cast<double>(n) / S));

    // Sort by x-centre globally
    std::sort(entries.begin(), entries.end(), [](const RTreeLeafEntry& a, const RTreeLeafEntry& b) {
        return (a.mbr.min_x + a.mbr.max_x) < (b.mbr.min_x + b.mbr.max_x);
    });

    std::vector<InternalEntry> parents;
    for (int s = 0; s < S; ++s) {
        size_t s_from = static_cast<size_t>(s) * slice_size;
        size_t s_to   = std::min(s_from + slice_size, n);
        if (s_from >= n) break;

        // Sort slice by y-centre
        std::sort(entries.begin() + static_cast<ptrdiff_t>(s_from),
                  entries.begin() + static_cast<ptrdiff_t>(s_to),
                  [](const RTreeLeafEntry& a, const RTreeLeafEntry& b) {
                      return (a.mbr.min_y + a.mbr.max_y) < (b.mbr.min_y + b.mbr.max_y);
                  });

        // Pack into leaf nodes
        for (size_t i = s_from; i < s_to; i += static_cast<size_t>(M)) {
            size_t end = std::min(i + static_cast<size_t>(M), s_to);
            MBR node_mbr = compute_union(entries, i, end);
            uint64_t child_off = write_leaf_node(buf, entries, i, end);
            parents.push_back({child_off, node_mbr});
        }
    }
    return parents;
}

// Build internal level from parent entries; returns next-level parents.
static std::vector<InternalEntry> build_internal_level(
        std::string& buf,
        std::vector<InternalEntry>& entries,
        int M) {
    const size_t n = entries.size();
    const int S = static_cast<int>(std::ceil(std::sqrt(static_cast<double>(n) / M)));
    const size_t slice_size = static_cast<size_t>(std::ceil(static_cast<double>(n) / S));

    std::sort(entries.begin(), entries.end(), [](const InternalEntry& a, const InternalEntry& b) {
        return (a.mbr.min_x + a.mbr.max_x) < (b.mbr.min_x + b.mbr.max_x);
    });

    std::vector<InternalEntry> parents;
    for (int s = 0; s < S; ++s) {
        size_t s_from = static_cast<size_t>(s) * slice_size;
        size_t s_to   = std::min(s_from + slice_size, n);
        if (s_from >= n) break;

        std::sort(entries.begin() + static_cast<ptrdiff_t>(s_from),
                  entries.begin() + static_cast<ptrdiff_t>(s_to),
                  [](const InternalEntry& a, const InternalEntry& b) {
                      return (a.mbr.min_y + a.mbr.max_y) < (b.mbr.min_y + b.mbr.max_y);
                  });

        for (size_t i = s_from; i < s_to; i += static_cast<size_t>(M)) {
            size_t end = std::min(i + static_cast<size_t>(M), s_to);
            MBR node_mbr = compute_union_internal(entries, i, end);
            uint64_t child_off = write_internal_node(buf, entries, i, end);
            parents.push_back({child_off, node_mbr});
        }
    }
    return parents;
}

std::string rtree_build_str(std::vector<RTreeLeafEntry>& entries, int node_capacity) {
    assert(node_capacity >= 4 && node_capacity <= 1024);

    std::string buf;
    buf.reserve(entries.size() * 40 + RTREE_HEADER_SIZE);

    // Reserve header space (fill in root_offset and num_leaves later)
    buf.append(RTREE_HEADER_SIZE, '\0');

    uint64_t root_offset = RTREE_HEADER_SIZE; // default: empty tree points at header

    if (!entries.empty()) {
        // Build leaf level
        auto parents = build_leaf_level(buf, entries, node_capacity);

        // Build internal levels until one root node
        while (parents.size() > 1) {
            parents = build_internal_level(buf, parents, node_capacity);
        }

        if (parents.size() == 1) {
            // parents[0] points to the root node written by the last level
            root_offset = parents[0].child_offset;
        }
    }

    // Write header
    size_t pos = 0;
    memcpy(&buf[pos], RTREE_MAGIC, 8);          pos += 8;
    uint32_t version = RTREE_VERSION;
    memcpy(&buf[pos], &version, 4);             pos += 4;
    uint32_t cap = static_cast<uint32_t>(node_capacity);
    memcpy(&buf[pos], &cap, 4);                 pos += 4;
    uint64_t nleaves = entries.size();
    memcpy(&buf[pos], &nleaves, 8);             pos += 8;
    memcpy(&buf[pos], &root_offset, 8);         pos += 8;
    // reserved 8 bytes already zeroed

    return buf;
}

// ============================================================================
// Search — iterative DFS using an explicit stack of node offsets
// ============================================================================

void rtree_search(const char* data, size_t size,
                  std::function<bool(const MBR&)> node_predicate,
                  std::function<bool(const MBR&)> leaf_predicate,
                  std::vector<uint32_t>* result_row_ids) {
    if (size < RTREE_HEADER_SIZE) return;
    if (memcmp(data, RTREE_MAGIC, 8) != 0) return;

    // Validate version
    uint32_t version = read_u32(data + 8);
    if (version != RTREE_VERSION) return;

    uint64_t num_leaves  = read_u64(data + 16);
    uint64_t root_offset = read_u64(data + 24);
    if (num_leaves == 0) return;

    // DFS with an explicit stack
    std::stack<uint64_t> stk;
    stk.push(root_offset);

    while (!stk.empty()) {
        uint64_t node_off = stk.top(); stk.pop();
        if (node_off + 5 > size) continue;

        const char* p = data + node_off;
        uint8_t  is_leaf    = read_u8(p);     p += 1;
        uint32_t num_entries = read_u32(p);    p += 4;

        if (is_leaf) {
            // Leaf entry: 4 (row_id) + 32 (MBR) = 36 bytes
            for (uint32_t i = 0; i < num_entries; ++i) {
                if (static_cast<size_t>(p - data) + 36 > size) break;
                uint32_t row_id = read_u32(p); p += 4;
                MBR mbr = read_mbr(p);         p += 32;
                if (leaf_predicate(mbr)) {
                    result_row_ids->push_back(row_id);
                }
            }
        } else {
            // Internal entry: 8 (child_offset) + 32 (MBR) = 40 bytes
            for (uint32_t i = 0; i < num_entries; ++i) {
                if (static_cast<size_t>(p - data) + 40 > size) break;
                uint64_t child_offset = read_u64(p); p += 8;
                MBR mbr = read_mbr(p);               p += 32;
                if (node_predicate(mbr)) {
                    stk.push(child_offset);
                }
            }
        }
    }
}

// ============================================================================
// Convenience wrappers
// ============================================================================

void rtree_search_intersects(const char* data, size_t size,
                             const MBR& q,
                             std::vector<uint32_t>* result_row_ids) {
    auto pred = [&](const MBR& m) { return mbr_intersects(m, q); };
    rtree_search(data, size, pred, pred, result_row_ids);
}

void rtree_search_within(const char* data, size_t size,
                         const MBR& q,
                         std::vector<uint32_t>* result_row_ids) {
    // row is "within" query → row MBR is contained by query MBR
    // node pruning: node MBR must intersect query
    rtree_search(data, size,
                 [&](const MBR& m) { return mbr_intersects(m, q); },
                 [&](const MBR& m) { return mbr_contains(q, m); },
                 result_row_ids);
}

void rtree_search_contains(const char* data, size_t size,
                           const MBR& q,
                           std::vector<uint32_t>* result_row_ids) {
    // row "contains" query → row MBR must contain query MBR
    // node pruning: node MBR must contain or overlap query
    rtree_search(data, size,
                 [&](const MBR& m) { return mbr_intersects(m, q); },
                 [&](const MBR& m) { return mbr_contains(m, q); },
                 result_row_ids);
}

} // namespace starrocks
