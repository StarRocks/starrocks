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

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>

namespace starrocks {

// -----------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------

static RTreeLeafEntry make_point(uint32_t row_id, double x, double y) {
    return {row_id, {x, y, x, y}};
}

static RTreeLeafEntry make_rect(uint32_t row_id, double x0, double y0, double x1, double y1) {
    return {row_id, {x0, y0, x1, y1}};
}

static std::vector<uint32_t> sorted(std::vector<uint32_t> v) {
    std::sort(v.begin(), v.end());
    return v;
}

// -----------------------------------------------------------------------
// MBR predicate unit tests
// -----------------------------------------------------------------------

TEST(MBRTest, Intersects) {
    MBR a{0, 0, 5, 5};
    MBR b{3, 3, 8, 8};
    MBR c{6, 6, 9, 9};
    EXPECT_TRUE(mbr_intersects(a, b));
    EXPECT_TRUE(mbr_intersects(b, a));
    EXPECT_FALSE(mbr_intersects(a, c));
    EXPECT_FALSE(mbr_intersects(c, a));
}

TEST(MBRTest, Contains) {
    MBR outer{0, 0, 10, 10};
    MBR inner{2, 2, 8, 8};
    MBR overlap{8, 8, 15, 15};
    EXPECT_TRUE(mbr_contains(outer, inner));
    EXPECT_FALSE(mbr_contains(inner, outer));
    EXPECT_FALSE(mbr_contains(outer, overlap));
}

TEST(MBRTest, PointOnBoundary) {
    MBR box{0, 0, 5, 5};
    MBR pt{5, 5, 5, 5}; // corner point
    EXPECT_TRUE(mbr_intersects(box, pt));
    EXPECT_TRUE(mbr_contains(box, pt));
}

// -----------------------------------------------------------------------
// Build and search: empty tree
// -----------------------------------------------------------------------

TEST(RTreeTest, EmptyTree) {
    std::vector<RTreeLeafEntry> entries;
    std::string data = rtree_build_str(entries, 50);
    ASSERT_GE(data.size(), RTREE_HEADER_SIZE);

    std::vector<uint32_t> result;
    MBR q{0, 0, 100, 100};
    rtree_search_intersects(data.data(), data.size(), q, &result);
    EXPECT_TRUE(result.empty());
}

// -----------------------------------------------------------------------
// Single entry
// -----------------------------------------------------------------------

TEST(RTreeTest, SinglePointIntersects) {
    std::vector<RTreeLeafEntry> entries = {make_point(42, 5.0, 7.0)};
    std::string data = rtree_build_str(entries, 50);

    std::vector<uint32_t> result;
    rtree_search_intersects(data.data(), data.size(), {0, 0, 10, 10}, &result);
    ASSERT_EQ(1u, result.size());
    EXPECT_EQ(42u, result[0]);
}

TEST(RTreeTest, SinglePointMiss) {
    std::vector<RTreeLeafEntry> entries = {make_point(0, 20.0, 20.0)};
    std::string data = rtree_build_str(entries, 50);

    std::vector<uint32_t> result;
    rtree_search_intersects(data.data(), data.size(), {0, 0, 10, 10}, &result);
    EXPECT_TRUE(result.empty());
}

// -----------------------------------------------------------------------
// 100 random points — correctness + recall
// -----------------------------------------------------------------------

TEST(RTreeTest, HundredPointsIntersects) {
    const int N = 100;
    std::vector<RTreeLeafEntry> entries;
    entries.reserve(N);
    for (int i = 0; i < N; ++i) {
        double x = (i * 13 % 101) * 0.5; // pseudo-random spread
        double y = (i * 7 % 97) * 0.5;
        entries.push_back(make_point(static_cast<uint32_t>(i), x, y));
    }

    std::string data = rtree_build_str(entries, 10);

    // Query: box covering roughly half the space
    MBR q{0, 0, 25, 25};

    // Brute-force expected answer
    std::vector<uint32_t> expected;
    for (const auto& e : entries) {
        if (mbr_intersects(e.mbr, q)) expected.push_back(e.row_id);
    }

    std::vector<uint32_t> result;
    rtree_search_intersects(data.data(), data.size(), q, &result);

    EXPECT_EQ(sorted(expected), sorted(result));
}

// -----------------------------------------------------------------------
// ST_Within candidates: result MBR must be inside query
// -----------------------------------------------------------------------

TEST(RTreeTest, WithinSearch) {
    std::vector<RTreeLeafEntry> entries = {
            make_point(0, 1, 1),        // inside query box (0,0,10,10)
            make_point(1, 5, 5),        // inside
            make_point(2, 15, 15),      // outside
            make_rect(3, 0, 0, 2, 2),   // fully inside
            make_rect(4, 8, 8, 12, 12), // partially outside
    };

    std::string data = rtree_build_str(entries, 50);
    MBR q{0, 0, 10, 10};

    std::vector<uint32_t> result;
    rtree_search_within(data.data(), data.size(), q, &result);
    std::sort(result.begin(), result.end());

    // rows 0, 1, 3 have MBR fully inside q; row 4 straddles boundary
    EXPECT_NE(result.end(), std::find(result.begin(), result.end(), 0u));
    EXPECT_NE(result.end(), std::find(result.begin(), result.end(), 1u));
    EXPECT_NE(result.end(), std::find(result.begin(), result.end(), 3u));
    EXPECT_EQ(result.end(), std::find(result.begin(), result.end(), 2u));
    EXPECT_EQ(result.end(), std::find(result.begin(), result.end(), 4u));
}

// -----------------------------------------------------------------------
// ST_Contains candidates: row MBR must contain query
// -----------------------------------------------------------------------

TEST(RTreeTest, ContainsSearch) {
    std::vector<RTreeLeafEntry> entries = {
            make_rect(0, 0, 0, 10, 10), // contains query (3,3,7,7)
            make_rect(1, 5, 5, 15, 15), // does not contain query
            make_rect(2, 2, 2, 8, 8),   // contains query
            make_point(3, 5, 5),        // does not contain (point)
    };

    std::string data = rtree_build_str(entries, 50);
    MBR q{3, 3, 7, 7};

    std::vector<uint32_t> result;
    rtree_search_contains(data.data(), data.size(), q, &result);
    std::sort(result.begin(), result.end());

    EXPECT_NE(result.end(), std::find(result.begin(), result.end(), 0u));
    EXPECT_NE(result.end(), std::find(result.begin(), result.end(), 2u));
    EXPECT_EQ(result.end(), std::find(result.begin(), result.end(), 1u));
    EXPECT_EQ(result.end(), std::find(result.begin(), result.end(), 3u));
}

// -----------------------------------------------------------------------
// Tree depth: many entries force multi-level tree
// -----------------------------------------------------------------------

TEST(RTreeTest, LargeDatasetMultiLevel) {
    const int N = 1000;
    std::vector<RTreeLeafEntry> entries;
    entries.reserve(N);
    for (int i = 0; i < N; ++i) {
        double x = static_cast<double>(i % 100);
        double y = static_cast<double>(i / 100);
        entries.push_back(make_point(static_cast<uint32_t>(i), x, y));
    }

    std::string data = rtree_build_str(entries, 16); // small node capacity → deep tree

    MBR q{10, 2, 30, 5}; // covers a rectangular slice

    std::vector<uint32_t> expected;
    for (const auto& e : entries) {
        if (mbr_intersects(e.mbr, q)) expected.push_back(e.row_id);
    }

    std::vector<uint32_t> result;
    rtree_search_intersects(data.data(), data.size(), q, &result);

    EXPECT_EQ(sorted(expected), sorted(result));
    EXPECT_FALSE(result.empty()); // sanity: we should find some
}

// -----------------------------------------------------------------------
// node_capacity = 4 (minimum) — forces maximum tree depth
// -----------------------------------------------------------------------

TEST(RTreeTest, MinNodeCapacity) {
    const int N = 50;
    std::vector<RTreeLeafEntry> entries;
    for (int i = 0; i < N; ++i) {
        entries.push_back(make_point(static_cast<uint32_t>(i), static_cast<double>(i), static_cast<double>(i)));
    }

    std::string data = rtree_build_str(entries, 4);

    // Query: diagonal strip
    MBR q{10, 10, 20, 20};
    std::vector<uint32_t> expected;
    for (const auto& e : entries) {
        if (mbr_intersects(e.mbr, q)) expected.push_back(e.row_id);
    }

    std::vector<uint32_t> result;
    rtree_search_intersects(data.data(), data.size(), q, &result);

    EXPECT_EQ(sorted(expected), sorted(result));
}

// -----------------------------------------------------------------------
// File header integrity
// -----------------------------------------------------------------------

TEST(RTreeTest, HeaderMagic) {
    std::vector<RTreeLeafEntry> entries = {make_point(0, 1.0, 2.0)};
    std::string data = rtree_build_str(entries, 50);

    ASSERT_GE(data.size(), RTREE_HEADER_SIZE);
    EXPECT_EQ(0, memcmp(data.data(), "GSTROCKS", 8));

    // Corrupt magic — search should return nothing
    data[0] = 'X';
    std::vector<uint32_t> result;
    rtree_search_intersects(data.data(), data.size(), {0, 0, 10, 10}, &result);
    EXPECT_TRUE(result.empty());
}

// -----------------------------------------------------------------------
// Append behavior: result vector is appended to, not cleared
// -----------------------------------------------------------------------
TEST(RTreeTest, SearchAppendsToExistingResults) {
    std::vector<RTreeLeafEntry> entries = {make_point(0, 5.0, 5.0)};
    std::string data = rtree_build_str(entries, 50);

    std::vector<uint32_t> result = {999u}; // pre-existing value
    MBR q{0, 0, 10, 10};
    rtree_search_intersects(data.data(), data.size(), q, &result);

    ASSERT_EQ(2u, result.size());
    EXPECT_EQ(999u, result[0]); // pre-existing preserved
    EXPECT_EQ(0u, result[1]);   // new result appended
}

// -----------------------------------------------------------------------
// Negative coordinates
// -----------------------------------------------------------------------
TEST(RTreeTest, NegativeCoordinates) {
    std::vector<RTreeLeafEntry> entries = {
            make_point(0, -10.0, -5.0),
            make_point(1, -0.5, -0.5),
            make_point(2, 5.0, 5.0),
    };
    std::string data = rtree_build_str(entries, 50);

    std::vector<uint32_t> result;
    rtree_search_intersects(data.data(), data.size(), {-15.0, -10.0, 0.0, 0.0}, &result);
    std::sort(result.begin(), result.end());
    ASSERT_EQ(2u, result.size());
    EXPECT_EQ(0u, result[0]);
    EXPECT_EQ(1u, result[1]);
    // row_id 2 is outside the negative-coord query
    EXPECT_EQ(result.end(), std::find(result.begin(), result.end(), 2u));
}

// -----------------------------------------------------------------------
// All points at the same location
// -----------------------------------------------------------------------
TEST(RTreeTest, AllPointsSameLocation) {
    std::vector<RTreeLeafEntry> entries;
    for (uint32_t i = 0; i < 20; ++i) entries.push_back(make_point(i, 3.0, 4.0));
    std::string data = rtree_build_str(entries, 10);

    std::vector<uint32_t> result;
    rtree_search_intersects(data.data(), data.size(), {2.5, 3.5, 3.5, 4.5}, &result);
    EXPECT_EQ(20u, result.size());

    // Query just outside should return nothing
    std::vector<uint32_t> miss;
    rtree_search_intersects(data.data(), data.size(), {5.0, 5.0, 10.0, 10.0}, &miss);
    EXPECT_TRUE(miss.empty());
}

// -----------------------------------------------------------------------
// Boundary exact match (query exactly touches a point MBR)
// -----------------------------------------------------------------------
TEST(RTreeTest, ExactBoundaryMatch) {
    std::vector<RTreeLeafEntry> entries = {make_point(42, 5.0, 5.0)};
    std::string data = rtree_build_str(entries, 50);

    // Query box with corner exactly at the point
    std::vector<uint32_t> result;
    rtree_search_intersects(data.data(), data.size(), {5.0, 5.0, 5.0, 5.0}, &result);
    EXPECT_EQ(1u, result.size());
    if (!result.empty()) EXPECT_EQ(42u, result[0]);
}

// -----------------------------------------------------------------------
// Query larger than entire dataset
// -----------------------------------------------------------------------
TEST(RTreeTest, QueryEncompassesAll) {
    const int N = 50;
    std::vector<RTreeLeafEntry> entries;
    for (int i = 0; i < N; ++i) entries.push_back(make_point(i, i * 0.1, i * 0.1));
    std::string data = rtree_build_str(entries, 8);

    std::vector<uint32_t> result;
    rtree_search_intersects(data.data(), data.size(), {-1e9, -1e9, 1e9, 1e9}, &result);
    EXPECT_EQ((size_t)N, result.size());
}

// -----------------------------------------------------------------------
// Degenerate MBR queries
// -----------------------------------------------------------------------
TEST(RTreeTest, ZeroAreaQuery) {
    std::vector<RTreeLeafEntry> entries = {
            make_point(0, 1.0, 1.0),
            make_point(1, 2.0, 2.0),
    };
    std::string data = rtree_build_str(entries, 50);

    // Zero-area point query that hits exactly on row 0
    std::vector<uint32_t> result;
    rtree_search_intersects(data.data(), data.size(), {1.0, 1.0, 1.0, 1.0}, &result);
    ASSERT_EQ(1u, result.size());
    EXPECT_EQ(0u, result[0]);
}

// -----------------------------------------------------------------------
// MBR predicate: floating-point near-boundary
// -----------------------------------------------------------------------
TEST(MBRTest, NearBoundaryStrictlyOutside) {
    MBR box{0.0, 0.0, 5.0, 5.0};
    // Point just outside (5.0 + epsilon)
    double eps = std::numeric_limits<double>::epsilon() * 10;
    MBR just_outside{5.0 + eps, 5.0, 5.0 + eps, 5.0};
    EXPECT_FALSE(mbr_intersects(box, just_outside));
    EXPECT_FALSE(mbr_contains(box, just_outside));
}

TEST(MBRTest, NegativeCoordinateMBR) {
    MBR a{-10.0, -10.0, -5.0, -5.0};
    MBR b{-7.0, -7.0, -3.0, -3.0};
    MBR c{0.0, 0.0, 5.0, 5.0};
    EXPECT_TRUE(mbr_intersects(a, b));
    EXPECT_FALSE(mbr_intersects(a, c));
    EXPECT_FALSE(mbr_contains(a, b));                       // partial overlap
    EXPECT_TRUE(mbr_contains(b, {-6.0, -6.0, -4.0, -4.0})); // inner box
}

TEST(MBRTest, ZeroWidthBox) {
    MBR line{0.0, 0.0, 0.0, 5.0}; // vertical line segment
    MBR pt{0.0, 2.5, 0.0, 2.5};
    EXPECT_TRUE(mbr_intersects(line, pt));
    EXPECT_TRUE(mbr_contains(line, pt));
}

// -----------------------------------------------------------------------
// Correctness: within-search returns NO false positives
// -----------------------------------------------------------------------
TEST(RTreeTest, WithinSearch_NoFalsePositives) {
    const int N = 100;
    std::vector<RTreeLeafEntry> entries;
    for (int i = 0; i < N; ++i) {
        double x = (i % 10) * 2.0;
        double y = (i / 10) * 2.0;
        entries.push_back(make_point(i, x, y));
    }
    std::string data = rtree_build_str(entries, 10);

    MBR q{3.0, 3.0, 7.0, 7.0};

    // R-Tree result (candidates)
    std::vector<uint32_t> tree_result;
    rtree_search_within(data.data(), data.size(), q, &tree_result);

    // Brute-force: find all rows where MBR is fully inside q
    for (uint32_t id : tree_result) {
        const auto& e = entries[id];
        EXPECT_TRUE(mbr_contains(q, e.mbr))
                << "False positive: row " << id << " at (" << e.mbr.min_x << "," << e.mbr.min_y << ")";
    }
}

// -----------------------------------------------------------------------
// Correctness: contains-search returns NO false positives
// -----------------------------------------------------------------------
TEST(RTreeTest, ContainsSearch_NoFalsePositives) {
    std::vector<RTreeLeafEntry> entries = {
            make_rect(0, 0, 0, 20, 20), // large — contains query
            make_rect(1, 5, 5, 8, 8),   // small — inside query, does not contain it
            make_rect(2, 3, 3, 18, 18), // medium — contains query
            make_rect(3, 0, 0, 10, 10), // border — contains query on one edge
    };
    std::string data = rtree_build_str(entries, 50);
    MBR q{4.0, 4.0, 7.0, 7.0};

    std::vector<uint32_t> result;
    rtree_search_contains(data.data(), data.size(), q, &result);

    for (uint32_t id : result) {
        const auto& e = entries[id];
        EXPECT_TRUE(mbr_contains(e.mbr, q)) << "False positive: row " << id;
    }
    // row 1 (smaller than query) must NOT appear
    EXPECT_EQ(result.end(), std::find(result.begin(), result.end(), 1u));
}

// -----------------------------------------------------------------------
// Truncated / undersized data should not crash
// -----------------------------------------------------------------------
TEST(RTreeTest, TruncatedData_DoesNotCrash) {
    std::vector<RTreeLeafEntry> entries = {make_point(0, 1.0, 2.0)};
    std::string data = rtree_build_str(entries, 50);

    // Try searching with progressively smaller buffers
    std::vector<uint32_t> result;
    for (size_t sz = 0; sz <= RTREE_HEADER_SIZE; ++sz) {
        result.clear();
        EXPECT_NO_FATAL_FAILURE(rtree_search_intersects(data.data(), sz, {0, 0, 10, 10}, &result))
                << "Crashed at size " << sz;
    }
    EXPECT_TRUE(result.empty());
}

// -----------------------------------------------------------------------
// Multiple independent searches on same tree (no internal state mutation)
// -----------------------------------------------------------------------
TEST(RTreeTest, RepeatedSearchGivesSameResults) {
    const int N = 50;
    std::vector<RTreeLeafEntry> entries;
    for (int i = 0; i < N; ++i) entries.push_back(make_point(i, i * 0.5, 0.0));
    std::string data = rtree_build_str(entries, 8);

    MBR q{5.0, -1.0, 15.0, 1.0};
    std::vector<uint32_t> r1, r2;
    rtree_search_intersects(data.data(), data.size(), q, &r1);
    rtree_search_intersects(data.data(), data.size(), q, &r2);
    EXPECT_EQ(sorted(r1), sorted(r2));
}

// -----------------------------------------------------------------------
// No false negatives: brute-force matches tree exactly
// -----------------------------------------------------------------------
TEST(RTreeTest, Intersects_BruteForceVsTree_NoFalseNegatives) {
    const int N = 80;
    std::vector<RTreeLeafEntry> entries;
    for (int i = 0; i < N; ++i) {
        double x = (i * 7) % 50;
        double y = (i * 11) % 50;
        entries.push_back(make_point(i, x, y));
    }
    std::string data = rtree_build_str(entries, 12);

    MBR q{10.0, 10.0, 30.0, 30.0};

    // Brute-force
    std::vector<uint32_t> expected;
    for (const auto& e : entries)
        if (mbr_intersects(e.mbr, q)) expected.push_back(e.row_id);

    std::vector<uint32_t> tree_result;
    rtree_search_intersects(data.data(), data.size(), q, &tree_result);

    EXPECT_EQ(sorted(expected), sorted(tree_result));
}

} // namespace starrocks
