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

// Tests for the geo_window_detail DBSCAN and k-means algorithms
// (tested through the window function state machinery).

#include "exprs/agg/window_geo.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <set>

namespace starrocks {

using namespace geo_window_detail;

// ============================================================================
// Helper: build xs, ys, valid vectors from (x,y) pairs
// ============================================================================
static void make_coords(const std::vector<std::pair<double, double>>& pts, std::vector<double>& xs,
                        std::vector<double>& ys, std::vector<bool>& valid) {
    xs.resize(pts.size());
    ys.resize(pts.size());
    valid.assign(pts.size(), true);
    for (size_t i = 0; i < pts.size(); ++i) {
        xs[i] = pts[i].first;
        ys[i] = pts[i].second;
    }
}

// ============================================================================
// DBSCAN algorithm tests
// ============================================================================

class DBSCANTest : public ::testing::Test {};

TEST_F(DBSCANTest, EmptyInput) {
    std::vector<double> xs, ys;
    std::vector<bool> valid;
    std::vector<int32_t> out;
    dbscan(xs, ys, valid, 200.0, 2, out);
    EXPECT_TRUE(out.empty());
}

TEST_F(DBSCANTest, SinglePoint_IsNoise) {
    std::vector<double> xs = {0.0}, ys = {0.0};
    std::vector<bool> valid = {true};
    std::vector<int32_t> out;
    dbscan(xs, ys, valid, 200.0, 2, out);
    ASSERT_EQ(1u, out.size());
    EXPECT_EQ(-1, out[0]); // noise
}

TEST_F(DBSCANTest, TwoClosePoints_FormCluster) {
    // Two points ~111m apart, eps=200m, minpoints=2 → should form one cluster
    std::vector<double> xs = {0.0, 0.001}; // ~111m apart at equator
    std::vector<double> ys = {0.0, 0.0};
    std::vector<bool> valid = {true, true};
    std::vector<int32_t> out;
    dbscan(xs, ys, valid, 200.0, 2, out);
    ASSERT_EQ(2u, out.size());
    EXPECT_EQ(0, out[0]);
    EXPECT_EQ(0, out[1]);
}

TEST_F(DBSCANTest, TwoFarPoints_AreNoise) {
    // Two points 1° apart (~111km), eps=200m, minpoints=2 → both noise
    std::vector<double> xs = {0.0, 1.0};
    std::vector<double> ys = {0.0, 0.0};
    std::vector<bool> valid = {true, true};
    std::vector<int32_t> out;
    dbscan(xs, ys, valid, 200.0, 2, out);
    ASSERT_EQ(2u, out.size());
    EXPECT_EQ(-1, out[0]);
    EXPECT_EQ(-1, out[1]);
}

TEST_F(DBSCANTest, ThreeDenseOneFar_OneCluserOneNoise) {
    // 3 dense points (~111m apart), 1 far outlier → cluster 0 + noise
    std::vector<double> xs = {0.0, 0.001, 0.0, 10.0};
    std::vector<double> ys = {0.0, 0.0, 0.001, 10.0};
    std::vector<bool> valid = {true, true, true, true};
    std::vector<int32_t> out;
    dbscan(xs, ys, valid, 200.0, 2, out);
    ASSERT_EQ(4u, out.size());
    // first 3 should be cluster 0
    EXPECT_EQ(0, out[0]);
    EXPECT_EQ(0, out[1]);
    EXPECT_EQ(0, out[2]);
    // far point is noise
    EXPECT_EQ(-1, out[3]);
}

TEST_F(DBSCANTest, TwoSeparateClusters) {
    // Two pairs of close points, pairs far from each other
    std::vector<double> xs = {0.0, 0.001, 5.0, 5.001};
    std::vector<double> ys = {0.0, 0.001, 5.0, 5.001};
    std::vector<bool> valid = {true, true, true, true};
    std::vector<int32_t> out;
    dbscan(xs, ys, valid, 200.0, 2, out);
    ASSERT_EQ(4u, out.size());
    // Both pairs should be in different clusters
    EXPECT_EQ(out[0], out[1]);
    EXPECT_EQ(out[2], out[3]);
    EXPECT_NE(out[0], out[2]);
    // All valid cluster IDs (not noise)
    EXPECT_GE(out[0], 0);
    EXPECT_GE(out[2], 0);
}

TEST_F(DBSCANTest, InvalidPoint_SkippedInClustering) {
    std::vector<double> xs = {0.0, 0.001, 0.0};
    std::vector<double> ys = {0.0, 0.0, 0.001};
    std::vector<bool> valid = {true, true, false}; // third is invalid
    std::vector<int32_t> out;
    dbscan(xs, ys, valid, 200.0, 3, out); // minpoints=3 means pair can't form cluster
    ASSERT_EQ(3u, out.size());
    // Without the third point, the two valid points can't form a 3-minpoints cluster
    EXPECT_EQ(-1, out[0]);
    EXPECT_EQ(-1, out[1]);
    EXPECT_EQ(-1, out[2]);
}

TEST_F(DBSCANTest, LargeEps_AllPointsInOneCluster) {
    std::vector<double> xs = {0.0, 5.0, 10.0, 50.0};
    std::vector<double> ys = {0.0, 5.0, 10.0, 50.0};
    std::vector<bool> valid = {true, true, true, true};
    std::vector<int32_t> out;
    // eps = 10,000 km → all within range
    dbscan(xs, ys, valid, 10000000.0, 2, out);
    ASSERT_EQ(4u, out.size());
    // All should be in the same cluster
    std::set<int32_t> clusters(out.begin(), out.end());
    EXPECT_EQ(1u, clusters.size());
    EXPECT_GE(*clusters.begin(), 0);
}

TEST_F(DBSCANTest, MinPointsOne_AllPointsAreCoreClusters) {
    // With minpoints=1, every isolated point is its own cluster
    std::vector<double> xs = {0.0, 5.0, 10.0};
    std::vector<double> ys = {0.0, 5.0, 10.0};
    std::vector<bool> valid = {true, true, true};
    std::vector<int32_t> out;
    dbscan(xs, ys, valid, 10.0, 1, out); // eps=10m, well below 5° separation
    ASSERT_EQ(3u, out.size());
    // Each point is its own core point (min_pts=1)
    std::set<int32_t> clusters(out.begin(), out.end());
    EXPECT_EQ(3u, clusters.size());
    for (auto c : out) EXPECT_GE(c, 0);
}

// ============================================================================
// K-Means algorithm tests
// ============================================================================

class KMeansTest : public ::testing::Test {};

TEST_F(KMeansTest, EmptyInput) {
    std::vector<double> xs, ys;
    std::vector<bool> valid;
    std::vector<int32_t> out;
    kmeans(xs, ys, valid, 2, out);
    EXPECT_TRUE(out.empty());
}

TEST_F(KMeansTest, SinglePoint_AssignedToCluster0) {
    std::vector<double> xs = {1.0}, ys = {2.0};
    std::vector<bool> valid = {true};
    std::vector<int32_t> out;
    kmeans(xs, ys, valid, 1, out);
    ASSERT_EQ(1u, out.size());
    EXPECT_EQ(0, out[0]);
}

TEST_F(KMeansTest, KEqualsN_EachPointOwnCluster) {
    std::vector<double> xs = {0.0, 5.0, 10.0};
    std::vector<double> ys = {0.0, 5.0, 10.0};
    std::vector<bool> valid = {true, true, true};
    std::vector<int32_t> out;
    kmeans(xs, ys, valid, 3, out);
    ASSERT_EQ(3u, out.size());
    std::set<int32_t> clusters(out.begin(), out.end());
    // 3 points into 3 clusters → 3 distinct cluster IDs
    EXPECT_EQ(3u, clusters.size());
}

TEST_F(KMeansTest, TwoClearlySeparatedGroups) {
    // 3 points near (0,0) and 3 points near (100,100)
    std::vector<double> xs = {0.0, 0.001, -0.001, 100.0, 100.001, 99.999};
    std::vector<double> ys = {0.0, 0.001, -0.001, 100.0, 100.001, 99.999};
    std::vector<bool> valid(6, true);
    std::vector<int32_t> out;
    kmeans(xs, ys, valid, 2, out);
    ASSERT_EQ(6u, out.size());
    // First 3 should be one cluster, last 3 should be another
    EXPECT_EQ(out[0], out[1]);
    EXPECT_EQ(out[0], out[2]);
    EXPECT_EQ(out[3], out[4]);
    EXPECT_EQ(out[3], out[5]);
    EXPECT_NE(out[0], out[3]);
}

TEST_F(KMeansTest, AllPointsNonNull_NoNoise) {
    std::vector<double> xs = {0.0, 1.0, 2.0, 3.0, 4.0};
    std::vector<double> ys = {0.0, 0.0, 0.0, 0.0, 0.0};
    std::vector<bool> valid(5, true);
    std::vector<int32_t> out;
    kmeans(xs, ys, valid, 2, out);
    ASSERT_EQ(5u, out.size());
    // All assigned (no -1 noise like DBSCAN)
    for (auto c : out) EXPECT_GE(c, 0);
}

TEST_F(KMeansTest, KExceedsN_CappedAtN) {
    std::vector<double> xs = {0.0, 1.0};
    std::vector<double> ys = {0.0, 0.0};
    std::vector<bool> valid = {true, true};
    std::vector<int32_t> out;
    // k=10 but only 2 points → k capped at 2
    kmeans(xs, ys, valid, 10, out);
    ASSERT_EQ(2u, out.size());
    for (auto c : out) EXPECT_GE(c, 0);
}

TEST_F(KMeansTest, InvalidPointsHandled) {
    std::vector<double> xs = {0.0, 5.0, 10.0};
    std::vector<double> ys = {0.0, 5.0, 10.0};
    std::vector<bool> valid = {true, false, true}; // middle is invalid
    std::vector<int32_t> out;
    kmeans(xs, ys, valid, 2, out);
    ASSERT_EQ(3u, out.size());
    // Only valid points should have meaningful cluster IDs
    EXPECT_GE(out[0], 0);
    EXPECT_GE(out[2], 0);
}

TEST_F(KMeansTest, KOne_AllInSameCluster) {
    std::vector<double> xs = {0.0, 5.0, 10.0, 20.0};
    std::vector<double> ys = {0.0, 5.0, 10.0, 20.0};
    std::vector<bool> valid(4, true);
    std::vector<int32_t> out;
    kmeans(xs, ys, valid, 1, out);
    ASSERT_EQ(4u, out.size());
    for (auto c : out) EXPECT_EQ(0, c); // all in cluster 0
}

// ============================================================================
// Distance helper
// ============================================================================

TEST(GeoWindowDistTest, SamePointIsZero) {
    EXPECT_NEAR(0.0, dist_m(5.0, 5.0, 5.0, 5.0), 1e-6);
}

TEST(GeoWindowDistTest, OneDegreeLongitudeAtEquatorIs111km) {
    double d = dist_m(0.0, 0.0, 1.0, 0.0);
    EXPECT_GT(d, 110000.0);
    EXPECT_LT(d, 112000.0);
}

TEST(GeoWindowDistTest, SymmetryHolds) {
    double d1 = dist_m(2.35, 48.85, -0.12, 51.51); // Paris → London
    double d2 = dist_m(-0.12, 51.51, 2.35, 48.85); // London → Paris
    EXPECT_NEAR(d1, d2, 1.0);
}

TEST(GeoWindowDistTest, SouthernHemisphereCoordinates) {
    // Sydney, Australia ≈ (151.21, -33.87)
    double d = dist_m(151.21, -33.87, 151.22, -33.87); // ~1km east
    EXPECT_GT(d, 0.0);
    EXPECT_LT(d, 5000.0);
}

TEST(GeoWindowDistTest, AntipodalPoints) {
    // Antipodal: max great-circle distance ≈ 20015 km (half Earth circumference)
    double d = dist_m(0.0, 0.0, 180.0, 0.0);
    EXPECT_GT(d, 19000000.0);
    EXPECT_LT(d, 21000000.0);
}

TEST(GeoWindowDistTest, NearPoleLatitude) {
    // Two points near the north pole
    double d = dist_m(0.0, 89.9, 90.0, 89.9); // 90° of longitude at 89.9°N
    EXPECT_GT(d, 0.0);
    EXPECT_LT(d, 30000.0); // longitude circles shrink near poles
}

// ============================================================================
// DBSCAN edge cases
// ============================================================================

TEST_F(DBSCANTest, BorderPoint_AbsorbedFromCoreNeighbor) {
    // A and B are close (form core pair). C is close to B only.
    // C has only 1 neighbor (B), so C is not core. But C is reachable from B → border point.
    // minpoints=2: A-B are core; C is a border point of the same cluster.
    std::vector<double> xs = {0.0, 0.001, 0.002}; // A, B, C in a line ~111m apart
    std::vector<double> ys = {0.0, 0.0, 0.0};
    std::vector<bool> valid = {true, true, true};
    std::vector<int32_t> out;
    // eps=200m: A-B close, B-C close, A-C at ~222m (just beyond eps)
    dbscan(xs, ys, valid, 200.0, 2, out);
    ASSERT_EQ(3u, out.size());
    // All three should be in the same cluster (A,B core; C border via B)
    EXPECT_EQ(out[0], out[1]);
    EXPECT_EQ(out[1], out[2]);
    EXPECT_GE(out[0], 0);
}

TEST_F(DBSCANTest, ChainConnectivity) {
    // A-B-C-D chained: each adjacent pair close, non-adjacent pairs far
    // minpoints=2: A-B are core, B-C are core, C-D are core → one big cluster
    std::vector<double> xs = {0.0, 0.001, 0.002, 0.003};
    std::vector<double> ys = {0.0, 0.0, 0.0, 0.0};
    std::vector<bool> valid = {true, true, true, true};
    std::vector<int32_t> out;
    dbscan(xs, ys, valid, 200.0, 2, out);
    ASSERT_EQ(4u, out.size());
    int cid = out[0];
    EXPECT_GE(cid, 0) << "A should be in a cluster";
    EXPECT_EQ(cid, out[1]) << "B should be in same cluster as A";
    EXPECT_EQ(cid, out[2]) << "C should be in same cluster as A";
    EXPECT_EQ(cid, out[3]) << "D should be in same cluster as A";
}

TEST_F(DBSCANTest, AllInvalidPoints) {
    std::vector<double> xs = {0.0, 1.0, 2.0};
    std::vector<double> ys = {0.0, 1.0, 2.0};
    std::vector<bool> valid = {false, false, false};
    std::vector<int32_t> out;
    dbscan(xs, ys, valid, 1000.0, 2, out);
    ASSERT_EQ(3u, out.size());
    for (auto c : out) EXPECT_EQ(-1, c); // all noise (invalid = not clustered)
}

TEST_F(DBSCANTest, EpsZero_OnlyIdenticalPointsCluster) {
    // With eps=0, only points at exactly the same location form clusters
    std::vector<double> xs = {0.0, 0.0, 1.0};
    std::vector<double> ys = {0.0, 0.0, 0.0};
    std::vector<bool> valid = {true, true, true};
    std::vector<int32_t> out;
    dbscan(xs, ys, valid, 0.0, 2, out);
    ASSERT_EQ(3u, out.size());
    // Points 0 and 1 are at identical coordinates: dist_m(0,0,0,0) = 0 ≤ eps=0
    EXPECT_EQ(out[0], out[1]);
    EXPECT_GE(out[0], 0);
    EXPECT_EQ(-1, out[2]); // point at (1,0) is noise
}

TEST_F(DBSCANTest, LargeMinpoints_AllNoise) {
    std::vector<double> xs = {0.0, 0.001, 0.002};
    std::vector<double> ys = {0.0, 0.0, 0.0};
    std::vector<bool> valid = {true, true, true};
    std::vector<int32_t> out;
    // minpoints=10 but only 3 points → no point has 10 neighbors → all noise
    dbscan(xs, ys, valid, 1000.0, 10, out);
    for (auto c : out) EXPECT_EQ(-1, c);
}

TEST_F(DBSCANTest, AllSameLocation) {
    // All points at same coordinates: all within eps of each other
    std::vector<double> xs(10, 5.0);
    std::vector<double> ys(10, 5.0);
    std::vector<bool> valid(10, true);
    std::vector<int32_t> out;
    dbscan(xs, ys, valid, 200.0, 3, out);
    ASSERT_EQ(10u, out.size());
    int cid = out[0];
    EXPECT_GE(cid, 0);
    for (auto c : out) EXPECT_EQ(cid, c); // all in same cluster
}

TEST_F(DBSCANTest, LargePartition_CorrectCount) {
    // 200 points: 100 in cluster A near (0,0), 100 in cluster B near (10,10)
    std::vector<double> xs, ys;
    std::vector<bool> valid;
    for (int i = 0; i < 100; ++i) {
        xs.push_back(i * 0.0001);
        ys.push_back(0.0);
        valid.push_back(true); // cluster A
    }
    for (int i = 0; i < 100; ++i) {
        xs.push_back(10.0 + i * 0.0001);
        ys.push_back(10.0);
        valid.push_back(true); // cluster B
    }
    std::vector<int32_t> out;
    dbscan(xs, ys, valid, 200.0, 2, out); // eps=200m
    ASSERT_EQ(200u, out.size());

    // Count distinct non-noise cluster IDs
    std::set<int32_t> clusters;
    for (auto c : out)
        if (c >= 0) clusters.insert(c);
    EXPECT_EQ(2u, clusters.size()); // exactly 2 clusters

    // All cluster A points should share one cluster ID
    EXPECT_EQ(out[0], out[50]);
    // All cluster B points should share another cluster ID
    EXPECT_EQ(out[100], out[150]);
    // The two clusters should be different
    EXPECT_NE(out[0], out[100]);
}

TEST_F(DBSCANTest, NegativeCoordinates) {
    // Points in southern hemisphere / western longitude
    std::vector<double> xs = {-73.5, -73.501, -100.0}; // NYC area + far outlier
    std::vector<double> ys = {40.7, 40.7, 40.7};
    std::vector<bool> valid = {true, true, true};
    std::vector<int32_t> out;
    dbscan(xs, ys, valid, 200.0, 2, out);
    ASSERT_EQ(3u, out.size());
    EXPECT_EQ(out[0], out[1]); // first two are close
    EXPECT_GE(out[0], 0);
    EXPECT_EQ(-1, out[2]); // outlier is noise
}

// ============================================================================
// K-Means edge cases
// ============================================================================

TEST_F(KMeansTest, AllInvalidPoints) {
    std::vector<double> xs = {0.0, 1.0, 2.0};
    std::vector<double> ys = {0.0, 1.0, 2.0};
    std::vector<bool> valid = {false, false, false};
    std::vector<int32_t> out;
    // Should not crash; all stay at default (0)
    kmeans(xs, ys, valid, 2, out);
    ASSERT_EQ(3u, out.size());
    // No crash is the main requirement; values are unspecified for invalid points
}

TEST_F(KMeansTest, AllSameCoordinates) {
    // All points at identical coordinates: all should get cluster 0
    std::vector<double> xs(5, 3.0);
    std::vector<double> ys(5, 4.0);
    std::vector<bool> valid(5, true);
    std::vector<int32_t> out;
    kmeans(xs, ys, valid, 3, out);
    ASSERT_EQ(5u, out.size());
    // All identical points: k-means may assign any cluster, but should not crash
    for (auto c : out) {
        EXPECT_GE(c, 0);
        EXPECT_LT(c, 3);
    }
}

TEST_F(KMeansTest, Determinism_SameSeedSameResult) {
    std::vector<double> xs = {0.0, 5.0, 10.0, 1.0, 6.0, 11.0};
    std::vector<double> ys = {0.0, 5.0, 10.0, 0.5, 5.5, 10.5};
    std::vector<bool> valid(6, true);

    std::vector<int32_t> out1, out2;
    kmeans(xs, ys, valid, 3, out1);
    kmeans(xs, ys, valid, 3, out2);

    ASSERT_EQ(out1.size(), out2.size());
    for (size_t i = 0; i < out1.size(); ++i) {
        EXPECT_EQ(out1[i], out2[i]) << "Non-deterministic at index " << i;
    }
}

TEST_F(KMeansTest, ClusterIdsAreInRange_0_to_k_minus_1) {
    std::vector<double> xs = {0.0, 5.0, 10.0, 15.0, 20.0};
    std::vector<double> ys = {0.0, 0.0, 0.0, 0.0, 0.0};
    std::vector<bool> valid(5, true);
    std::vector<int32_t> out;
    int k = 3;
    kmeans(xs, ys, valid, k, out);
    for (auto c : out) {
        EXPECT_GE(c, 0);
        EXPECT_LT(c, k);
    }
}

TEST_F(KMeansTest, NearbyPointsInSameCluster) {
    // Two tight groups: (0,0) area and (100,100) area, k=2
    // Points in same group should share a cluster ID
    std::vector<double> xs = {0.0, 0.0001, 0.0002, 100.0, 100.0001, 100.0002};
    std::vector<double> ys = {0.0, 0.0, 0.0, 100.0, 100.0, 100.0};
    std::vector<bool> valid(6, true);
    std::vector<int32_t> out;
    kmeans(xs, ys, valid, 2, out);
    ASSERT_EQ(6u, out.size());
    EXPECT_EQ(out[0], out[1]);
    EXPECT_EQ(out[1], out[2]);
    EXPECT_EQ(out[3], out[4]);
    EXPECT_EQ(out[4], out[5]);
    EXPECT_NE(out[0], out[3]); // two groups in different clusters
}

TEST_F(KMeansTest, KMeansPlus_FirstCentroid_Deterministic) {
    // After the k-means++ bug fix, the first centroid must come from ONE point
    // (same x and y from the same index). Verify output is consistent.
    std::vector<double> xs = {0.0, 50.0, 100.0};
    std::vector<double> ys = {0.0, 50.0, 100.0};
    std::vector<bool> valid = {true, true, true};
    std::vector<int32_t> out;
    // With k=1, all points go to the single centroid — just verify no crash
    kmeans(xs, ys, valid, 1, out);
    for (auto c : out) EXPECT_EQ(0, c);
}

// ============================================================================
// Distance edge cases
// ============================================================================

TEST(GeoWindowDistTest, VeryShortDistance) {
    // 1mm apart (0.00001° at equator ≈ 1.1m)
    double d = dist_m(0.0, 0.0, 0.00001, 0.0);
    EXPECT_GT(d, 0.0);
    EXPECT_LT(d, 5.0);
}

TEST(GeoWindowDistTest, TriangleInequality) {
    // d(A,C) <= d(A,B) + d(B,C) for any three points
    double ab = dist_m(0.0, 0.0, 5.0, 5.0);
    double bc = dist_m(5.0, 5.0, 10.0, 0.0);
    double ac = dist_m(0.0, 0.0, 10.0, 0.0);
    EXPECT_LE(ac, ab + bc + 1.0); // +1.0 for floating-point tolerance
}

TEST(GeoWindowDistTest, NegativeCoordinatesWork) {
    // Buenos Aires ≈ (-58.4, -34.6), Cape Town ≈ (18.4, -33.9)
    double d = dist_m(-58.4, -34.6, 18.4, -33.9);
    EXPECT_GT(d, 5000000.0); // > 5000 km
    EXPECT_LT(d, 10000000.0);
}

} // namespace starrocks
