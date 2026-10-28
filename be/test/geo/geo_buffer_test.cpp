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

#include "geo/geo_buffer.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <limits>
#include <thread>

#include "geo/geo_measurements.h"

namespace starrocks {
namespace {
constexpr auto kCartesian = WkbCoordinateSemantics::GEOMETRY_CARTESIAN;
constexpr double kPi = 3.14159265358979323846;
constexpr const char* kSquare = "POLYGON ((0 0,10 0,10 10,0 10,0 0))";

StatusOr<std::unique_ptr<PreparedGeoBuffer>> prepared(std::string_view wkt) {
    WkbGeometry geometry;
    RETURN_IF_ERROR(WkbCodec::parse_wkt(wkt, &geometry, kCartesian));
    std::string wkb;
    RETURN_IF_ERROR(WkbCodec::to_wkb(geometry, &wkb, kCartesian));
    return PreparedGeoBuffer::prepare(Slice(wkb));
}

StatusOr<WkbGeometry> buffered(std::string_view wkt, double distance) {
    ASSIGN_OR_RETURN(auto input, prepared(wkt));
    return input->buffer(distance);
}

double area(const WkbGeometry& geometry) {
    auto result = planar_measurement(geometry, GeoMeasurementKind::AREA);
    EXPECT_TRUE(result.ok()) << result.status();
    return result.ok() ? *result : -1;
}

size_t count(const WkbGeometry& geometry) {
    size_t result = geometry.coordinates.size();
    for (const auto& ring : geometry.rings) result += ring.size();
    for (const auto& child : geometry.children) result += count(child);
    return result;
}

// Independent clamped Euclidean segment distance, used for line/corner references.
double segment_distance(WkbCoordinate p, WkbCoordinate a, WkbCoordinate b) {
    const double dx = b.x - a.x, dy = b.y - a.y;
    const double t = std::clamp(((p.x - a.x) * dx + (p.y - a.y) * dy) / (dx * dx + dy * dy), 0.0, 1.0);
    return std::hypot(p.x - (a.x + t * dx), p.y - (a.y + t * dy));
}

void expect_round_boundary(const WkbGeometry& output, const std::vector<WkbCoordinate>& input, double radius,
                           double simplification = 0) {
    ASSERT_EQ(WkbGeometryType::POLYGON, output.type);
    ASSERT_EQ(1, output.rings.size());
    auto distance = [&](WkbCoordinate p) {
        double result = std::numeric_limits<double>::infinity();
        for (size_t i = 1; i < input.size(); ++i) {
            result = std::min(result, segment_distance(p, input[i - 1], input[i]));
        }
        return result;
    };
    const auto& ring = output.rings.front();
    for (size_t i = 1; i < ring.size(); ++i) {
        EXPECT_NEAR(radius, distance(ring[i]), simplification + 1e-10);
        WkbCoordinate midpoint{(ring[i - 1].x + ring[i].x) / 2, (ring[i - 1].y + ring[i].y) / 2};
        EXPECT_GE(distance(midpoint), radius * std::cos(kPi / 32) - simplification - 1e-10);
        EXPECT_LE(distance(midpoint), radius + simplification + 1e-10);
    }
}
} // namespace

TEST(GeoBufferTest, PointCircleHasDocumentedApproximation) {
    auto result = buffered("POINT (7 -3)", 2);
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(WkbGeometryType::POLYGON, result->type);
    ASSERT_EQ(33, count(*result));
    EXPECT_NEAR(16 * 4 * std::sin(kPi / 16), area(*result), 1e-10);
    const auto& ring = result->rings.front();
    for (size_t i = 1; i < ring.size(); ++i) {
        EXPECT_NEAR(2, std::hypot(ring[i].x - 7, ring[i].y + 3), 1e-12);
        EXPECT_NEAR(2 * std::cos(kPi / 32),
                    std::hypot((ring[i].x + ring[i - 1].x) / 2 - 7, (ring[i].y + ring[i - 1].y) / 2 + 3), 1e-12);
    }
}

TEST(GeoBufferTest, LineCapsAndCornerJoinsAreRound) {
    auto line = buffered("LINESTRING (0 0,10 0)", 2);
    ASSERT_TRUE(line.ok()) << line.status();
    EXPECT_NEAR(40 + 16 * 4 * std::sin(kPi / 16), area(*line), 1e-10);
    expect_round_boundary(*line, {{0, 0}, {10, 0}}, 2);
    auto corner = buffered("LINESTRING (0 0,10 0,10 10)", 1);
    ASSERT_TRUE(corner.ok()) << corner.status();
    expect_round_boundary(*corner, {{0, 0}, {10, 0}, {10, 10}}, 1);
}

TEST(GeoBufferTest, SignedPolygonDistanceAndZeroAreDistinct) {
    auto expanded = buffered(kSquare, 1);
    auto eroded = buffered(kSquare, -1);
    auto zero = buffered(kSquare, 0);
    ASSERT_TRUE(expanded.ok()) << expanded.status();
    ASSERT_TRUE(eroded.ok()) << eroded.status();
    ASSERT_TRUE(zero.ok()) << zero.status();
    // Round joins may add a subdivision when an angle rounds across a step boundary.
    // Bound the area by a 32-gon and the exact circle instead of fixing the vertex count.
    EXPECT_GE(area(*expanded), 140 + 16 * std::sin(kPi / 16) - 1e-10);
    EXPECT_LE(area(*expanded), 140 + kPi + 1e-10);
    EXPECT_DOUBLE_EQ(64, area(*eroded));
    EXPECT_DOUBLE_EQ(100, area(*zero));
    expect_round_boundary(*expanded, {{0, 0}, {10, 0}, {10, 10}, {0, 10}, {0, 0}}, 1);
}

TEST(GeoBufferTest, RoundApproximationIncludesBoostInputSimplification) {
    auto result = buffered("POLYGON ((0 0,0.1 0,0.1 0.1,0 0.1,0 0))", 1000);
    ASSERT_TRUE(result.ok()) << result.status();
    // Boost 1.80 distance_symmetric simplifies input at abs(distance) / 1000.
    expect_round_boundary(*result, {{0, 0}, {0.1, 0}, {0.1, 0.1}, {0, 0.1}, {0, 0}}, 1000, 1);
}

TEST(GeoBufferTest, PolygonErosionCanBecomeEmpty) {
    for (double distance : {-5.0, -6.0, -1000.0}) {
        auto result = buffered(kSquare, distance);
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(WkbGeometryType::POLYGON, result->type);
        EXPECT_TRUE(result->empty);
    }
}

TEST(GeoBufferTest, HolesCanCloseAndErodedNecksCanSplit) {
    auto closed = buffered("POLYGON ((0 0,10 0,10 10,0 10,0 0),(4 4,4 6,6 6,6 4,4 4))", 1.1);
    ASSERT_TRUE(closed.ok()) << closed.status();
    EXPECT_EQ(1, closed->rings.size());
    auto split = buffered("POLYGON ((0 0,4 0,4 1,6 1,6 0,10 0,10 4,6 4,6 3,4 3,4 4,0 4,0 0))", -1.1);
    ASSERT_TRUE(split.ok()) << split.status();
    EXPECT_EQ(WkbGeometryType::MULTIPOLYGON, split->type);
    EXPECT_EQ(2, split->children.size());
}

TEST(GeoBufferTest, MultipleFamiliesMergeOrRemainSeparate) {
    for (const char* input : {"MULTIPOINT ((0 0),(1 0))", "MULTILINESTRING ((0 0,0 2),(1 0,1 2))",
                              "MULTIPOLYGON (((0 0,2 0,2 2,0 2,0 0)),((3 0,5 0,5 2,3 2,3 0)))"}) {
        auto result = buffered(input, 1);
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(WkbGeometryType::POLYGON, result->type);
    }
    for (const char* input : {"MULTIPOINT ((0 0),(10 0))", "MULTILINESTRING ((0 0,0 2),(10 0,10 2))",
                              "MULTIPOLYGON (((0 0,2 0,2 2,0 2,0 0)),((10 0,12 0,12 2,10 2,10 0)))"}) {
        auto result = buffered(input, 1);
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(WkbGeometryType::MULTIPOLYGON, result->type);
        EXPECT_EQ(2, result->children.size());
    }
}

TEST(GeoBufferTest, NonPositivePointAndLineDistanceReturnsPolygonEmpty) {
    for (const char* input :
         {"POINT (0 0)", "LINESTRING (0 0,2 0)", "MULTIPOINT ((0 0),(2 0))", "MULTILINESTRING ((0 0,2 0),(4 0,6 0))"}) {
        for (double distance : {0.0, -0.0, -1.0}) {
            auto result = buffered(input, distance);
            ASSERT_TRUE(result.ok()) << result.status();
            EXPECT_EQ(WkbGeometryType::POLYGON, result->type);
            EXPECT_TRUE(result->empty);
        }
    }
}

TEST(GeoBufferTest, TypedEmptyAndEmptyChildrenAreHandled) {
    for (const char* family : {"POINT", "LINESTRING", "POLYGON", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON"}) {
        for (double distance : {1.0, 0.0, -1.0}) {
            auto result = buffered(std::string(family) + " EMPTY", distance);
            ASSERT_TRUE(result.ok()) << result.status();
            EXPECT_EQ(WkbGeometryType::POLYGON, result->type);
            EXPECT_TRUE(result->empty);
        }
    }
    for (const char* input : {"MULTIPOINT (EMPTY,(0 0),EMPTY)", "MULTILINESTRING (EMPTY,(0 0,2 0),EMPTY)",
                              "MULTIPOLYGON (EMPTY,((0 0,2 0,2 2,0 2,0 0)),EMPTY)"}) {
        auto result = buffered(input, 1);
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ(WkbGeometryType::POLYGON, result->type);
    }
}

TEST(GeoBufferTest, ZeroCanonicalizesSinglePolygonMemberWithoutRepair) {
    auto result = buffered("MULTIPOLYGON (((0 0,2 0,2 2,0 2,0 0)))", 0);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(WkbGeometryType::POLYGON, result->type);
    EXPECT_DOUBLE_EQ(4, area(*result));
    EXPECT_EQ(5, count(*result));
}

TEST(GeoBufferTest, InvalidTopologyIsRejectedBeforeZeroOrErosion) {
    for (const char* input :
         {"POLYGON ((0 0,4 4,0 4,4 0,0 0))", "POLYGON ((0 0,4 0,4 4,0 4,0 0),(5 5,5 6,6 6,6 5,5 5))",
          "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((2 0,6 0,6 4,2 4,2 0)))", "LINESTRING (0 0,0 0)"}) {
        EXPECT_FALSE(prepared(input).ok()) << input;
    }
}

TEST(GeoBufferTest, CollectionAndNonFiniteDistancesAreControlledErrors) {
    EXPECT_FALSE(prepared("GEOMETRYCOLLECTION EMPTY").ok());
    for (const char* input : {kSquare, "POINT (0 0)", "POLYGON EMPTY"}) {
        auto model = prepared(input);
        ASSERT_TRUE(model.ok()) << model.status();
        for (double distance : {std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::infinity(),
                                -std::numeric_limits<double>::infinity()}) {
            EXPECT_FALSE((*model)->buffer(distance).ok());
        }
    }
}

TEST(GeoBufferTest, MalformedDimensionalAndExcessiveWkbAreRejected) {
    auto model = prepared(kSquare);
    ASSERT_TRUE(model.ok());
    EXPECT_FALSE(PreparedGeoBuffer::prepare(Slice("bad", 3)).ok());
    std::string dimensional("\x01\x01\x00\x00\x80", 5);
    dimensional.append(24, '\0');
    EXPECT_FALSE(PreparedGeoBuffer::prepare(Slice(dimensional)).ok());
    std::string excessive(256 * 1024 + 1, '\0');
    EXPECT_FALSE(PreparedGeoBuffer::prepare(Slice(excessive)).ok());
    WkbGeometry line;
    line.type = WkbGeometryType::LINESTRING;
    for (size_t i = 0; i <= kGeoBufferMaxInputCoordinates; ++i) line.coordinates.push_back({double(i), 0});
    std::string wkb;
    ASSERT_TRUE(WkbCodec::to_wkb(line, &wkb, kCartesian).ok());
    EXPECT_FALSE(PreparedGeoBuffer::prepare(Slice(wkb)).ok());
}

TEST(GeoBufferTest, OutputCoordinateLimitIsAnErrorWithoutTruncation) {
    WkbGeometry multi;
    multi.type = WkbGeometryType::MULTIPOINT;
    for (size_t i = 0; i < 310; ++i) {
        WkbGeometry point;
        point.type = WkbGeometryType::POINT;
        point.coordinates.push_back({double(i) * 10, 0});
        multi.children.emplace_back(std::move(point));
    }
    std::string wkb;
    ASSERT_TRUE(WkbCodec::to_wkb(multi, &wkb, kCartesian).ok());
    auto input = PreparedGeoBuffer::prepare(Slice(wkb));
    ASSERT_TRUE(input.ok()) << input.status();
    auto result = (*input)->buffer(1);
    ASSERT_FALSE(result.ok());
    EXPECT_NE(std::string::npos, result.status().to_string().find("result exceeds coordinate limit"));
}

TEST(GeoBufferTest, CartesianCoordinatesAreNotLongitudeLatitudeClamped) {
    auto result = buffered("POINT (37 55)", 1000);
    ASSERT_TRUE(result.ok()) << result.status();
    for (const auto& point : result->rings.front()) {
        EXPECT_NEAR(1000, std::hypot(point.x - 37, point.y - 55), 1e-9);
    }
}

TEST(GeoBufferTest, LargeOffsetsAndSubUlpDistancesHaveExplicitOutcomes) {
    auto result = buffered("POINT (1000000000000 -1000000000000)", 0.25);
    ASSERT_TRUE(result.ok()) << result.status();
    for (const auto& point : result->rings.front()) {
        // sqrt(2) times one binary64 ULP at 1e12 is below this bound.
        EXPECT_NEAR(0.25, std::hypot(point.x - 1e12, point.y + 1e12), 0.00018);
    }
    EXPECT_FALSE(buffered("POINT (1e20 0)", 0.25).ok());
    EXPECT_FALSE(prepared("MULTIPOINT ((1e20 0),(4 0))").ok());
    EXPECT_FALSE(buffered("POINT (1.7e308 0)", 1.7e308).ok());
}

TEST(GeoBufferTest, CheckpointsObserveCancellationAndMemoryErrors) {
    WkbGeometry geometry;
    ASSERT_TRUE(WkbCodec::parse_wkt(kSquare, &geometry, kCartesian).ok());
    std::string wkb;
    ASSERT_TRUE(WkbCodec::to_wkb(geometry, &wkb, kCartesian).ok());
    auto cancelled = PreparedGeoBuffer::prepare(Slice(wkb), [] { return Status::Cancelled("test"); });
    ASSERT_FALSE(cancelled.ok());
    EXPECT_TRUE(cancelled.status().is_cancelled());
    auto input = PreparedGeoBuffer::prepare(Slice(wkb));
    ASSERT_TRUE(input.ok());
    int checkpoints = 0;
    auto result = (*input)->buffer(
            1, [&] { return ++checkpoints == 2 ? Status::MemoryLimitExceeded("test") : Status::OK(); });
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_mem_limit_exceeded());
    EXPECT_EQ(2, checkpoints);
}

TEST(GeoBufferTest, OwnedPreparationReusesInputWithVaryingDistance) {
    auto input = prepared(kSquare);
    ASSERT_TRUE(input.ok()) << input.status();
    for (int batch = 0; batch < 2; ++batch) {
        for (double distance : {0.0, -1.0, 1.0, -6.0}) {
            auto result = (*input)->buffer(distance);
            ASSERT_TRUE(result.ok()) << result.status();
            if (distance == 0) {
                EXPECT_DOUBLE_EQ(100, area(*result));
            }
            if (distance == -1) {
                EXPECT_DOUBLE_EQ(64, area(*result));
            }
            if (distance == 1) {
                EXPECT_GT(area(*result), 140);
            }
            if (distance == -6) {
                EXPECT_TRUE(result->empty);
            }
        }
    }
}

TEST(GeoBufferTest, SharedPreparationHasNoMutableBufferScratch) {
    auto input = prepared(kSquare);
    ASSERT_TRUE(input.ok()) << input.status();
    std::atomic<int> failures{0};
    std::vector<std::thread> workers;
    for (int worker = 0; worker < 4; ++worker) {
        workers.emplace_back([&, worker] {
            for (int batch = 0; batch < 3; ++batch) {
                auto result = (*input)->buffer(worker % 2 == 0 ? -1 : 0);
                if (!result.ok()) {
                    ++failures;
                } else {
                    auto measured = planar_measurement(*result, GeoMeasurementKind::AREA);
                    if (!measured.ok() || *measured != (worker % 2 == 0 ? 64 : 100)) ++failures;
                }
            }
        });
    }
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(0, failures);
}

} // namespace starrocks
