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

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <thread>

#include "geo/geo_overlay.h"

namespace starrocks {
namespace {
constexpr GeoOverlayKind functions[] = {GeoOverlayKind::UNION, GeoOverlayKind::DIFFERENCE,
                                        GeoOverlayKind::SYMMETRIC_DIFFERENCE};
constexpr const char* square = "POLYGON ((0 0,4 0,4 4,0 4,0 0))";
constexpr const char* shifted = "POLYGON ((2 0,6 0,6 4,2 4,2 0))";
constexpr const char* empty = "POLYGON EMPTY";

StatusOr<std::unique_ptr<PreparedGeoPolygon>> prepare(const char* text) {
    WkbGeometry geometry;
    RETURN_IF_ERROR(WkbCodec::parse_wkt(text, &geometry, WkbCoordinateSemantics::GEOMETRY_CARTESIAN));
    std::string wkb;
    RETURN_IF_ERROR(WkbCodec::to_wkb(geometry, &wkb, WkbCoordinateSemantics::GEOMETRY_CARTESIAN));
    return PreparedGeoPolygon::prepare(Slice(wkb));
}
StatusOr<WkbGeometry> apply(GeoOverlayKind kind, const char* a, const char* b) {
    ASSIGN_OR_RETURN(auto left, prepare(a));
    ASSIGN_OR_RETURN(auto right, prepare(b));
    return left->overlay(*right, kind);
}
WkbGeometry run(GeoOverlayKind kind, const char* a, const char* b) {
    auto result = apply(kind, a, b);
    EXPECT_TRUE(result.ok()) << result.status();
    return result.ok() ? std::move(result.value()) : WkbGeometry{};
}
std::string multi(const char* polygon) {
    std::string value(polygon);
    return "MULTIPOLYGON (" + value.substr(value.find('(')) + ")";
}
std::vector<const WkbGeometry*> polygons(const WkbGeometry& geometry) {
    if (geometry.empty) return {};
    if (geometry.type == WkbGeometryType::POLYGON) return {&geometry};
    std::vector<const WkbGeometry*> result;
    for (const auto& child : geometry.children)
        if (!child.empty) result.push_back(&child);
    return result;
}

// Independent Euclidean boundary oracle: every vertex AND edge midpoint must
// lie on an expected segment, in both directions. Match components and holes
// individually so area/bbox equality cannot hide misplaced geometry.
double segment_distance(WkbCoordinate point, WkbCoordinate a, WkbCoordinate b) {
    const double dx = b.x - a.x, dy = b.y - a.y;
    const double length2 = dx * dx + dy * dy;
    const double t = length2 == 0 ? 0 : std::clamp(((point.x - a.x) * dx + (point.y - a.y) * dy) / length2, 0.0, 1.0);
    return std::hypot(point.x - a.x - t * dx, point.y - a.y - t * dy);
}
bool directed_ring_match(const std::vector<WkbCoordinate>& a, const std::vector<WkbCoordinate>& b, double tolerance) {
    if (a.size() < 4 || b.size() < 4 || !(a.front() == a.back()) || !(b.front() == b.back())) return false;
    for (size_t i = 1; i < a.size(); ++i) {
        for (const auto& point : {a[i], WkbCoordinate{(a[i - 1].x + a[i].x) / 2, (a[i - 1].y + a[i].y) / 2}}) {
            double distance = std::numeric_limits<double>::infinity();
            for (size_t j = 1; j < b.size(); ++j)
                distance = std::min(distance, segment_distance(point, b[j - 1], b[j]));
            if (distance > tolerance) return false;
        }
    }
    return true;
}
bool ring_match(const std::vector<WkbCoordinate>& a, const std::vector<WkbCoordinate>& b, double tolerance) {
    return directed_ring_match(a, b, tolerance) && directed_ring_match(b, a, tolerance);
}
bool polygon_match(const WkbGeometry& a, const WkbGeometry& b, double tolerance) {
    if (a.rings.size() != b.rings.size() || !ring_match(a.rings.front(), b.rings.front(), tolerance)) return false;
    std::vector<bool> matched(b.rings.size(), false);
    for (size_t i = 1; i < a.rings.size(); ++i) {
        bool found = false;
        for (size_t j = 1; j < b.rings.size(); ++j) {
            if (!matched[j] && ring_match(a.rings[i], b.rings[j], tolerance)) {
                matched[j] = found = true;
                break;
            }
        }
        if (!found) return false;
    }
    return true;
}
void expect_geometry(const WkbGeometry& output, const char* expected_wkt, double tolerance = 1e-12) {
    WkbGeometry expected;
    ASSERT_TRUE(WkbCodec::parse_wkt(expected_wkt, &expected, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
    const auto& actual = output;
    ASSERT_EQ(expected.type, actual.type);
    ASSERT_EQ(expected.empty, actual.empty);
    const auto a = polygons(actual), b = polygons(expected);
    ASSERT_EQ(a.size(), b.size());
    std::vector<bool> matched(b.size(), false);
    for (const auto* polygon : a) {
        bool found = false;
        for (size_t j = 0; j < b.size(); ++j) {
            if (!matched[j] && polygon_match(*polygon, *b[j], tolerance)) {
                matched[j] = found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "No matching expected component";
    }
    std::string wkb;
    ASSERT_TRUE(WkbCodec::to_wkb(output, &wkb, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
    EXPECT_TRUE(PreparedGeoPolygon::prepare(Slice(wkb)).ok());
}
} // namespace

TEST(GeoOverlayCoreTest, AnalyticOverlapAndAllFamilyPairs) {
    const char* expected[] = {"POLYGON ((0 0,6 0,6 4,0 4,0 0))", "POLYGON ((0 0,2 0,2 4,0 4,0 0))",
                              "MULTIPOLYGON (((0 0,2 0,2 4,0 4,0 0)),((4 0,6 0,6 4,4 4,4 0)))"};
    for (size_t op = 0; op < 3; ++op) {
        for (bool a_multi : {false, true})
            for (bool b_multi : {false, true}) {
                const auto a = a_multi ? multi(square) : square, b = b_multi ? multi(shifted) : shifted;
                expect_geometry(run(functions[op], a.c_str(), b.c_str()), expected[op]);
            }
    }
    expect_geometry(run(functions[1], shifted, square), "POLYGON ((4 0,6 0,6 4,4 4,4 0))");
    expect_geometry(run(functions[0], shifted, square), expected[0]);
    expect_geometry(run(functions[2], shifted, square), expected[2]);
}

TEST(GeoOverlayCoreTest, IdenticalDisjointTouchingAndEmpty) {
    const char* disjoint = "POLYGON ((8 0,9 0,9 1,8 1,8 0))";
    const char* combined = "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((8 0,9 0,9 1,8 1,8 0)))";
    expect_geometry(run(functions[0], square, square), square);
    expect_geometry(run(functions[1], square, square), empty);
    expect_geometry(run(functions[2], square, square), empty);
    for (auto fn : {functions[0], functions[2]}) expect_geometry(run(fn, square, disjoint), combined);
    expect_geometry(run(functions[1], square, disjoint), square);
    const char* adjacent = "POLYGON ((4 0,8 0,8 4,4 4,4 0))";
    for (auto fn : {functions[0], functions[2]}) {
        expect_geometry(run(fn, square, adjacent), "POLYGON ((0 0,8 0,8 4,0 4,0 0))");
    }
    expect_geometry(run(functions[1], square, adjacent), square);
    const char* point_touch = "POLYGON ((4 4,5 4,5 5,4 5,4 4))";
    expect_geometry(run(functions[0], square, point_touch),
                    "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((4 4,5 4,5 5,4 5,4 4)))");
    for (auto fn : functions) {
        expect_geometry(run(fn, square, empty), square);
        expect_geometry(run(fn, empty, empty), empty);
        expect_geometry(run(fn, empty, square), fn == functions[1] ? empty : square);
    }
}

TEST(GeoOverlayCoreTest, HolesAndTrueMultiComponents) {
    const char* inner = "POLYGON ((1 1,3 1,3 3,1 3,1 1))";
    const char* donut = "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,1 3,3 3,3 1,1 1))";
    for (auto fn : {functions[1], functions[2]}) expect_geometry(run(fn, square, inner), donut);
    expect_geometry(run(functions[1], inner, square), empty);
    expect_geometry(run(functions[0], donut, inner), square);
    expect_geometry(run(functions[1], donut, inner), donut);
    const char* multipart = "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((8 0,9 0,9 1,8 1,8 0)))";
    expect_geometry(run(functions[1], multipart, inner),
                    "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0),(1 1,1 3,3 3,3 1,1 1)),((8 0,9 0,9 1,8 1,8 0)))");
    expect_geometry(run(functions[1], square, "MULTIPOLYGON (((1 1,2 1,2 2,1 2,1 1)),((3 1,3.5 1,3.5 2,3 2,3 1)))"),
                    "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,1 2,2 2,2 1,1 1),(3 1,3 2,3.5 2,3.5 1,3 1))");
}

TEST(GeoOverlayCoreTest, PreservesNarrowGapsSliversAndLargeCoordinates) {
    // A gap much smaller than an integer rescale grid, but representable in double.
    expect_geometry(run(functions[0], "POLYGON ((0 0,1 0,1 1,0 1,0 0))",
                        "POLYGON ((1.0000000001 0,2 0,2 1,1.0000000001 1,1.0000000001 0))"),
                    "MULTIPOLYGON (((0 0,1 0,1 1,0 1,0 0)),"
                    "((1.0000000001 0,2 0,2 1,1.0000000001 1,1.0000000001 0)))",
                    1e-14);
    expect_geometry(run(functions[1], square, "POLYGON ((0.0000000001 0,4 0,4 4,0.0000000001 4,0.0000000001 0))"),
                    "POLYGON ((0 0,0.0000000001 0,0.0000000001 4,0 4,0 0))", 1e-14);
    const char* a =
            "POLYGON ((10000000 10000000,10000000.1 10000000,10000000.1 10000000.1,"
            "10000000 10000000.1,10000000 10000000))";
    const char* b =
            "POLYGON ((10000000.05 10000000,10000000.15 10000000,10000000.15 10000000.1,"
            "10000000.05 10000000.1,10000000.05 10000000))";
    expect_geometry(run(functions[0], empty, a), a, 2e-9);
    expect_geometry(run(functions[0], a, b),
                    "POLYGON ((10000000 10000000,10000000.15 10000000,10000000.15 10000000.1,"
                    "10000000 10000000.1,10000000 10000000))",
                    2e-9);
    expect_geometry(run(functions[1], a, b),
                    "POLYGON ((10000000 10000000,10000000.05 10000000,10000000.05 10000000.1,"
                    "10000000 10000000.1,10000000 10000000))",
                    2e-9);
}

TEST(GeoOverlayCoreTest, MultiComponentsAndTinyHoles) {
    const char* a = "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((8 0,9 0,9 1,8 1,8 0)))";
    const char* b = "MULTIPOLYGON (((2 0,6 0,6 4,2 4,2 0)),((10 0,11 0,11 1,10 1,10 0)))";
    expect_geometry(run(functions[0], a, b),
                    "MULTIPOLYGON (((0 0,6 0,6 4,0 4,0 0)),((8 0,9 0,9 1,8 1,8 0)),((10 0,11 0,11 1,10 1,10 0)))");
    expect_geometry(run(functions[2], a, b),
                    "MULTIPOLYGON (((0 0,2 0,2 4,0 4,0 0)),((4 0,6 0,6 4,4 4,4 0)),"
                    "((8 0,9 0,9 1,8 1,8 0)),((10 0,11 0,11 1,10 1,10 0)))");
    expect_geometry(run(functions[1], square, "POLYGON ((1 1,1.0000000001 1,1.0000000001 2,1 2,1 1))"),
                    "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,1 2,1.0000000001 2,1.0000000001 1,1 1))", 1e-14);
    expect_geometry(run(functions[0], "POLYGON ((0 0,1 0,1 1,0 1,0 0))", "POLYGON ((1 0,2 0,2 1.0000000001,1 1,1 0))"),
                    "POLYGON ((0 0,2 0,2 1.0000000001,1 1,0 1,0 0))", 1e-14);
}

TEST(GeoOverlayCoreTest, CoordinateAndPairWorkLimits) {
    WkbGeometry polygon;
    polygon.type = WkbGeometryType::POLYGON;
    auto& ring = polygon.rings.emplace_back();
    for (size_t i = 0; i < 4000; ++i) {
        const double angle = 2 * 3.14159265358979323846 * i / 4000;
        ring.push_back({std::cos(angle), std::sin(angle)});
    }
    ring.push_back(ring.front());
    std::string wkb;
    ASSERT_TRUE(WkbCodec::to_wkb(polygon, &wkb, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
    auto prepared = PreparedGeoPolygon::prepare(Slice(wkb));
    ASSERT_TRUE(prepared.ok()) << prepared.status();
    auto result = (*prepared)->overlay(**prepared, GeoOverlayKind::UNION);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_invalid_argument());
    std::string oversized(256 * 1024 + 1, 'x');
    EXPECT_FALSE(PreparedGeoPolygon::prepare(Slice(oversized)).ok());
}

TEST(GeoOverlayCoreTest, OutputCoordinateLimitDoesNotTruncate) {
    auto stripes = [](bool vertical) {
        WkbGeometry geometry;
        geometry.type = WkbGeometryType::MULTIPOLYGON;
        for (size_t i = 0; i < 40; ++i) {
            WkbGeometry polygon;
            polygon.type = WkbGeometryType::POLYGON;
            const double start = i;
            if (vertical)
                polygon.rings = {{{start, 0}, {start + 0.4, 0}, {start + 0.4, 40}, {start, 40}, {start, 0}}};
            else
                polygon.rings = {{{0, start}, {40, start}, {40, start + 0.4}, {0, start + 0.4}, {0, start}}};
            geometry.children.push_back(std::move(polygon));
        }
        std::string wkb;
        EXPECT_TRUE(WkbCodec::to_wkb(geometry, &wkb, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
        return PreparedGeoPolygon::prepare(Slice(wkb));
    };
    auto a = stripes(true), b = stripes(false);
    ASSERT_TRUE(a.ok()) << a.status();
    ASSERT_TRUE(b.ok()) << b.status();
    auto result = (*a)->overlay(**b, GeoOverlayKind::UNION);
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_invalid_argument());
    EXPECT_NE(std::string::npos, result.status().to_string().find("result exceeds coordinate limit"));
    auto symmetric = (*a)->overlay(**b, GeoOverlayKind::SYMMETRIC_DIFFERENCE);
    ASSERT_FALSE(symmetric.ok());
    EXPECT_NE(std::string::npos, symmetric.status().to_string().find("intermediate exceeds coordinate limit"));
}

TEST(GeoOverlayCoreTest, RejectsTopologyLossFromCommonOriginTranslation) {
    const char* remote =
            "POLYGON ((100000000000000000000 0,100000000000000100000 0,"
            "100000000000000100000 100000,100000000000000000000 100000,100000000000000000000 0))";
    const char* combined =
            "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),"
            "((100000000000000000000 0,100000000000000100000 0,100000000000000100000 100000,"
            "100000000000000000000 100000,100000000000000000000 0)))";
    // Both operands are valid independently; translating the small one by -1e20
    // collapses its X coordinates even in the extended-precision working model.
    expect_geometry(run(functions[0], remote, empty), remote);
    expect_geometry(run(functions[0], square, empty), square);
    for (size_t op = 0; op < 3; ++op) {
        auto result = apply(functions[op], remote, square);
        ASSERT_FALSE(result.ok());
        EXPECT_TRUE(result.status().is_invalid_argument());
        EXPECT_NE(std::string::npos, result.status().to_string().find("loses topology during coordinate translation"));
        // The reverse order does not collapse either model and preserves the
        // two disjoint components for union and symmetric difference.
        expect_geometry(run(functions[op], square, remote), op == 1 ? square : combined);
    }
}

TEST(GeoOverlayCoreTest, RejectsTranslatedCollapsedHolesAndMultiComponents) {
    const char* remote =
            "POLYGON ((100000000000000000000 0,100000000000000100000 0,"
            "100000000000000100000 100000,100000000000000000000 100000,100000000000000000000 0))";
    const char* with_hole = "POLYGON ((0 0,32 0,32 32,0 32,0 0),(1 1,1 3,3 3,3 1,1 1))";
    const char* multipart = "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((32 0,64 0,64 32,32 32,32 0)))";
    for (const char* value : {with_hole, multipart}) {
        expect_geometry(run(functions[0], value, empty), value);
        for (auto fn : functions) {
            auto result = apply(fn, remote, value);
            ASSERT_FALSE(result.ok());
            EXPECT_TRUE(result.status().is_invalid_argument());
            EXPECT_NE(std::string::npos,
                      result.status().to_string().find("loses topology during coordinate translation"));
        }
    }
}

TEST(GeoOverlayCoreTest, SymmetricDifferenceChecksCancellationBetweenPrimitives) {
    auto left = prepare(square);
    auto right = prepare(shifted);
    ASSERT_TRUE(left.ok());
    ASSERT_TRUE(right.ok());
    size_t checkpoints = 0;
    // Entry and post-translation checks precede the first difference.
    auto result = (*left)->overlay(**right, GeoOverlayKind::SYMMETRIC_DIFFERENCE, [&]() {
        return ++checkpoints == 3 ? Status::Cancelled("cancel after first difference") : Status::OK();
    });
    ASSERT_FALSE(result.ok());
    EXPECT_TRUE(result.status().is_cancelled());
    EXPECT_EQ(3, checkpoints);
}

TEST(GeoOverlayCoreTest, RejectsInvalidTopologyAndUnsupportedWkb) {
    const char* invalid[] = {"POLYGON ((0 0,4 4,0 4,4 0,0 0))",
                             "POLYGON ((0 0,4 0,4 4,0 4,0 0),(8 8,9 8,9 9,8 9,8 8))",
                             "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1),(1.5 1.5,2 1.5,2 2,1.5 2,1.5 1.5))",
                             "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((2 0,6 0,6 4,2 4,2 0)))",
                             "POINT EMPTY",
                             "LINESTRING (0 0,1 1)",
                             "GEOMETRYCOLLECTION EMPTY",
                             "POLYGON ((0 0,4 0,4 4,0 4))",
                             "POLYGON ((nan 0,4 0,4 4,0 4,nan 0))"};
    for (const auto* value : invalid) EXPECT_FALSE(prepare(value).ok()) << value;
    EXPECT_FALSE(PreparedGeoPolygon::prepare(Slice("not WKB")).ok());
}
TEST(GeoOverlayCoreTest, ImmutablePreparedModelsSupportConcurrentReaders) {
    auto a = prepare(square), b = prepare(shifted);
    ASSERT_TRUE(a.ok());
    ASSERT_TRUE(b.ok());
    std::vector<std::thread> workers;
    for (size_t i = 0; i < 4; ++i)
        workers.emplace_back([&]() {
            for (size_t repeat = 0; repeat < 20; ++repeat) {
                auto result = (*a)->overlay(**b, GeoOverlayKind::UNION);
                ASSERT_TRUE(result.ok()) << result.status();
                expect_geometry(*result, "POLYGON ((0 0,6 0,6 4,0 4,0 0))");
            }
        });
    for (auto& worker : workers) worker.join();
}
} // namespace starrocks
