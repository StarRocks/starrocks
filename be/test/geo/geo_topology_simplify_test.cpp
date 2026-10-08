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

#include "geo/geo_topology_simplify.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <cmath>
#include <limits>
#include <thread>

#define BOOST_MATH_DISABLE_FLOAT128
#define BOOST_CSTDFLOAT_NO_LIBQUADMATH_SUPPORT
#include <boost/geometry.hpp>

#include "geo/geo_planar.h"

namespace starrocks {
namespace {
constexpr auto kCartesian = WkbCoordinateSemantics::GEOMETRY_CARTESIAN;
constexpr const char* kSquare = "POLYGON ((0 0,1 0,2 0,2 1,2 2,1 2,0 2,0 1,0 0))";

StatusOr<std::string> encoded(std::string_view wkt) {
    WkbGeometry geometry;
    RETURN_IF_ERROR(WkbCodec::parse_wkt(wkt, &geometry, kCartesian));
    std::string wkb;
    RETURN_IF_ERROR(WkbCodec::to_wkb(geometry, &wkb, kCartesian));
    return wkb;
}
StatusOr<std::unique_ptr<PreparedGeoTopologySimplify>> prepared(std::string_view wkt,
                                                                size_t work = kGeoTopologySimplifyMaxWork) {
    ASSIGN_OR_RETURN(auto wkb, encoded(wkt));
    return PreparedGeoTopologySimplify::prepare(Slice(wkb), {}, work);
}
StatusOr<WkbGeometry> simplified(std::string_view wkt, double tolerance) {
    ASSIGN_OR_RETURN(auto input, prepared(wkt));
    ASSIGN_OR_RETURN(auto wkb, input->simplify(tolerance));
    WkbGeometry output;
    RETURN_IF_ERROR(WkbCodec::parse_wkb(Slice(wkb), &output, kCartesian));
    return output;
}
size_t count(const WkbGeometry& geometry) {
    size_t total = geometry.coordinates.size();
    for (const auto& ring : geometry.rings) total += ring.size();
    for (const auto& child : geometry.children) total += count(child);
    return total;
}
bool has(const std::vector<WkbCoordinate>& points, WkbCoordinate p) {
    return std::find(points.begin(), points.end(), p) != points.end();
}
long double distance(WkbCoordinate point, WkbCoordinate a, WkbCoordinate b) {
    const long double x = static_cast<long double>(b.x) - a.x, y = static_cast<long double>(b.y) - a.y;
    const long double px = static_cast<long double>(point.x) - a.x, py = static_cast<long double>(point.y) - a.y;
    const long double t = std::clamp((px * x + py * y) / (x * x + y * y), 0.0L, 1.0L);
    return std::hypot(px - t * x, py - t * y);
}
// An independent original-subchain certificate; tests do not call the kernel's
// distance predicate or merely inspect its number of output vertices.
void expect_chain_bound(const std::vector<WkbCoordinate>& input, const std::vector<WkbCoordinate>& output,
                        double tolerance) {
    ASSERT_EQ(input.front(), output.front());
    ASSERT_EQ(input.back(), output.back());
    size_t start = 0;
    for (size_t i = 1; i < output.size(); ++i) {
        auto it = std::find(input.begin() + start + 1, input.end(), output[i]);
        ASSERT_NE(it, input.end());
        const auto end = static_cast<size_t>(it - input.begin());
        for (size_t j = start; j <= end; ++j) EXPECT_LE(distance(input[j], output[i - 1], output[i]), tolerance);
        start = end;
    }
    EXPECT_EQ(input.size() - 1, start);
}
StatusOr<std::unique_ptr<PreparedGeoTopologySimplify>> from_tree(const WkbGeometry& input) {
    std::string wkb;
    RETURN_IF_ERROR(WkbCodec::to_wkb(input, &wkb, kCartesian));
    return PreparedGeoTopologySimplify::prepare(Slice(wkb));
}
} // namespace

TEST(GeoTopologySimplifyTest, SafeReductionPreservesSourceVerticesAndWinding) {
    for (const char* wkt : {kSquare, "POLYGON ((0 0,0 1,0 2,1 2,2 2,2 1,2 0,1 0,0 0))"}) {
        WkbGeometry source;
        ASSERT_TRUE(WkbCodec::parse_wkt(wkt, &source, kCartesian).ok());
        auto output = simplified(wkt, 0.01);
        ASSERT_TRUE(output.ok()) << output.status();
        EXPECT_EQ(WkbGeometryType::POLYGON, output->type);
        EXPECT_EQ(5, count(*output));
        expect_chain_bound(source.rings[0], output->rings[0], 0.01);
        EXPECT_GT(signed_ring_area2(source.rings[0]) * signed_ring_area2(output->rings[0]), 0);
    }
}

TEST(GeoTopologySimplifyTest, OriginalSubchainsPreventAccumulatedError) {
    WkbGeometry source;
    source.type = WkbGeometryType::LINESTRING;
    for (size_t i = 0; i <= 64; ++i) {
        const double x = i / 64.0;
        source.coordinates.push_back({x, 4 * x * (1 - x)});
    }
    auto model = from_tree(source);
    ASSERT_TRUE(model.ok()) << model.status();
    auto result = model.value()->simplify(0.08);
    ASSERT_TRUE(result.ok()) << result.status();
    WkbGeometry output;
    ASSERT_TRUE(WkbCodec::parse_wkb(Slice(*result), &output, kCartesian).ok());
    EXPECT_LT(count(output), count(source));
    EXPECT_GT(count(output), 2);
    expect_chain_bound(source.coordinates, output.coordinates, 0.08);
}

TEST(GeoTopologySimplifyTest, DistanceBoundaryHasNoEpsilonInflation) {
    WkbGeometry source;
    source.type = WkbGeometryType::LINESTRING;
    for (double height : {0.125, std::ldexp(1.0, -1000), std::numeric_limits<double>::denorm_min()}) {
        source.coordinates = {{0, 0}, {1, height}, {2, 0}};
        auto model = from_tree(source);
        ASSERT_TRUE(model.ok()) << model.status();
        auto equal = model.value()->simplify(height);
        auto smaller = model.value()->simplify(std::nextafter(height, 0));
        ASSERT_TRUE(equal.ok()) << equal.status();
        ASSERT_TRUE(smaller.ok()) << smaller.status();
        WkbGeometry a, b;
        ASSERT_TRUE(WkbCodec::parse_wkb(Slice(*equal), &a, kCartesian).ok());
        ASSERT_TRUE(WkbCodec::parse_wkb(Slice(*smaller), &b, kCartesian).ok());
        EXPECT_EQ(2, count(a));
        EXPECT_EQ(3, count(b));
    }
}

TEST(GeoTopologySimplifyTest, NarrowHoleAndSmallMultiMemberCannotDisappear) {
    auto hole = simplified(
            "POLYGON ((0 0,10 0,10 10,0 10,0 0),"
            "(4.9 4.9,4.9 5.1,5.1 5.1,5.1 4.9,4.9 4.9))",
            0.3);
    ASSERT_TRUE(hole.ok()) << hole.status();
    ASSERT_EQ(2, hole->rings.size());
    EXPECT_GE(hole->rings[1].size(), 4);
    EXPECT_TRUE(boost::geometry::is_valid(to_planar_polygon<PlanarPolygon>(*hole)));
    auto multi = simplified(
            "MULTIPOLYGON (((0 0,10 0,10 10,0 10,0 0)),"
            "((20 0,20.1 0,20.1 0.1,20 0.1,20 0)))",
            0.3);
    ASSERT_TRUE(multi.ok()) << multi.status();
    EXPECT_EQ(WkbGeometryType::MULTIPOLYGON, multi->type);
    ASSERT_EQ(2, multi->children.size());
    EXPECT_GE(multi->children[1].rings[0].size(), 4);
}

TEST(GeoTopologySimplifyTest, HoleOwnerAndIslandStaySeparate) {
    auto output = simplified(
            "MULTIPOLYGON (((0 0,10 0,10 10,0 10,0 0),"
            "(4 4,4 6,6 6,6 4,4 4)),"
            "((4.5 4.5,5.5 4.5,5.5 5.5,4.5 5.5,4.5 4.5)))",
            10);
    ASSERT_TRUE(output.ok()) << output.status();
    ASSERT_EQ(2, output->children.size());
    EXPECT_EQ(2, output->children[0].rings.size());
    EXPECT_EQ(1, output->children[1].rings.size());
}

TEST(GeoTopologySimplifyTest, OriginalPolygonAndLineContactsRemainAnchored) {
    auto polygons = simplified(
            "MULTIPOLYGON (((0 0.1,1 0,2 0.1,2 2,0 2,0 0.1)),"
            "((1 0,0.8 -1,1.2 -1,1 0)))",
            0.2);
    ASSERT_TRUE(polygons.ok()) << polygons.status();
    ASSERT_EQ(2, polygons->children.size());
    EXPECT_TRUE(has(polygons->children[0].rings[0], {1, 0}));
    EXPECT_TRUE(has(polygons->children[1].rings[0], {1, 0}));
    auto lines = simplified("MULTILINESTRING ((0 0,1 2,2 0,3 2),(-1 1,4 1))", 2);
    ASSERT_TRUE(lines.ok()) << lines.status();
    ASSERT_EQ(2, lines->children.size());
    EXPECT_EQ(4, lines->children[0].coordinates.size()); // All three crossings, not just DE-9IM.
    EXPECT_EQ(2, lines->children[1].coordinates.size());
}

TEST(GeoTopologySimplifyTest, UnsafeShortcutCannotCreateSelfIntersection) {
    auto output = simplified("LINESTRING (0 0,0 2,2 2,2 -1,-1 -1)", 2.2);
    ASSERT_TRUE(output.ok()) << output.status();
    EXPECT_GT(count(*output), 3);
    PlanarLine line;
    for (const auto p : output->coordinates) line.emplace_back(p.x, p.y);
    EXPECT_TRUE(boost::geometry::is_simple(line));
    EXPECT_EQ((WkbCoordinate{0, 0}), output->coordinates.front());
    EXPECT_EQ((WkbCoordinate{-1, -1}), output->coordinates.back());
}

TEST(GeoTopologySimplifyTest, SweptRegionCannotMoveBoundaryPastInteriorPoint) {
    auto output = simplified("GEOMETRYCOLLECTION (POLYGON ((0 0.1,1 0,2 0.1,2 2,0 2,0 0.1)),POINT (1 0.05))", 0.2);
    ASSERT_TRUE(output.ok()) << output.status();
    ASSERT_EQ(2, output->children.size());
    EXPECT_TRUE(has(output->children[0].rings[0], {1, 0}));
    EXPECT_TRUE(boost::geometry::within(PlanarPoint(1, 0.05), to_planar_polygon<PlanarPolygon>(output->children[0])));
}

TEST(GeoTopologySimplifyTest, NestedFamilyTreeAndEmptyChildrenArePreserved) {
    auto output = simplified(
            "GEOMETRYCOLLECTION (POINT EMPTY,"
            "GEOMETRYCOLLECTION (MULTIPOLYGON EMPTY,LINESTRING (0 0,1 0,2 0)),"
            "MULTIPOINT (EMPTY,(7 8),(7 8)),POLYGON EMPTY)",
            1);
    ASSERT_TRUE(output.ok()) << output.status();
    ASSERT_EQ(4, output->children.size());
    EXPECT_EQ(WkbGeometryType::GEOMETRYCOLLECTION, output->type);
    EXPECT_TRUE(output->children[0].empty);
    ASSERT_EQ(2, output->children[1].children.size());
    EXPECT_EQ(WkbGeometryType::MULTIPOLYGON, output->children[1].children[0].type);
    EXPECT_TRUE(output->children[1].children[0].empty);
    EXPECT_EQ(2, count(output->children[1].children[1]));
    ASSERT_EQ(3, output->children[2].children.size());
    EXPECT_TRUE(output->children[2].children[0].empty);
    EXPECT_EQ(output->children[2].children[1].coordinates, output->children[2].children[2].coordinates);
    EXPECT_EQ(WkbGeometryType::POLYGON, output->children[3].type);
    EXPECT_TRUE(output->children[3].empty);
}

TEST(GeoTopologySimplifyTest, SingleMemberMultiRemainsMultiAndClosedLinesRetainEndpoints) {
    auto multi = simplified("MULTIPOLYGON (((0 0,1 0,2 0,2 2,0 2,0 0)))", 0.1);
    ASSERT_TRUE(multi.ok()) << multi.status();
    EXPECT_EQ(WkbGeometryType::MULTIPOLYGON, multi->type);
    EXPECT_EQ(1, multi->children.size());
    auto line = simplified("LINESTRING (0 0,1 0,2 0,2 2,0 2,0 0)", 0.1);
    ASSERT_TRUE(line.ok()) << line.status();
    EXPECT_EQ(line->coordinates.front(), line->coordinates.back());
    EXPECT_EQ((WkbCoordinate{0, 0}), line->coordinates.front());
}

TEST(GeoTopologySimplifyTest, ValidTouchingHolesAreAcceptedButDisconnectedInteriorIsRejected) {
    const char* one = "POLYGON ((0 0,10 0,10 10,0 10,0 0),(0 5,2 4,2 6,0 5))";
    const char* triple =
            "POLYGON ((0 0,10 0,10 10,0 10,0 0),"
            "(0 5,2 3,3 4,0 5),(0 5,3 6,2 7,0 5))";
    for (const char* wkt : {one, triple}) {
        auto output = simplified(wkt, 0.1);
        ASSERT_TRUE(output.ok()) << output.status();
        EXPECT_TRUE(boost::geometry::is_valid(to_planar_polygon<PlanarPolygon>(*output)));
    }
    EXPECT_FALSE(prepared("POLYGON ((0 0,10 0,10 10,0 10,0 0),(0 5,5 2,10 5,5 8,0 5))").ok());
}

TEST(GeoTopologySimplifyTest, InvalidTopologyIsRejectedEvenBeforeZeroShortcut) {
    for (const char* wkt : {"POLYGON ((0 0,2 2,0 2,2 0,0 0))", "POLYGON ((0 0,1 0,2 0,0 0))",
                            "POLYGON ((0 0,10 0,10 10,0 10,0 0),(20 20,21 20,21 21,20 20))",
                            "POLYGON ((0 0,10 0,10 10,0 10,0 0),(2 2,8 2,8 8,2 8,2 2),(3 3,4 3,4 4,3 3))",
                            "MULTIPOLYGON (((0 0,3 0,3 3,0 3,0 0)),((1 1,2 1,2 2,1 2,1 1)))", "LINESTRING (1 1,1 1)"}) {
        EXPECT_FALSE(prepared(wkt).ok()) << wkt;
    }
}

TEST(GeoTopologySimplifyTest, RepeatedAdjacentVerticesAndExistingLineCrossingsAreValid) {
    auto polygon = simplified("POLYGON ((0 0,1 0,1 0,1 1,0 1,0 0))", 0.01);
    ASSERT_TRUE(polygon.ok()) << polygon.status();
    auto line = simplified("LINESTRING (0 0,2 2,0 2,2 0)", 10);
    ASSERT_TRUE(line.ok()) << line.status();
    EXPECT_EQ(4, count(*line)); // Existing intersection segments remain unchanged.
}

TEST(GeoTopologySimplifyTest, ExtremeScalesDoNotTranslateOrCollapseSmallComponents) {
    auto output = simplified(
            "MULTIPOLYGON (((100000000000000000000 0,100000000000000032768 0,"
            "100000000000000032768 32768,100000000000000000000 32768,100000000000000000000 0)),"
            "((0 0,4 0,4 4,0 4,0 0)))",
            1);
    ASSERT_TRUE(output.ok()) << output.status();
    ASSERT_EQ(2, output->children.size());
    EXPECT_TRUE(has(output->children[1].rings[0], {4, 0}));
    WkbGeometry source;
    source.type = WkbGeometryType::LINESTRING;
    source.coordinates = {{-std::numeric_limits<double>::max(), 0}, {0, 0}, {std::numeric_limits<double>::max(), 0}};
    auto model = from_tree(source);
    ASSERT_TRUE(model.ok()) << model.status();
    auto reduced = model.value()->simplify(std::numeric_limits<double>::denorm_min());
    ASSERT_TRUE(reduced.ok()) << reduced.status();
    WkbGeometry decoded;
    ASSERT_TRUE(WkbCodec::parse_wkb(Slice(*reduced), &decoded, kCartesian).ok());
    EXPECT_EQ(2, count(decoded));
}

TEST(GeoTopologySimplifyTest, SubnormalPolygonCannotCollapseDimension) {
    const double d = std::numeric_limits<double>::denorm_min();
    WkbGeometry source;
    source.type = WkbGeometryType::POLYGON;
    source.rings = {{{0, 0}, {2 * d, 0}, {2 * d, 2 * d}, {0, 2 * d}, {0, 0}}};
    auto model = from_tree(source);
    ASSERT_TRUE(model.ok()) << model.status();
    auto result = model.value()->simplify(1);
    ASSERT_TRUE(result.ok()) << result.status();
    WkbGeometry output;
    ASSERT_TRUE(WkbCodec::parse_wkb(Slice(*result), &output, kCartesian).ok());
    EXPECT_EQ(4, count(output));
    EXPECT_NE(output.rings[0][1], output.rings[0][2]);
}

TEST(GeoTopologySimplifyTest, ExtremeBoxesSurviveRtreeSplits) {
    WkbGeometry source;
    source.type = WkbGeometryType::MULTIPOINT;
    for (int i = 0; i < 64; ++i) {
        WkbGeometry point;
        point.type = WkbGeometryType::POINT;
        point.coordinates = {{std::numeric_limits<double>::max() * (i % 2 ? 1 : -1), double(i)}};
        source.children.push_back(point);
    }
    auto model = from_tree(source);
    ASSERT_TRUE(model.ok()) << model.status();
    auto result = model.value()->simplify(1);
    ASSERT_TRUE(result.ok()) << result.status();
    WkbGeometry output;
    ASSERT_TRUE(WkbCodec::parse_wkb(Slice(*result), &output, kCartesian).ok());
    ASSERT_EQ(source.children.size(), output.children.size());
    for (size_t i = 0; i < output.children.size(); ++i)
        EXPECT_EQ(source.children[i].coordinates, output.children[i].coordinates);
}

TEST(GeoTopologySimplifyTest, ZeroPreservesOriginalBigEndianBytes) {
    std::string wkb(1, '\0');
    auto append = [&](uint64_t bits, size_t bytes) {
        for (size_t i = bytes; i > 0; --i) wkb.push_back(static_cast<char>(bits >> ((i - 1) * 8)));
    };
    append(2, 4);
    append(3, 4);
    for (double v : {-0.0, 0.0, 1.0, 0.0, 2.0, 0.0}) append(std::bit_cast<uint64_t>(v), 8);
    auto model = PreparedGeoTopologySimplify::prepare(Slice(wkb));
    ASSERT_TRUE(model.ok()) << model.status();
    auto result = model.value()->simplify(-0.0);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(wkb, *result);
}

TEST(GeoTopologySimplifyTest, ToleranceValidationAlsoAppliesToEmpty) {
    for (const char* wkt : {"POLYGON EMPTY", kSquare, "POINT (1 2)"}) {
        auto model = prepared(wkt);
        ASSERT_TRUE(model.ok()) << model.status();
        for (double t : {-1.0, std::numeric_limits<double>::quiet_NaN(), std::numeric_limits<double>::infinity()}) {
            EXPECT_FALSE(model.value()->simplify(t).ok());
        }
        EXPECT_TRUE(model.value()->simplify(0).ok());
    }
}

TEST(GeoTopologySimplifyTest, MalformedAndDimensionalWkbAreRejected) {
    auto source = encoded(kSquare);
    ASSERT_TRUE(source.ok());
    std::string truncated = source->substr(0, source->size() - 1);
    EXPECT_FALSE(PreparedGeoTopologySimplify::prepare(Slice(truncated)).ok());
    std::string dimensional = *source;
    dimensional[1] = static_cast<char>(1003 & 255);
    dimensional[2] = static_cast<char>(1003 >> 8);
    EXPECT_FALSE(PreparedGeoTopologySimplify::prepare(Slice(dimensional)).ok());
    EXPECT_FALSE(PreparedGeoTopologySimplify::prepare(Slice(std::string(300 * 1024, 'x'))).ok());
}

TEST(GeoTopologySimplifyTest, CoordinateComponentAndWorkLimitsAreControlled) {
    WkbGeometry line;
    line.type = WkbGeometryType::LINESTRING;
    for (size_t i = 0; i <= kGeoTopologySimplifyMaxCoordinates; ++i) line.coordinates.push_back({double(i), 0});
    EXPECT_FALSE(from_tree(line).ok());
    WkbGeometry collection;
    collection.type = WkbGeometryType::GEOMETRYCOLLECTION;
    WkbGeometry empty;
    empty.type = WkbGeometryType::POINT;
    empty.empty = true;
    collection.children.assign(kGeoTopologySimplifyMaxComponents, empty);
    EXPECT_FALSE(from_tree(collection).ok());
    EXPECT_FALSE(prepared(kSquare, 1).ok());
    auto limited = prepared(kSquare, 350);
    ASSERT_TRUE(limited.ok()) << limited.status();
    EXPECT_FALSE(limited.value()->simplify(0.01).ok());
}

TEST(GeoTopologySimplifyTest, LowerReaderLimitKeepsDefaultBehaviorAndBoundsDeclaredChildren) {
    auto source = encoded(kSquare);
    ASSERT_TRUE(source.ok());
    WkbGeometry output;
    EXPECT_TRUE(WkbCodec::parse_wkb(Slice(*source), &output, kCartesian).ok());
    EXPECT_FALSE(WkbCodec::parse_wkb_bounded(Slice(*source), &output, 5, kCartesian).ok());
    EXPECT_TRUE(WkbCodec::parse_wkb_bounded(Slice(*source), &output, 11, kCartesian).ok());
    auto malicious = encoded("GEOMETRYCOLLECTION (POINT EMPTY)");
    ASSERT_TRUE(malicious.ok());
    (*malicious)[5] = static_cast<char>(0xff);
    EXPECT_FALSE(WkbCodec::parse_wkb_bounded(Slice(*malicious), &output, 10, kCartesian).ok());
}

TEST(GeoTopologySimplifyTest, CheckpointPreservesCancellationAndMemoryErrors) {
    auto source = encoded(kSquare);
    ASSERT_TRUE(source.ok());
    auto cancelled =
            PreparedGeoTopologySimplify::prepare(Slice(*source), [] { return Status::Cancelled("cancel probe"); });
    EXPECT_FALSE(cancelled.ok());
    EXPECT_TRUE(cancelled.status().is_cancelled());
    auto model = prepared(kSquare);
    ASSERT_TRUE(model.ok());
    auto memory = model.value()->simplify(1, [] { return Status::MemoryLimitExceeded("memory probe"); });
    EXPECT_TRUE(memory.status().is_mem_limit_exceeded());
    int checks = 0;
    auto interrupted = model.value()->simplify(
            1, [&] { return ++checks == 3 ? Status::Cancelled("middle probe") : Status::OK(); });
    EXPECT_TRUE(interrupted.status().is_cancelled());
}

TEST(GeoTopologySimplifyTest, ImmutablePreparedInputSupportsConcurrentVaryingCalls) {
    auto model = prepared(kSquare);
    ASSERT_TRUE(model.ok());
    auto source = encoded(kSquare);
    ASSERT_TRUE(source.ok());
    std::atomic<size_t> passed = 0;
    std::vector<std::thread> workers;
    for (size_t i = 0; i < 8; ++i) {
        workers.emplace_back([&, i] {
            auto result = model.value()->simplify(i % 2 ? 0 : 0.01);
            if (!result.ok()) return;
            WkbGeometry geometry;
            if (!WkbCodec::parse_wkb(Slice(*result), &geometry, kCartesian).ok()) return;
            if (count(geometry) == (i % 2 ? 9 : 5)) ++passed;
        });
    }
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(8, passed.load());
    EXPECT_EQ(*source, *model.value()->simplify(0));
}

} // namespace starrocks
