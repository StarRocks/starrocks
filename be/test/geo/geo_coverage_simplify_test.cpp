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

#include "geo/geo_coverage_simplify.h"

#include <gtest/gtest.h>

#include <cmath>
#include <cstdlib>
#include <limits>
#include <thread>

#include "geo/wkb.h"

namespace starrocks {
namespace {
// Size-class allocator with explicit ownership and deterministic failure
// injection. It does not stand in for runtime/query memory tracker validation.
class TestAllocator final : public memory::Allocator {
public:
    size_t live = 0, peak = 0, allocations = 0, fail_after = std::numeric_limits<size_t>::max();
    void* alloc(size_t size, size_t = 0) override {
        if (allocations++ >= fail_after) return nullptr;
        size_t rounded = nallox(size);
        void* p = std::malloc(rounded);
        if (p) {
            live += rounded;
            peak = std::max(peak, live);
        }
        return p;
    }
    void free(void* pointer, size_t size) override {
        if (pointer) {
            live -= nallox(size);
            std::free(pointer);
        }
    }
    void* realloc(void*, size_t, size_t, size_t = 0) override { return nullptr; }
    int64_t nallox(size_t size, int = 0) const override { return (size + 63) / 64 * 64; }
    MemoryKind memory_kind() const override { return MemoryKind::kMalloc; }
};
constexpr auto kCartesian = WkbCoordinateSemantics::GEOMETRY_CARTESIAN;
std::string wkb(const std::string& text) {
    WkbGeometry geometry;
    auto status = WkbCodec::parse_wkt(text, &geometry, kCartesian);
    EXPECT_TRUE(status.ok()) << status;
    std::string result;
    status = WkbCodec::to_wkb(geometry, &result, kCartesian);
    EXPECT_TRUE(status.ok()) << status;
    return result;
}
WkbGeometry decoded(Slice bytes) {
    WkbGeometry geometry;
    auto status = WkbCodec::parse_wkb(bytes, &geometry, kCartesian);
    EXPECT_TRUE(status.ok()) << status;
    return geometry;
}
size_t positions(const WkbGeometry& geometry) {
    size_t count = geometry.coordinates.size();
    for (const auto& ring : geometry.rings) count += ring.size();
    for (const auto& child : geometry.children) count += positions(child);
    return count;
}
std::vector<std::string> adjacent() {
    return {wkb("POLYGON ((0 0,0 8,4 8,4.1 6,3.8 4,4.2 2,4 0,0 0))"),
            wkb("POLYGON ((4 0,4.2 2,3.8 4,4.1 6,4 8,8 8,8 0,4 0))")};
}
std::unique_ptr<GeoCoverageSimplify> make(TestAllocator& allocator, GeoCoverageLimits limits = {},
                                          GeoCoverageCheckpoint checkpoint = {}) {
    auto result = GeoCoverageSimplify::create(&allocator, limits, std::move(checkpoint));
    EXPECT_TRUE(result.ok()) << result.status();
    return result.ok() ? std::move(result).value() : nullptr;
}
void append(GeoCoverageSimplify& coverage, const std::vector<std::string>& rows) {
    for (const auto& row : rows) {
        auto status = coverage.append(Slice(row));
        ASSERT_TRUE(status.ok()) << status;
    }
}

TEST(GeoCoverageTest, JointSeamAndBoundaryFlag) {
    TestAllocator allocator;
    auto coverage = make(allocator);
    ASSERT_NE(coverage, nullptr);
    auto inputs = adjacent();
    append(*coverage, inputs);
    ASSERT_FALSE(coverage->result(0).ok());
    auto status = coverage->finish(1, false);
    ASSERT_TRUE(status.ok()) << status;
    auto first = coverage->result(0), second = coverage->result(1);
    ASSERT_TRUE(first.ok());
    ASSERT_TRUE(second.ok());
    auto a = decoded(first->value()), b = decoded(second->value());
    ASSERT_EQ(positions(a), 5);
    ASSERT_EQ(positions(b), 5);
    EXPECT_EQ(coverage->rows(), 2);
    EXPECT_EQ(coverage->kernel_calls(), 1);
    EXPECT_LE(coverage->peak_memory_usage(), 268435456);
    coverage.reset();
    EXPECT_EQ(allocator.live, 0);
}
TEST(GeoCoverageTest, DefaultMatchesTrue) {
    TestAllocator allocator;
    auto a = make(allocator), b = make(allocator);
    auto inputs = adjacent();
    append(*a, inputs);
    append(*b, inputs);
    ASSERT_TRUE(a->finish(1).ok());
    ASSERT_TRUE(b->finish(1, true).ok());
    for (size_t i = 0; i < inputs.size(); ++i)
        EXPECT_EQ(a->result(i)->value().to_string(), b->result(i)->value().to_string());
}
TEST(GeoCoverageTest, ZeroValidatesAndPreservesOriginalWkb) {
    TestAllocator allocator;
    auto c = make(allocator);
    auto inputs = adjacent();
    append(*c, inputs);
    ASSERT_TRUE(c->finish(0).ok());
    EXPECT_EQ(c->kernel_calls(), 0);
    for (size_t i = 0; i < inputs.size(); ++i) EXPECT_EQ(c->result(i)->value().to_string(), inputs[i]);
}
TEST(GeoCoverageTest, RepeatedClosingCoordinatesInShellsAndHoles) {
    const std::vector<std::pair<std::string, std::string>> cases{
            {"POLYGON ((0 0,0 2,2 2,2 0,0 0,0 0))", "POLYGON ((0 0,0 2,2 2,2 0,0 0))"},
            {"POLYGON ((0 0,0 0,0 2,0 2,2 2,2 0,0 0,0 0,0 0))", "POLYGON ((0 0,0 2,2 2,2 0,0 0))"},
            {"POLYGON ((0 0,0 8,8 8,8 0,0 0),(2 2,6 2,6 6,2 6,2 2,2 2,2 2))",
             "POLYGON ((0 0,0 8,8 8,8 0,0 0),(2 2,6 2,6 6,2 6,2 2))"},
            {"POLYGON ((0 0,0 0,0 8,8 8,8 0,0 0,0 0),(2 2,2 2,6 2,6 6,2 6,2 2,2 2))",
             "POLYGON ((0 0,0 8,8 8,8 0,0 0),(2 2,6 2,6 6,2 6,2 2))"},
            {"MULTIPOLYGON (((0 0,0 2,2 2,2 0,0 0,0 0)),((4 0,4 2,6 2,6 0,4 0,4 0)))",
             "MULTIPOLYGON (((0 0,0 2,2 2,2 0,0 0)),((4 0,4 2,6 2,6 0,4 0)))"}};
    for (const auto& [repeated_text, control_text] : cases) {
        SCOPED_TRACE(repeated_text);
        const auto repeated = wkb(repeated_text), control = wkb(control_text);
        ASSERT_GT(positions(decoded(Slice(repeated))), positions(decoded(Slice(control))));
        for (double tolerance : {0.0, 1.0}) {
            for (bool boundary : {false, true}) {
                TestAllocator allocator;
                auto c = make(allocator), expected = make(allocator);
                ASSERT_TRUE(c->append(Slice(repeated)).ok());
                ASSERT_TRUE(expected->append(Slice(control)).ok());
                auto status = c->finish(tolerance, boundary);
                ASSERT_TRUE(status.ok()) << status;
                ASSERT_TRUE(expected->finish(tolerance, boundary).ok());
                const auto output = c->result(0)->value().to_string();
                if (tolerance == 0)
                    EXPECT_EQ(output, repeated);
                else
                    EXPECT_EQ(output, expected->result(0)->value().to_string());
                c.reset();
                expected.reset();
                EXPECT_EQ(allocator.live, 0);
            }
        }
    }
}
TEST(GeoCoverageTest, RepeatedClosingCoordinatesOnSharedCoverageEdges) {
    const std::vector<std::string> repeated{wkb("POLYGON ((2 0,0 0,0 2,2 2,2 0,2 0,2 0))"),
                                            wkb("POLYGON ((2 0,2 2,4 2,4 0,2 0,2 0))")};
    const std::vector<std::string> control{wkb("POLYGON ((2 0,0 0,0 2,2 2,2 0))"),
                                           wkb("POLYGON ((2 0,2 2,4 2,4 0,2 0))")};
    for (double tolerance : {0.0, 1.0}) {
        for (bool boundary : {false, true}) {
            TestAllocator allocator;
            auto c = make(allocator), expected = make(allocator);
            append(*c, repeated);
            append(*expected, control);
            auto status = c->finish(tolerance, boundary);
            ASSERT_TRUE(status.ok()) << status;
            ASSERT_TRUE(expected->finish(tolerance, boundary).ok());
            ASSERT_EQ(c->rows(), repeated.size());
            for (size_t row = 0; row < repeated.size(); ++row) {
                EXPECT_EQ(c->result(row)->value().to_string(),
                          tolerance == 0 ? repeated[row] : expected->result(row)->value().to_string());
            }
        }
    }
}
TEST(GeoCoverageTest, RepeatedClosingCoordinatesStillCountTowardsAdmissionLimits) {
    const auto input = wkb("POLYGON ((0 0,0 2,2 2,2 0,0 0,0 0))");
    ASSERT_EQ(positions(decoded(Slice(input))), 6);
    TestAllocator allocator;
    GeoCoverageLimits limits;
    limits.vertices = 6;
    limits.input_bytes = input.size();
    auto exact = make(allocator, limits);
    ASSERT_TRUE(exact->append(Slice(input)).ok());
    auto status = exact->finish(0);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(exact->result(0)->value().to_string(), input);
    --limits.vertices;
    auto too_many = make(allocator, limits);
    EXPECT_FALSE(too_many->append(Slice(input)).ok());
    ++limits.vertices;
    --limits.input_bytes;
    auto too_large = make(allocator, limits);
    EXPECT_FALSE(too_large->append(Slice(input)).ok());
}
TEST(GeoCoverageTest, CircularDeduplicationDoesNotRepairInvalidRings) {
    for (const auto& text : {"POLYGON ((0 0,0 2,2 2,0 0,2 0,0 0,0 0))", "POLYGON ((0 0,0 0,0 0,0 0,0 0))",
                             "POLYGON ((0 0,0 2,0 0,0 0))"}) {
        SCOPED_TRACE(text);
        const auto input = wkb(text);
        for (double tolerance : {0.0, 1.0}) {
            TestAllocator allocator;
            auto c = make(allocator);
            ASSERT_TRUE(c->append(Slice(input)).ok());
            EXPECT_FALSE(c->finish(tolerance).ok());
            EXPECT_FALSE(c->result(0).ok());
        }
    }
}
TEST(GeoCoverageTest, RowMappingNullEmptyMultiAndIslands) {
    TestAllocator allocator;
    auto c = make(allocator);
    ASSERT_TRUE(c->append(std::nullopt).ok());
    auto empty = wkb("MULTIPOLYGON EMPTY"),
         multi = wkb("MULTIPOLYGON (((0 0,0 2,1 2,2 2,2 0,0 0)),((10 0,10 2,12 2,12 0,10 0)))");
    ASSERT_TRUE(c->append(Slice(empty)).ok());
    ASSERT_TRUE(c->append(Slice(multi)).ok());
    ASSERT_TRUE(c->finish(1).ok());
    EXPECT_FALSE(c->result(0)->has_value());
    auto e = decoded(c->result(1)->value()), m = decoded(c->result(2)->value());
    EXPECT_EQ(e.type, WkbGeometryType::MULTIPOLYGON);
    EXPECT_TRUE(e.empty);
    EXPECT_EQ(m.type, WkbGeometryType::MULTIPOLYGON);
    EXPECT_EQ(m.children.size(), 2);
}
TEST(GeoCoverageTest, NullParameterSkipsKernel) {
    for (bool null_tolerance : {false, true}) {
        TestAllocator allocator;
        auto c = make(allocator);
        append(*c, adjacent());
        ASSERT_TRUE(c->finish(null_tolerance ? std::optional<double>{} : 1,
                              null_tolerance ? std::optional<bool>{true} : std::optional<bool>{})
                            .ok());
        EXPECT_FALSE(c->result(0)->has_value());
        EXPECT_FALSE(c->result(1)->has_value());
        EXPECT_EQ(c->kernel_calls(), 0);
    }
}
TEST(GeoCoverageTest, InvalidParametersEvenEmpty) {
    for (double tolerance :
         {-1.0, std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN(), 1e200}) {
        TestAllocator allocator;
        auto c = make(allocator);
        auto empty = wkb("POLYGON EMPTY");
        ASSERT_TRUE(c->append(Slice(empty)).ok());
        EXPECT_FALSE(c->finish(tolerance).ok());
        EXPECT_FALSE(c->result(0).ok());
    }
}
TEST(GeoCoverageTest, InvalidCoverageNotBypassedByZero) {
    std::vector<std::vector<std::string>> invalid{
            {"POLYGON ((0 0,0 4,4 4,4 0,0 0))", "POLYGON ((0 0,0 4,4 4,4 0,0 0))"},
            {"POLYGON ((0 0,0 4,4 4,4 0,0 0))", "POLYGON ((2 1,2 3,6 3,6 1,2 1))"},
            {"POLYGON ((0 0,0 8,8 8,8 0,0 0))", "POLYGON ((2 2,2 3,3 3,3 2,2 2))"},
            {"POLYGON ((0 0,0 2,2 2,2 0,0 0))", "POLYGON ((2 0,2 1,2 2,4 2,4 0,2 0))"},
            {"POLYGON ((0 0,3 3,0 3,3 0,0 0))"},
            {"POLYGON ((0 0,0 4,4 4,4 0,0 0),(5 5,5 6,6 6,6 5,5 5))"}};
    for (const auto& text : invalid) {
        TestAllocator allocator;
        auto c = make(allocator);
        for (const auto& row : text) ASSERT_TRUE(c->append(Slice(wkb(row))).ok());
        EXPECT_FALSE(c->finish(0).ok()) << text[0];
        EXPECT_FALSE(c->result(0).ok());
    }
}
TEST(GeoCoverageTest, FreeBoundaryAndHoleFrozen) {
    TestAllocator allocator;
    auto c = make(allocator);
    auto input = wkb("POLYGON ((0 0,0 8,4 8,8 8,8 0,0 0),(2 2,3 2,4 2,4 4,2 4,2 2))");
    ASSERT_TRUE(c->append(Slice(input)).ok());
    ASSERT_TRUE(c->finish(1e6, false).ok());
    EXPECT_EQ(c->result(0)->value().to_string(), input);
}
TEST(GeoCoverageTest, JunctionAndOriginalPointContactRemain) {
    TestAllocator allocator;
    auto c = make(allocator);
    std::vector<std::string> inputs{wkb("POLYGON ((0 0,0 4,2 4,2 2,2 0,0 0))"), wkb("POLYGON ((2 2,2 4,4 4,4 2,2 2))"),
                                    wkb("POLYGON ((2 0,2 2,4 2,4 0,2 0))")};
    append(*c, inputs);
    ASSERT_TRUE(c->finish(1e6).ok());
    for (size_t i = 0; i < 3; ++i) {
        auto output = decoded(c->result(i)->value());
        bool found = false;
        for (const auto& point : output.rings[0]) found |= point.x == 2 && point.y == 2;
        EXPECT_TRUE(found);
    }
}
TEST(GeoCoverageTest, AdmissionLimitsBeforeKernel) {
    auto inputs = adjacent();
    for (int limit : {0, 1, 2}) {
        TestAllocator allocator;
        GeoCoverageLimits limits;
        if (limit == 0) limits.rows = 1;
        if (limit == 1) limits.vertices = 15;
        if (limit == 2) limits.input_bytes = inputs[0].size() + inputs[1].size() - 1;
        auto c = make(allocator, limits);
        ASSERT_TRUE(c->append(Slice(inputs[0])).ok());
        auto status = c->append(Slice(inputs[1]));
        ASSERT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find(limit == 0 ? "max_rows" : limit == 1 ? "max_vertices" : "max_input_bytes"),
                  std::string::npos);
        EXPECT_EQ(c->kernel_calls(), 0);
        EXPECT_FALSE(c->result(0).ok());
    }
    TestAllocator allocator;
    GeoCoverageLimits limits;
    limits.rows = 2;
    limits.vertices = 16;
    limits.input_bytes = inputs[0].size() + inputs[1].size();
    auto c = make(allocator, limits);
    append(*c, inputs);
    EXPECT_TRUE(c->finish(0).ok());
}
TEST(GeoCoverageTest, HardWorkingLimitAccountsTemporaryPeaks) {
    auto inputs = adjacent();
    TestAllocator first;
    auto reference = make(first);
    append(*reference, inputs);
    ASSERT_TRUE(reference->finish(1).ok());
    size_t peak = reference->peak_memory_usage();
    reference.reset();
    EXPECT_EQ(first.live, 0);
    for (size_t cap : {peak, peak - 1}) {
        TestAllocator allocator;
        GeoCoverageLimits limits;
        limits.working_bytes = cap;
        auto c = make(allocator, limits);
        ASSERT_NE(c, nullptr);
        append(*c, inputs);
        auto status = c->finish(1);
        EXPECT_EQ(status.ok(), cap == peak) << status;
        EXPECT_LE(c->peak_memory_usage(), cap);
        if (!status.ok()) {
            EXPECT_NE(status.to_string().find("geo_coverage_max_working_bytes"), std::string::npos);
        }
        c.reset();
        EXPECT_EQ(allocator.live, 0);
    }
}
TEST(GeoCoverageTest, AllocationFailureAndCancellationCleanup) {
    for (size_t fail_after : {0, 3, 10, 100}) {
        TestAllocator allocator;
        allocator.fail_after = fail_after;
        auto created = GeoCoverageSimplify::create(&allocator);
        if (!created.ok()) {
            EXPECT_EQ(allocator.live, 0);
            continue;
        }
        auto c = std::move(created).value();
        auto inputs = adjacent();
        Status status = Status::OK();
        for (const auto& input : inputs) {
            status = c->append(Slice(input));
            if (!status.ok()) break;
        }
        if (status.ok()) status = c->finish(1);
        if (!status.ok()) {
            EXPECT_FALSE(c->result(0).ok());
        }
        c.reset();
        EXPECT_EQ(allocator.live, 0);
    }
    TestAllocator allocator;
    bool cancelled = false;
    GeoCoverageCheckpoint checkpoint{
            &cancelled, [](void* context) {
                return *static_cast<bool*>(context) ? Status::Cancelled("coverage test cancellation") : Status::OK();
            }};
    auto c = make(allocator, {}, checkpoint);
    append(*c, adjacent());
    cancelled = true;
    EXPECT_TRUE(c->finish(1).is_cancelled());
    EXPECT_FALSE(c->result(0).ok());
    c.reset();
    EXPECT_EQ(allocator.live, 0);
}
TEST(GeoCoverageTest, SequentialWorkerMigrationAndWorkLimit) {
    TestAllocator allocator;
    auto c = make(allocator);
    append(*c, adjacent());
    Status status;
    std::thread worker([&] { status = c->finish(1); });
    worker.join();
    ASSERT_TRUE(status.ok()) << status;
    std::thread closer([&] { c.reset(); });
    closer.join();
    EXPECT_EQ(allocator.live, 0);
    GeoCoverageLimits limits;
    limits.work = 200;
    auto bounded = make(allocator, limits);
    append(*bounded, adjacent());
    EXPECT_FALSE(bounded->finish(1).ok());
    EXPECT_FALSE(bounded->result(0).ok());
}
TEST(GeoCoverageTest, UnsupportedMalformedAndUnclosedWkb) {
    for (const auto& value : {wkb("POINT (1 2)"), std::string("bad"), wkb("GEOMETRYCOLLECTION EMPTY")}) {
        TestAllocator allocator;
        auto c = make(allocator);
        EXPECT_FALSE(c->append(Slice(value)).ok());
        EXPECT_EQ(c->kernel_calls(), 0);
    }
    TestAllocator allocator;
    auto c = make(allocator);
    auto polygon = wkb("POLYGON ((0 0,0 2,2 2,2 0,0 0))");
    double last = 1;
    memcpy(polygon.data() + polygon.size() - 16, &last, 8);
    ASSERT_TRUE(c->append(Slice(polygon)).ok());
    EXPECT_FALSE(c->finish(0).ok());
}

TEST(GeoCoverageTest, NativeWkbCapabilityAndStructuralLimits) {
    auto polygon = wkb("POLYGON ((0 0,0 2,2 2,2 0,0 0))");
    for (uint32_t tag : {1003u, 2003u, 3003u, 0x20000003u}) {
        TestAllocator allocator;
        auto coverage = make(allocator);
        auto value = polygon;
        memcpy(value.data() + 1, &tag, sizeof(tag));
        EXPECT_FALSE(coverage->append(Slice(value)).ok());
        EXPECT_EQ(coverage->kernel_calls(), 0);
    }
    TestAllocator allocator;
    auto coverage = make(allocator);
    std::string declared = polygon.substr(0, 9);
    uint32_t count = 1000001;
    memcpy(declared.data() + 5, &count, sizeof(count));
    auto status = coverage->append(Slice(declared));
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("native WKB component limit"), std::string::npos);
    EXPECT_EQ(coverage->kernel_calls(), 0);
}

TEST(GeoCoverageTest, BigEndianZeroPreservesBytesAndPositiveEmitsNativeWkb) {
    auto little = wkb("POLYGON ((0 0,0 2,1 2,2 2,2 0,0 0))");
    auto big = little;
    big[0] = 0;
    for (size_t offset : {size_t{1}, size_t{5}, size_t{9}})
        std::reverse(big.begin() + offset, big.begin() + offset + 4);
    for (size_t offset = 13; offset < big.size(); offset += 8)
        std::reverse(big.begin() + offset, big.begin() + offset + 8);
    for (double tolerance : {0.0, 1.0}) {
        TestAllocator allocator;
        auto coverage = make(allocator);
        ASSERT_TRUE(coverage->append(Slice(big)).ok());
        ASSERT_TRUE(coverage->finish(tolerance).ok());
        auto result = coverage->result(0)->value();
        EXPECT_EQ(decoded(result).type, WkbGeometryType::POLYGON);
        if (tolerance == 0) {
            EXPECT_EQ(result.to_string(), big);
        } else {
            EXPECT_EQ(uint8_t(result.data[0]), 1);
            EXPECT_LT(positions(decoded(result)), 6);
        }
    }
}
TEST(GeoCoverageTest, ExactValidityAtExtremeScales) {
    for (double scale : {std::ldexp(1.0, -1070), 1e150, 1e300}) {
        TestAllocator allocator;
        auto c = make(allocator);
        for (const auto& input : adjacent()) {
            auto geometry = decoded(Slice(input));
            for (auto& ring : geometry.rings)
                for (auto& point : ring) {
                    point.x *= scale;
                    point.y *= scale;
                }
            std::string value;
            ASSERT_TRUE(WkbCodec::to_wkb(geometry, &value, kCartesian).ok());
            ASSERT_TRUE(c->append(Slice(value)).ok());
        }
        auto status = c->finish(0);
        EXPECT_TRUE(status.ok()) << scale << " " << status;
    }
}
TEST(GeoCoverageTest, GeneratedGridKeepsSharedJunctionsAndReducesEdges) {
    TestAllocator allocator;
    auto c = make(allocator);
    size_t before = 0;
    for (int y = 0; y < 3; ++y)
        for (int x = 0; x < 3; ++x) {
            WkbGeometry geometry;
            geometry.type = WkbGeometryType::POLYGON;
            geometry.rings.push_back({{double(x), double(y)},
                                      {double(x), y + 0.5},
                                      {double(x), y + 1.0},
                                      {x + 0.5, y + 1.0},
                                      {x + 1.0, y + 1.0},
                                      {x + 1.0, y + 0.5},
                                      {x + 1.0, double(y)},
                                      {x + 0.5, double(y)},
                                      {double(x), double(y)}});
            before += positions(geometry);
            std::string value;
            ASSERT_TRUE(WkbCodec::to_wkb(geometry, &value, kCartesian).ok());
            ASSERT_TRUE(c->append(Slice(value)).ok());
        }
    ASSERT_TRUE(c->finish(1, false).ok());
    size_t after = 0;
    for (size_t row = 0; row < 9; ++row) {
        auto geometry = decoded(c->result(row)->value());
        after += positions(geometry);
        EXPECT_EQ(geometry.type, WkbGeometryType::POLYGON);
        EXPECT_EQ(geometry.rings.size(), 1);
    }
    EXPECT_LT(after, before);
    EXPECT_EQ(c->kernel_calls(), 1);
}
TEST(GeoCoverageTest, ConcurrentPartitionsOwnAllocatorsAndMapping) {
    auto inputs = adjacent();
    auto run = [&](bool reverse) {
        TestAllocator allocator;
        auto c = make(allocator);
        auto data = inputs;
        if (reverse) std::reverse(data.begin(), data.end());
        append(*c, data);
        auto status = c->finish(1, false);
        ASSERT_TRUE(status.ok()) << status;
        auto output = decoded(c->result(0)->value());
        double xmin = std::numeric_limits<double>::infinity();
        for (const auto& point : output.rings[0]) xmin = std::min(xmin, point.x);
        EXPECT_EQ(xmin, reverse ? 4 : 0);
        c.reset();
        EXPECT_EQ(allocator.live, 0);
    };
    std::thread first([&] { run(false); }), second([&] { run(true); });
    first.join();
    second.join();
}
TEST(GeoCoverageTest, ContiguousOutputAndResourceFollowPartitionOwner) {
    TestAllocator allocator;
    auto c = make(allocator);
    ASSERT_TRUE(c->append(std::nullopt).ok());
    append(*c, adjacent());
    ASSERT_TRUE(c->finish(1, false).ok());
    ASSERT_FALSE(c->result(0)->has_value());
    const auto first = c->result(1)->value(), second = c->result(2)->value();
    const auto buffer = c->result_buffer().value();
    EXPECT_EQ(first.data, buffer.data);
    EXPECT_EQ(first.data + first.size, second.data);
    EXPECT_EQ(first.size + second.size, buffer.size);
    const auto retained = c->memory_usage();
    void* pointer = c->resource()->allocate(127, alignof(std::max_align_t));
    EXPECT_GT(c->memory_usage(), retained);
    std::thread downstream([&] { c->resource()->deallocate(pointer, 127, alignof(std::max_align_t)); });
    downstream.join();
    EXPECT_EQ(c->memory_usage(), retained);
    c.reset();
    EXPECT_EQ(allocator.live, 0);
}
} // namespace
} // namespace starrocks
