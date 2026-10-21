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
std::unique_ptr<GeoCoverageSimplify> make(TestAllocator& allocator, GeoCoverageCheckpoint checkpoint = {}) {
    auto result = GeoCoverageSimplify::create(&allocator, std::move(checkpoint));
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
    EXPECT_GT(coverage->peak_memory_usage(), 0);
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
TEST(GeoCoverageTest, MoreThanTenThousandRowsRetainMapping) {
    TestAllocator allocator;
    auto coverage = make(allocator);
    const auto empty = wkb("POLYGON EMPTY");
    const auto polygon = wkb("POLYGON ((0 0,0 2,2 2,2 0,0 0))");
    constexpr size_t count = 10001;
    for (size_t i = 0; i < count; ++i) {
        const auto status = coverage->append(i == count - 1 ? Slice(polygon) : Slice(empty));
        ASSERT_TRUE(status.ok()) << status;
    }
    ASSERT_TRUE(coverage->finish(0).ok());
    ASSERT_EQ(coverage->rows(), count);
    for (size_t i = 0; i < count; ++i) {
        const auto result = coverage->result(i);
        ASSERT_TRUE(result.ok()) << result.status();
        ASSERT_TRUE(result->has_value());
        EXPECT_EQ(result->value().to_string(), i == count - 1 ? polygon : empty);
    }
    coverage.reset();
    EXPECT_EQ(allocator.live, 0);
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
    auto c = make(allocator, checkpoint);
    append(*c, adjacent());
    cancelled = true;
    EXPECT_TRUE(c->finish(1).is_cancelled());
    EXPECT_FALSE(c->result(0).ok());
    c.reset();
    EXPECT_EQ(allocator.live, 0);
}
TEST(GeoCoverageTest, KernelPollingRetainsCancellationAndMemoryErrors) {
    WkbGeometry polygon;
    polygon.type = WkbGeometryType::POLYGON;
    auto& ring = polygon.rings.emplace_back();
    // A zero-area ring would fail in decode before the next explicit check.
    for (int i = 0; i < 512; ++i) ring.push_back({double(i), 0});
    ring.push_back({0, 0});
    std::string input;
    ASSERT_TRUE(WkbCodec::to_wkb(polygon, &input, kCartesian).ok());
    for (bool memory_error : {false, true}) {
        struct State {
            bool armed = false;
            int checks = 0;
            bool memory_error;
        } state{false, 0, memory_error};
        GeoCoverageCheckpoint checkpoint{
                &state, [](void* context) {
                    auto& state = *static_cast<State*>(context);
                    // Entry and row checks precede the coordinate-loop poll.
                    if (state.armed && ++state.checks == 3) {
                        return state.memory_error ? Status::MemoryLimitExceeded("coverage test memory limit")
                                                  : Status::Cancelled("coverage test cancellation");
                    }
                    return Status::OK();
                }};
        TestAllocator allocator;
        auto coverage = make(allocator, checkpoint);
        ASSERT_NE(coverage, nullptr);
        ASSERT_TRUE(coverage->append(Slice(input)).ok());
        state.armed = true;
        const auto status = coverage->finish(1, false);
        EXPECT_EQ(state.checks, 3);
        EXPECT_EQ(status.is_mem_limit_exceeded(), memory_error) << status;
        EXPECT_EQ(status.is_cancelled(), !memory_error) << status;
        EXPECT_FALSE(coverage->result(0).ok());
        coverage.reset();
        EXPECT_EQ(allocator.live, 0);
    }
}
TEST(GeoCoverageTest, SequentialWorkerMigration) {
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
// All fixtures below are polygons; encode expected bytes with the same native codec.
std::string transformed_wkb(const std::string& text, double scale, double offset) {
    auto geometry = decoded(Slice(wkb(text)));
    for (auto& ring : geometry.rings)
        for (auto& point : ring) {
            point.x = offset + scale * point.x;
            point.y = offset + scale * point.y;
        }
    std::string result;
    EXPECT_TRUE(WkbCodec::to_wkb(geometry, &result, kCartesian).ok());
    return result;
}
TEST(GeoCoverageTest, ExactThresholdAndWholePartitionArithmetic) {
    struct Case {
        double x, tolerance;
        bool removed;
    };
    const Case cases[] = {{6, 1, true},
                          {std::nextafter(6.0, 7.0), 1, false},
                          {std::nextafter(6.0, 5.0), 1, true},
                          {6, std::nextafter(1.0, 0.0), false},
                          {6, std::nextafter(1.0, 2.0), true},
                          {5.9375, std::nextafter(1.0, 0.0), true}};
    const std::vector<std::string> source = {wkb("POLYGON ((0 0,0 1,4 1,6 0.5,4 0,0 0))"),
                                             wkb("POLYGON ((4 0,6 0.5,4 1,8 1,8 0,4 0))")};
    const std::vector<std::string> simplified = {wkb("POLYGON ((0 0,0 1,4 1,4 0,0 0))"),
                                                 wkb("POLYGON ((4 0,4 1,8 1,8 0,4 0))")};
    auto triangle = decoded(Slice(wkb("POLYGON ((20 0,24 0,24 1,20 0))")));
    triangle.rings[0][2].y = std::numeric_limits<double>::denorm_min();
    std::string subnormal;
    ASSERT_TRUE(WkbCodec::to_wkb(triangle, &subnormal, kCartesian).ok());
    for (const auto& test : cases)
        for (bool fallback : {false, true})
            for (int form = 0; form < 3; ++form) {
                SCOPED_TRACE(::testing::Message() << "x=" << std::hexfloat << test.x << " tolerance=" << test.tolerance
                                                  << " fallback=" << fallback << " form=" << form);
                TestAllocator allocator;
                auto c = make(allocator);
                std::vector<std::string> inputs;
                for (const auto& row : source) {
                    auto geometry = decoded(Slice(row));
                    for (auto& point : geometry.rings[0])
                        if (point.x == 6) point.x = test.x;
                    std::string input;
                    ASSERT_TRUE(WkbCodec::to_wkb(geometry, &input, kCartesian).ok());
                    inputs.push_back(std::move(input));
                }
                append(*c, inputs);
                // A last-admitted tiny coordinate forces the entire partition
                // onto the original path, including its earlier normal rows.
                if (fallback) {
                    ASSERT_TRUE(c->append(Slice(subnormal)).ok());
                }
                const auto status = form == 2 ? c->finish(test.tolerance) : c->finish(test.tolerance, form == 1);
                ASSERT_TRUE(status.ok()) << status;
                for (size_t i = 0; i < inputs.size(); ++i)
                    EXPECT_EQ(c->result(i)->value().to_string(), test.removed ? simplified[i] : inputs[i]);
                if (fallback) {
                    EXPECT_EQ(c->result(2)->value().to_string(), subnormal);
                }
                c.reset();
                EXPECT_EQ(allocator.live, 0);
            }
}
TEST(GeoCoverageTest, ExactArithmeticAtCapacityBoundary) {
    // The unit triangle prevents cancellation of the large square's exponent.
    // 2^123 coordinates fit the partition proof; 2^124 and 2^1023 use fallback.
    const auto unit = wkb("POLYGON ((-2 0,-1 0,-1 1,-2 0))");
    for (int power : {123, 124, 1023})
        for (double tolerance : {0.0, 1.0})
            for (int form = 0; form < 3; ++form) {
                SCOPED_TRACE(::testing::Message()
                             << "power=" << power << " tolerance=" << tolerance << " form=" << form);
                TestAllocator allocator;
                auto c = make(allocator);
                const auto square = transformed_wkb("POLYGON ((0 0,0 1,1 1,1 0,0 0))", std::ldexp(1.0, power), 0);
                ASSERT_TRUE(c->append(Slice(square)).ok());
                ASSERT_TRUE(c->append(Slice(unit)).ok());
                const auto status = form == 2 ? c->finish(tolerance) : c->finish(tolerance, form == 1);
                ASSERT_TRUE(status.ok()) << status;
                EXPECT_EQ(c->result(0)->value().to_string(), square);
                EXPECT_EQ(c->result(1)->value().to_string(), unit);
                c.reset();
                EXPECT_EQ(allocator.live, 0);
            }
}
TEST(GeoCoverageTest, ExactMidpointRangesPreserveBoundaryAndHoleClassification) {
    const std::vector<std::vector<std::string>> cases{
            {"POLYGON ((0 0,8 0,8 8,0 8,0 0))", "POLYGON ((8 0,16 0,16 8,8 8,8 0))"},
            {"POLYGON ((0 8,8 16,16 8,8 0,0 8))", "POLYGON ((16 8,20 12,24 8,20 4,16 8))"},
            {"POLYGON ((0 0,16 0,16 16,0 16,0 0),(4 8,8 12,12 8,8 4,4 8))", "POLYGON ((7 7,10 7,8 10,7 7))"},
            {"POLYGON ((0 0,16 0,16 16,0 16,0 8,0 0),(0 8,4 12,8 8,4 4,0 8))"}};
    const std::pair<double, double> transforms[] = {
            {1, 0}, {1, 1e6}, {1, 2e7}, {1, -2e7}, {std::numeric_limits<double>::denorm_min(), 0}, {1e150, 0}};
    for (const auto& [scale, offset] : transforms)
        for (const auto& text : cases)
            for (double tolerance : {0.0, 0.25 * scale})
                for (bool boundary : {false, true}) {
                    SCOPED_TRACE(::testing::Message() << "scale=" << scale << " offset=" << offset << " tolerance="
                                                      << tolerance << " boundary=" << boundary << " input=" << text[0]);
                    TestAllocator allocator;
                    auto c = make(allocator);
                    std::vector<std::string> inputs;
                    for (const auto& row : text) inputs.push_back(transformed_wkb(row, scale, offset));
                    append(*c, inputs);
                    ASSERT_TRUE(c->finish(tolerance, boundary).ok());
                    for (size_t i = 0; i < inputs.size(); ++i) EXPECT_EQ(c->result(i)->value().to_string(), inputs[i]);
                }
}
TEST(GeoCoverageTest, ExactMidpointRangesKeepNearlyCollinearSharedEdges) {
    for (double offset : {0.0, 1e6, 2e7}) {
        TestAllocator allocator;
        auto c = make(allocator);
        for (const auto& input : adjacent()) {
            auto geometry = decoded(Slice(input));
            for (auto& ring : geometry.rings)
                for (auto& point : ring) {
                    const double x = point.x;
                    point.x += offset;
                    point.y += offset;
                    if (3 < x && x < 5 && x != 4) point.x = std::nextafter(offset + 4, offset + (x < 4 ? 3 : 5));
                }
            std::string value;
            ASSERT_TRUE(WkbCodec::to_wkb(geometry, &value, kCartesian).ok());
            ASSERT_TRUE(c->append(Slice(value)).ok());
        }
        ASSERT_TRUE(c->finish(0.25, false).ok());
        EXPECT_EQ(c->result(0)->value().to_string(), transformed_wkb("POLYGON ((0 0,0 8,4 8,4 0,0 0))", 1, offset));
        EXPECT_EQ(c->result(1)->value().to_string(), transformed_wkb("POLYGON ((4 0,4 8,8 8,8 0,4 0))", 1, offset));
    }
}
TEST(GeoCoverageTest, ExactMidpointRangesDoNotHideNestedInteriors) {
    const std::vector<std::vector<std::string>> cases{
            {"POLYGON ((0 8,8 16,16 8,8 0,0 8))", "POLYGON ((7 7,10 7,8 10,7 7))"},
            {"POLYGON ((0 0,16 0,16 16,0 16,0 0),(2 8,8 14,14 8,8 2,2 8),(7 7,10 7,8 10,7 7))"}};
    for (double offset : {0.0, 1e6, 2e7, -2e7})
        for (const auto& text : cases)
            for (double tolerance : {0.0, 0.25}) {
                TestAllocator allocator;
                auto c = make(allocator);
                for (const auto& row : text) ASSERT_TRUE(c->append(Slice(transformed_wkb(row, 1, offset))).ok());
                const auto status = c->finish(tolerance);
                EXPECT_FALSE(status.ok());
                EXPECT_NE(status.to_string().find("overlapping"), std::string::npos) << status;
                EXPECT_FALSE(c->result(0).ok());
            }
}
std::vector<std::string> interval_coverage() {
    return {"POLYGON ((0 0,16 0,16 16,12 16,12 4,10 4,10 14,6 14,6 4,4 4,4 16,0 16,0 0),"
            "(7 1,7 3,9 3,9 1,7 1))",
            "POLYGON ((7 15,9 15,9 17,7 17,7 15))", "POLYGON ((7.5 1.5,8.5 1.5,8 2.5,7.5 1.5))"};
}
TEST(GeoCoverageTest, RingIntervalsRetainLongEdgesHolesAndClosedBounds) {
    const std::pair<double, double> transforms[] = {
            {1, 0}, {1, -2e7}, {1, 2e7}, {2 * std::numeric_limits<double>::denorm_min(), 0}, {1e150, 0}, {1e307, 0}};
    for (const auto& [scale, offset] : transforms)
        for (bool reverse : {false, true})
            for (int form = 0; form < 3; ++form) {
                TestAllocator allocator;
                auto c = make(allocator);
                std::vector<std::string> inputs;
                for (const auto& text : interval_coverage()) {
                    auto geometry = decoded(Slice(transformed_wkb(text, scale, offset)));
                    if (reverse)
                        for (auto& ring : geometry.rings) std::reverse(ring.begin(), ring.end());
                    std::string value;
                    ASSERT_TRUE(WkbCodec::to_wkb(geometry, &value, kCartesian).ok());
                    inputs.push_back(value);
                }
                append(*c, inputs);
                const double tolerance = scale > 1e150 ? 0 : 0.125 * scale;
                const auto status = form == 2 ? c->finish(tolerance) : c->finish(tolerance, form == 1);
                ASSERT_TRUE(status.ok()) << scale << " " << offset << " " << reverse << " " << form << " " << status;
                for (size_t row = 0; row < inputs.size(); ++row)
                    EXPECT_EQ(c->result(row)->value().to_string(), inputs[row]);
                c.reset();
                EXPECT_EQ(allocator.live, 0);
            }
}
TEST(GeoCoverageTest, RingIntervalsDoNotSkipContainingLongEdges) {
    for (double offset : {0.0, -2e7, 2e7}) {
        TestAllocator allocator;
        auto c = make(allocator);
        for (const auto& text : {interval_coverage()[0], std::string("POLYGON ((1 14,3 14,2 15,1 14))")})
            ASSERT_TRUE(c->append(Slice(transformed_wkb(text, 1, offset))).ok());
        const auto status = c->finish(0);
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("overlapping coverage interiors"), std::string::npos) << status;
        c.reset();
        EXPECT_EQ(allocator.live, 0);
    }
}
TEST(GeoCoverageTest, RingPreparationUnwindsAtEveryCheckpoint) {
    for (bool allocation_failure : {false, true}) {
        size_t total = 0;
        for (size_t attempt = 0; attempt <= total; ++attempt) {
            TestAllocator allocator;
            struct State {
                size_t checks = 0, stop;
                bool allocation_failure, armed = false;
                TestAllocator* allocator;
            } state{0, attempt == 0 ? std::numeric_limits<size_t>::max() : attempt, allocation_failure, false,
                    &allocator};
            GeoCoverageCheckpoint checkpoint{&state, [](void* context) {
                                                 auto& s = *static_cast<State*>(context);
                                                 if (!s.armed || ++s.checks != s.stop) return Status::OK();
                                                 if (!s.allocation_failure)
                                                     return Status::Cancelled("ring preparation test");
                                                 s.allocator->fail_after = s.allocator->allocations;
                                                 return Status::OK();
                                             }};
            auto c = make(allocator, checkpoint);
            for (const auto& text : interval_coverage()) ASSERT_TRUE(c->append(Slice(wkb(text))).ok());
            state.checks = 0;
            state.armed = true;
            const auto status = c->finish(0.125);
            if (attempt == 0) {
                ASSERT_TRUE(status.ok()) << status;
                total = state.checks;
            } else if (!allocation_failure) {
                EXPECT_TRUE(status.is_cancelled()) << attempt << " " << status;
            } else {
                EXPECT_TRUE(status.ok() || status.is_mem_limit_exceeded()) << attempt << " " << status;
            }
            c.reset();
            EXPECT_EQ(allocator.live, 0) << attempt;
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
