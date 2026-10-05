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

#include "exprs/agg/coverage_window.h"

#include <gtest/gtest.h>

#include <array>
#include <cmath>
#include <limits>
#include <thread>

#include "column/column_helper.h"
#include "column/geo_column.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_expr_fwd.h"
#include "exprs/agg/aggregate_factory.h"
#include "geo/wkb.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace starrocks {
namespace {
TypeDescriptor coverage_type(const std::string& crs = "EPSG:3857") {
    return TypeDescriptor::create_geo_type(
            TYPE_GEOMETRY, {GEO_LOGICAL_TYPE_GEOMETRY, GEO_COORDINATE_SYSTEM_CARTESIAN, GEO_EDGE_ALGORITHM_PLANAR, crs,
                            crs == "EPSG:4326" ? 4326 : 3857});
}
std::string bytes(const std::string& text) {
    WkbGeometry geometry;
    EXPECT_TRUE(WkbCodec::parse_wkt(text, &geometry, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
    std::string result;
    EXPECT_TRUE(WkbCodec::to_wkb(geometry, &result, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
    return result;
}
ColumnPtr coverage_input(const std::vector<std::optional<std::string>>& texts,
                         const TypeDescriptor& type = coverage_type()) {
    auto geo = GeoColumn::create(type, 0);
    auto nulls = NullColumn::create();
    for (const auto& text : texts) {
        if (text)
            geo->append_wkb(Slice(bytes(*text)));
        else
            geo->append_default();
        nulls->append(!text);
    }
    return NullableColumn::create(std::move(geo), std::move(nulls));
}
const GeoColumn* geo_data(const ColumnPtr& column) {
    return down_cast<const GeoColumn*>(down_cast<const NullableColumn*>(column.get())->data_column().get());
}
std::string output_text(const ColumnPtr& column, size_t row) {
    if (column->is_null(row)) return "NULL";
    WkbGeometry geometry;
    EXPECT_TRUE(
            WkbCodec::parse_wkb(geo_data(column)->get_wkb(row), &geometry, WkbCoordinateSemantics::GEOMETRY_CARTESIAN)
                    .ok());
    std::string text;
    EXPECT_TRUE(WkbCodec::to_wkt(geometry, &text, WkbCoordinateSemantics::GEOMETRY_CARTESIAN).ok());
    return text;
}
const std::string left = "POLYGON ((0 0,0 8,4 8,4.1 6,3.8 4,4.2 2,4 0,0 0))";
const std::string right = "POLYGON ((4 0,4.2 2,3.8 4,4.1 6,4 8,8 8,8 0,4 0))";

struct WindowHarness {
    RuntimeState runtime{TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr};
    MemPool pool;
    std::shared_ptr<MemTracker> query;
    std::unique_ptr<FunctionContext> ctx;
    CoverageWindowFunction fn;
    alignas(CoverageWindowState) std::array<uint8_t, sizeof(CoverageWindowState)> storage;
    bool destroyed = false;
    explicit WindowHarness(std::optional<double> tolerance = 1, std::optional<bool> boundary = false, bool third = true,
                           int64_t limit = -1, TypeDescriptor type = coverage_type()) {
        query = std::make_shared<MemTracker>(limit);
        runtime.init_mem_trackers(query);
        std::vector<TypeDescriptor> args{type, TypeDescriptor(TYPE_DOUBLE)};
        if (third) args.emplace_back(TYPE_BOOLEAN);
        ctx.reset(FunctionContext::create_context(&runtime, &pool, type, args));
        Columns constants{nullptr, tolerance ? ColumnHelper::create_const_column<TYPE_DOUBLE>(*tolerance, 1)
                                             : ColumnHelper::create_const_null_column(1)};
        if (third)
            constants.push_back(boundary ? ColumnHelper::create_const_column<TYPE_BOOLEAN>(*boundary, 1)
                                         : ColumnHelper::create_const_null_column(1));
        ctx->set_constant_columns(std::move(constants));
        fn.create(ctx.get(), storage.data());
    }
    void destroy() {
        if (!destroyed) {
            fn.destroy(ctx.get(), storage.data());
            destroyed = true;
        }
    }
    ~WindowHarness() { destroy(); }
    Status admit(const ColumnPtr& input, bool fresh = true, size_t start = 0, size_t end = 0) {
        return fn.admit_window_segment(ctx.get(), storage.data(), input.get(), input->size(), start,
                                       end ? end : input->size(), fresh, nullptr);
    }
    void finish(size_t rows) {
        fn.update_batch_single_state_with_frame(ctx.get(), storage.data(), nullptr, 0, rows, 0, rows);
    }
    ColumnPtr result(size_t rows) {
        auto result = fn.window_result_column(ctx.get(), storage.data());
        fn.get_values(ctx.get(), storage.data(), result.get(), 0, rows);
        return result;
    }
};

TEST(CoverageWindowTest, FullPartitionAcrossChunksPreservesEveryPosition) {
    for (const char* crs : {"EPSG:3857", "EPSG:4326"}) {
        auto type = coverage_type(crs);
        WindowHarness h(1, false, true, -1, type);
        auto a = coverage_input({left, std::nullopt}, type);
        auto b = coverage_input({right, "MULTIPOLYGON EMPTY"}, type);
        ASSERT_TRUE(h.admit(a).ok());
        ASSERT_TRUE(h.admit(b, false).ok());
        EXPECT_EQ(h.fn.kernel_calls(h.storage.data()), 0);
        h.finish(4);
        ASSERT_FALSE(h.ctx->has_error()) << h.ctx->error_msg();
        EXPECT_EQ(h.fn.kernel_calls(h.storage.data()), 1);
        auto first = h.result(2), second = h.result(2);
        ASSERT_FALSE(h.ctx->has_error()) << h.ctx->error_msg();
        EXPECT_EQ(output_text(first, 0), "POLYGON ((0 0, 0 8, 4 8, 4 0, 0 0))");
        EXPECT_EQ(output_text(first, 1), "NULL");
        EXPECT_EQ(output_text(second, 0), "POLYGON ((4 0, 4 8, 8 8, 8 0, 4 0))");
        EXPECT_EQ(output_text(second, 1), "MULTIPOLYGON EMPTY");
        EXPECT_EQ(geo_data(first)->descriptor().type, *type.geo_type);
        first->check_or_die();
        second->check_or_die();
    }
}
TEST(CoverageWindowTest, MultiplePartitionsShareAChunkWithoutDeduplicatingRows) {
    WindowHarness h(0);
    auto source = coverage_input({left, left, "POLYGON EMPTY", std::nullopt});
    ASSERT_TRUE(h.admit(source, true, 0, 1).ok());
    ASSERT_TRUE(h.admit(source, true, 1, 4).ok());
    h.finish(1);
    auto result = h.fn.window_result_column(h.ctx.get(), h.storage.data());
    h.fn.get_values(h.ctx.get(), h.storage.data(), result.get(), 0, 1);
    h.fn.reset(h.ctx.get(), {}, h.storage.data());
    h.finish(3);
    h.fn.get_values(h.ctx.get(), h.storage.data(), result.get(), 1, 4);
    ASSERT_FALSE(h.ctx->has_error()) << h.ctx->error_msg();
    EXPECT_EQ(geo_data(result)->get_wkb(0).to_string(), bytes(left));
    EXPECT_EQ(geo_data(result)->get_wkb(1).to_string(), bytes(left));
    EXPECT_EQ(output_text(result, 2), "POLYGON EMPTY");
    EXPECT_TRUE(result->is_null(3));
    result->check_or_die();
}
TEST(CoverageWindowTest, NullParametersSkipInvalidTopologyAndDefaultIsTrue) {
    for (bool null_tolerance : {false, true}) {
        WindowHarness h(null_tolerance ? std::optional<double>{} : -1,
                        null_tolerance ? std::optional<bool>{true} : std::optional<bool>{});
        auto source = coverage_input({left, left, std::nullopt});
        ASSERT_TRUE(h.admit(source).ok());
        h.finish(3);
        auto result = h.result(3);
        ASSERT_FALSE(h.ctx->has_error()) << h.ctx->error_msg();
        for (size_t i = 0; i < 3; ++i) EXPECT_TRUE(result->is_null(i));
        EXPECT_EQ(h.fn.kernel_calls(h.storage.data()), 0);
    }
    WindowHarness two(1, true, false), three(1, true);
    auto input = coverage_input({left, right});
    ASSERT_TRUE(two.admit(input).ok());
    ASSERT_TRUE(three.admit(input).ok());
    two.finish(2);
    three.finish(2);
    auto a = two.result(2), b = three.result(2);
    ASSERT_FALSE(two.ctx->has_error());
    ASSERT_FALSE(three.ctx->has_error());
    for (size_t i = 0; i < 2; ++i) EXPECT_EQ(geo_data(a)->get_wkb(i), geo_data(b)->get_wkb(i));
}
TEST(CoverageWindowTest, ParameterAndDescriptorGuardsAlsoApplyToNullOrEmptyInput) {
    for (double value :
         {-1.0, 1e200, std::numeric_limits<double>::infinity(), std::numeric_limits<double>::quiet_NaN()}) {
        WindowHarness h(value);
        EXPECT_FALSE(h.admit(coverage_input({std::nullopt, "POLYGON EMPTY"})).ok());
    }
    WindowHarness varying;
    varying.ctx->set_constant_columns({nullptr, nullptr, ColumnHelper::create_const_column<TYPE_BOOLEAN>(true, 1)});
    EXPECT_FALSE(varying.admit(coverage_input({"POLYGON EMPTY"})).ok());
    WindowHarness wrong_descriptor;
    EXPECT_FALSE(wrong_descriptor.admit(coverage_input({"POLYGON EMPTY"}, coverage_type("EPSG:4326"))).ok());
    WindowHarness malformed;
    auto raw = GeoColumn::create(coverage_type(), 0);
    raw->append_wkb(Slice("x"));
    EXPECT_FALSE(malformed.admit(raw).ok());
    WindowHarness line;
    EXPECT_FALSE(line.admit(coverage_input({"LINESTRING (0 0,1 1)"})).ok());
}
TEST(CoverageWindowTest, InvalidCoverageFailsBeforeAnyPartitionOutput) {
    WindowHarness h;
    auto source = coverage_input({left, left});
    ASSERT_TRUE(h.admit(source).ok());
    h.finish(2);
    ASSERT_TRUE(h.ctx->has_error());
    EXPECT_NE(std::string(h.ctx->error_msg()).find("ST_CoverageSimplify"), std::string::npos);
}
TEST(CoverageWindowTest, EmptyInputStillChecksPlanParametersAndTypes) {
    WindowHarness negative(-1);
    EXPECT_FALSE(negative.fn.validate_window_plan(negative.ctx.get()).ok());
    WindowHarness valid(0);
    EXPECT_TRUE(valid.fn.validate_window_plan(valid.ctx.get()).ok());
    TypeDescriptor missing(TYPE_GEOMETRY);
    WindowHarness descriptor(0, true, false, -1, missing);
    EXPECT_FALSE(descriptor.fn.validate_window_plan(descriptor.ctx.get()).ok());
    std::unique_ptr<FunctionContext> arity(
            FunctionContext::create_context(&valid.runtime, &valid.pool, coverage_type(), {coverage_type()}));
    EXPECT_FALSE(valid.fn.validate_window_plan(arity.get()).ok());
}
TEST(CoverageWindowTest, LimitsAreCapturedAndQueryMemoryHasAnExplicitOwner) {
    const auto previous = config::geo_coverage_max_rows_per_partition;
    struct Restore {
        int64_t value;
        ~Restore() { config::geo_coverage_max_rows_per_partition = value; }
    } restore{previous};
    config::geo_coverage_max_rows_per_partition = 2;
    WindowHarness h;
    ASSERT_TRUE(h.admit(coverage_input({left})).ok());
    config::geo_coverage_max_rows_per_partition = 100;
    auto status = h.admit(coverage_input({right, "POLYGON EMPTY"}), false);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("max_rows"), std::string::npos);
    WindowHarness query_limited(1, false, true, 1);
    EXPECT_FALSE(query_limited.admit(coverage_input({left})).ok());
    query_limited.destroy();
    EXPECT_EQ(query_limited.query->consumption(), 0);
}
TEST(CoverageWindowTest, CancellationAndOutputOwnerSurviveWorkerTransfer) {
    WindowHarness cancelled;
    cancelled.runtime.set_is_cancelled(true);
    EXPECT_FALSE(cancelled.admit(coverage_input({left})).ok());
    WindowHarness h;
    ASSERT_TRUE(h.admit(coverage_input({left, right})).ok());
    h.finish(2);
    auto result = h.result(2);
    ASSERT_FALSE(h.ctx->has_error()) << h.ctx->error_msg();
    EXPECT_GT(h.query->consumption(), 0);
    EXPECT_GT(geo_data(result)->reference_memory_usage(0, 2), 0);
    h.destroy();
    EXPECT_GT(h.query->consumption(), 0);
    auto query = h.query;
    std::thread worker([column = std::move(result), query]() mutable {
        column->check_or_die();
        EXPECT_FALSE(output_text(column, 0).empty());
        column.reset();
        EXPECT_EQ(query->consumption(), 0);
    });
    worker.join();
}
TEST(CoverageWindowTest, EveryAdmissionLimitRejectsBeforeKernelAndCleansUp) {
    struct LimitsRestore {
        int64_t rows = config::geo_coverage_max_rows_per_partition;
        int64_t vertices = config::geo_coverage_max_vertices_per_partition;
        int64_t input = config::geo_coverage_max_input_bytes_per_partition;
        int64_t working = config::geo_coverage_max_working_bytes_per_partition;
        ~LimitsRestore() {
            config::geo_coverage_max_rows_per_partition = rows;
            config::geo_coverage_max_vertices_per_partition = vertices;
            config::geo_coverage_max_input_bytes_per_partition = input;
            config::geo_coverage_max_working_bytes_per_partition = working;
        }
    } saved;
    const auto wkb_bytes = bytes(left).size() + bytes(right).size();
    config::geo_coverage_max_rows_per_partition = 2;
    config::geo_coverage_max_vertices_per_partition = 16;
    config::geo_coverage_max_input_bytes_per_partition = wkb_bytes;
    auto source = coverage_input({left, right});
    {
        WindowHarness h;
        ASSERT_TRUE(h.admit(source).ok());
        h.finish(2);
        ASSERT_FALSE(h.ctx->has_error()) << h.ctx->error_msg();
        EXPECT_EQ(h.fn.kernel_calls(h.storage.data()), 1);
    }
    for (int limit = 0; limit < 4; ++limit) {
        config::geo_coverage_max_rows_per_partition = limit == 0 ? 1 : 2;
        config::geo_coverage_max_vertices_per_partition = limit == 1 ? 15 : 16;
        config::geo_coverage_max_input_bytes_per_partition = limit == 2 ? wkb_bytes - 1 : wkb_bytes;
        config::geo_coverage_max_working_bytes_per_partition = limit == 3 ? 1 : saved.working;
        WindowHarness h;
        auto status = h.admit(source);
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find("geo_coverage_max_"), std::string::npos) << status;
        EXPECT_EQ(h.fn.kernel_calls(h.storage.data()), 0);
        h.destroy();
        EXPECT_EQ(h.query->consumption(), 0);
    }
}
TEST(CoverageWindowTest, SpillAndWrongPhysicalParameterFailAtTheBoundary) {
    WindowHarness h;
    auto& options = const_cast<TQueryOptions&>(h.runtime.query_options());
    options.__set_enable_spill(true);
    EXPECT_FALSE(h.fn.validate_window_plan(h.ctx.get()).ok());
    options.__set_enable_spill(false);
    h.ctx->set_constant_columns({nullptr, ColumnHelper::create_const_column<TYPE_INT>(1, 1),
                                 ColumnHelper::create_const_column<TYPE_BOOLEAN>(true, 1)});
    EXPECT_FALSE(h.fn.validate_window_plan(h.ctx.get()).ok());
}
TEST(CoverageWindowTest, IndependentPartitionsCanRunOnConcurrentWorkers) {
    std::array<std::thread, 2> workers;
    for (auto& worker : workers)
        worker = std::thread([] {
            WindowHarness h;
            ASSERT_TRUE(h.admit(coverage_input({left})).ok());
            ASSERT_TRUE(h.admit(coverage_input({right}), false).ok());
            h.finish(2);
            auto first = h.result(1), second = h.result(1);
            ASSERT_FALSE(h.ctx->has_error()) << h.ctx->error_msg();
            EXPECT_EQ(h.fn.kernel_calls(h.storage.data()), 1);
            EXPECT_EQ(output_text(first, 0), "POLYGON ((0 0, 0 8, 4 8, 4 0, 0 0))");
            EXPECT_EQ(output_text(second, 0), "POLYGON ((4 0, 4 8, 8 8, 8 0, 4 0))");
            first.reset();
            second.reset();
            h.destroy();
            EXPECT_EQ(h.query->consumption(), 0);
        });
    for (auto& worker : workers) worker.join();
}
TEST(CoverageWindowTest, NativeViewSerializesWithoutExecutionPointers) {
    WindowHarness h(0);
    auto input = coverage_input({left, std::nullopt, "MULTIPOLYGON EMPTY"});
    ASSERT_TRUE(h.admit(input).ok());
    h.finish(3);
    auto result = h.result(3);
    ASSERT_FALSE(h.ctx->has_error()) << h.ctx->error_msg();
    const auto* source = geo_data(result);
    std::vector<uint8_t> wire(source->serialized_column_size());
    ASSERT_TRUE(source->serialize_column(wire.data()).ok());
    auto restored = GeoColumn::create(coverage_type(), 0);
    auto decoded = restored->deserialize_column(wire.data(), wire.data() + wire.size());
    ASSERT_TRUE(decoded.ok()) << decoded.status();
    EXPECT_EQ(restored->descriptor(), source->descriptor());
    for (size_t i = 0; i < 3; ++i) EXPECT_EQ(restored->get_wkb(i), source->get_wkb(i));
    result.reset();
    h.destroy();
    EXPECT_EQ(h.query->consumption(), 0);
    EXPECT_EQ(restored->get_wkb(0).to_string(), bytes(left));
}
TEST(CoverageWindowTest, WindowOnlyRegistrationDoesNotExposeOrdinaryAggregation) {
    EXPECT_NE(get_window_function("st_coveragesimplify", TYPE_GEOMETRY, TYPE_GEOMETRY, false), nullptr);
    EXPECT_NE(get_window_function("st_coveragesimplify", TYPE_GEOMETRY, TYPE_GEOMETRY, true), nullptr);
    EXPECT_EQ(get_window_function("st_coveragesimplify", TYPE_GEOGRAPHY, TYPE_GEOGRAPHY, true), nullptr);
    EXPECT_EQ(get_aggregate_function("st_coveragesimplify", TYPE_GEOMETRY, TYPE_GEOMETRY, true), nullptr);
}
} // namespace
} // namespace starrocks
