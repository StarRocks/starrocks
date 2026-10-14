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

// Compare FIRST_VALUE/LAST_VALUE IGNORE NULLS input retention using the real Analytor,
// with pipeline_analytic_enable_ignore_nulls_streaming disabled (legacy) and enabled.
// Like analytor_lead_lag_bench, use one partition of ~1M nullable INT rows, 4096-row
// input chunks, four-chunk eviction batches, and drain output after each input chunk.
//
// Cumulative ROWS frames should retain only a few chunks even with all NULLs, a long
// NULL tail after the first value, or a first non-null arriving at the end of input.
// FIRST_VALUE/LAST_VALUE copy the selected value into aggregate state, so those rows
// do not pin the input buffer. Also compare bounded preceding/following ROWS frames
// and a fully unbounded frame: UNBOUNDED FOLLOWING must still materialize the partition.
//
// PeakRows comes from PeakBufferedRows; PeakMiB estimates nullable INT input payload
// (five bytes per row), not allocator usage or process RSS. Timing includes input
// construction and output draining, as in the LEAD/LAG benchmark.
//
// Build with --with-bench, then export STARROCKS_HOME and UDF_RUNTIME_DIR before running:
//   analytor_first_last_value_bench --benchmark_filter='.*cumulative.*'

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "base/utility/defer_op.h"
#include "bench/thrift_bench_util.h"
#include "column/chunk.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/runtime_profile.h"
#include "common/system/cpu_info.h"
#include "exec/analytor.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {
namespace {

enum Fn { FIRST_VALUE, LAST_VALUE };
enum Pattern { ALL_NULL, DENSE, SPARSE, HEAD_THEN_NULL, TAIL_NONNULL };
enum Frame { CUMULATIVE, PRECEDING, FOLLOWING, UNBOUNDED };

constexpr int64_t kRows = 1 << 20;
constexpr int64_t kChunk = 4096;
constexpr int64_t kSparseGap = 64;
constexpr int64_t kFrameOffset = 64;

TPlanNode make_first_last_value_plan(Fn fn, Frame frame, TupleId input_tuple, SlotId value_slot) {
    const TTypeDesc int_type = bench::make_scalar_type(TPrimitiveType::INT);
    const TExpr agg =
            bench::make_builtin_aggregate_expr(fn == FIRST_VALUE ? "first_value" : "last_value", {int_type}, int_type,
                                               {bench::make_slot_ref_expr(input_tuple, value_slot, int_type)}, true);

    TAnalyticWindowBoundary current_row;
    current_row.__set_type(TAnalyticWindowBoundaryType::CURRENT_ROW);
    TAnalyticWindow window;
    switch (frame) {
    case CUMULATIVE:
        window = bench::make_rows_window(std::nullopt, current_row);
        break;
    case PRECEDING:
        window = bench::make_rows_window(
                bench::make_rows_offset_boundary(TAnalyticWindowBoundaryType::PRECEDING, kFrameOffset), current_row);
        break;
    case FOLLOWING:
        window = bench::make_rows_window(
                current_row, bench::make_rows_offset_boundary(TAnalyticWindowBoundaryType::FOLLOWING, kFrameOffset));
        break;
    case UNBOUNDED:
        window = bench::make_rows_window(std::nullopt, std::nullopt);
        break;
    }
    return bench::make_analytic_plan_node({agg}, input_tuple, window);
}

ColumnPtr make_value_column(Pattern pattern, int64_t begin, int64_t count) {
    auto data = Int32Column::create();
    auto nulls = NullColumn::create();
    for (int64_t row = begin; row < begin + count; ++row) {
        bool is_null = false;
        switch (pattern) {
        case ALL_NULL:
            is_null = true;
            break;
        case DENSE:
            break;
        case SPARSE:
            is_null = row % kSparseGap != 0;
            break;
        case HEAD_THEN_NULL:
            is_null = row != 0;
            break;
        case TAIL_NONNULL:
            is_null = row != kRows - 1;
            break;
        }
        data->append(is_null ? 0 : static_cast<int32_t>(row));
        nulls->append(is_null ? 1 : 0);
    }
    return NullableColumn::create(std::move(data), std::move(nulls));
}

void BM_FirstLastValueIgnoreNulls(benchmark::State& bstate, Fn fn, Pattern pattern, Frame frame, bool streaming) {
    const auto old_streaming = config::pipeline_analytic_enable_streaming_process;
    const auto old_ignore_nulls_streaming = config::pipeline_analytic_enable_ignore_nulls_streaming;
    const auto old_removable_chunks = config::pipeline_analytic_removable_chunk_num;
    DeferOp restore_config([&] {
        config::pipeline_analytic_enable_streaming_process = old_streaming;
        config::pipeline_analytic_enable_ignore_nulls_streaming = old_ignore_nulls_streaming;
        config::pipeline_analytic_removable_chunk_num = old_removable_chunks;
    });

    for (auto _ : bstate) {
        bstate.PauseTiming();
        config::pipeline_analytic_enable_streaming_process = true;
        config::pipeline_analytic_enable_ignore_nulls_streaming = streaming;
        config::pipeline_analytic_removable_chunk_num = 4;

        ObjectPool pool;
        TDescriptorTableBuilder descriptors;
        TTupleDescriptorBuilder input_tuple;
        input_tuple.add_slot(TSlotDescriptorBuilder().type(TYPE_INT).nullable(true).column_name("v").build());
        input_tuple.build(&descriptors);
        TTupleDescriptorBuilder output_tuple;
        output_tuple.add_slot(TSlotDescriptorBuilder().type(TYPE_INT).nullable(true).column_name("out_v").build());
        output_tuple.build(&descriptors);
        RuntimeState state(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr);
        DescriptorTbl* desc_tbl = nullptr;
        CHECK(DescriptorTbl::create(&state, &pool, descriptors.desc_tbl(), &desc_tbl, config::vector_chunk_size).ok());
        state.set_desc_tbl(desc_tbl);
        state.init_instance_mem_tracker();

        const SlotId value_slot = desc_tbl->get_tuple_descriptor(0)->slots()[0]->id();
        const auto* result_tuple = desc_tbl->get_tuple_descriptor(1);
        const TPlanNode plan = make_first_last_value_plan(fn, frame, 0, value_slot);
        RuntimeProfile profile("Analytor");
        Analytor analytor(plan, result_tuple, false);
        CHECK(analytor.prepare(&state, &pool, &profile).ok());
        const bool expect_streaming = streaming && frame != UNBOUNDED;
        const std::string expected_mode = std::string(expect_streaming ? "Streaming/" : "Materializing/") +
                                          (frame == PRECEDING || frame == FOLLOWING ? "ByDefinition" : "Cumulative");
        const auto process_mode = profile.get_info_string("ProcessMode");
        CHECK(process_mode.has_value() && *process_mode == expected_mode) << "Expected " << expected_mode;
        CHECK(analytor.open(&state).ok());

        int64_t output_rows = 0;
        auto drain = [&] {
            while (ChunkPtr out = analytor.poll_chunk_buffer()) {
                output_rows += out->num_rows();
                benchmark::DoNotOptimize(out);
            }
        };
        bstate.ResumeTiming();
        for (int64_t begin = 0; begin < kRows; begin += kChunk) {
            auto chunk = std::make_shared<Chunk>();
            chunk->append_column(make_value_column(pattern, begin, std::min(kChunk, kRows - begin)), value_slot);
            CHECK(analytor.process(&state, chunk).ok());
            drain();
        }
        const int64_t rows_before_eos = output_rows;
        CHECK(analytor.finish_process(&state).ok());
        drain();
        bstate.PauseTiming();
        CHECK_EQ(output_rows, kRows);

        const auto* peak = profile.get_counter("PeakBufferedRows");
        const auto* evictions = profile.get_counter("RemoveUnusedRowsCount");
        const auto* evicted_rows = profile.get_counter("RemoveUnusedTotalRows");
        CHECK(peak != nullptr && evictions != nullptr && evicted_rows != nullptr);
        bstate.counters["PeakRows"] = static_cast<double>(peak->value());
        bstate.counters["PeakMiB"] = static_cast<double>(peak->value()) * 5.0 / (1024.0 * 1024.0);
        bstate.counters["Evictions"] = static_cast<double>(evictions->value());
        bstate.counters["EvictedRows"] = static_cast<double>(evicted_rows->value());
        bstate.counters["RowsBeforeEOS"] = static_cast<double>(rows_before_eos);
        analytor.close(&state);
        bstate.ResumeTiming();
    }
}

BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, first_cumulative_all_null_legacy, FIRST_VALUE, ALL_NULL, CUMULATIVE,
                  false)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, first_cumulative_all_null_streaming, FIRST_VALUE, ALL_NULL, CUMULATIVE,
                  true)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, first_cumulative_dense_legacy, FIRST_VALUE, DENSE, CUMULATIVE, false)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, first_cumulative_dense_streaming, FIRST_VALUE, DENSE, CUMULATIVE, true)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, first_cumulative_sparse_legacy, FIRST_VALUE, SPARSE, CUMULATIVE, false)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, first_cumulative_sparse_streaming, FIRST_VALUE, SPARSE, CUMULATIVE,
                  true)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, first_cumulative_head_then_null_legacy, FIRST_VALUE, HEAD_THEN_NULL,
                  CUMULATIVE, false)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, first_cumulative_head_then_null_streaming, FIRST_VALUE, HEAD_THEN_NULL,
                  CUMULATIVE, true)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, first_cumulative_tail_nonnull_legacy, FIRST_VALUE, TAIL_NONNULL,
                  CUMULATIVE, false)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, first_cumulative_tail_nonnull_streaming, FIRST_VALUE, TAIL_NONNULL,
                  CUMULATIVE, true)
        ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, last_cumulative_all_null_legacy, LAST_VALUE, ALL_NULL, CUMULATIVE,
                  false)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, last_cumulative_all_null_streaming, LAST_VALUE, ALL_NULL, CUMULATIVE,
                  true)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, last_cumulative_dense_legacy, LAST_VALUE, DENSE, CUMULATIVE, false)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, last_cumulative_dense_streaming, LAST_VALUE, DENSE, CUMULATIVE, true)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, last_cumulative_sparse_legacy, LAST_VALUE, SPARSE, CUMULATIVE, false)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, last_cumulative_sparse_streaming, LAST_VALUE, SPARSE, CUMULATIVE, true)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, last_cumulative_head_then_null_legacy, LAST_VALUE, HEAD_THEN_NULL,
                  CUMULATIVE, false)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, last_cumulative_head_then_null_streaming, LAST_VALUE, HEAD_THEN_NULL,
                  CUMULATIVE, true)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, last_cumulative_tail_nonnull_legacy, LAST_VALUE, TAIL_NONNULL,
                  CUMULATIVE, false)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, last_cumulative_tail_nonnull_streaming, LAST_VALUE, TAIL_NONNULL,
                  CUMULATIVE, true)
        ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, first_preceding_sparse_legacy, FIRST_VALUE, SPARSE, PRECEDING, false)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, first_preceding_sparse_streaming, FIRST_VALUE, SPARSE, PRECEDING, true)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, last_preceding_sparse_legacy, LAST_VALUE, SPARSE, PRECEDING, false)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, last_preceding_sparse_streaming, LAST_VALUE, SPARSE, PRECEDING, true)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, first_following_sparse_legacy, FIRST_VALUE, SPARSE, FOLLOWING, false)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, first_following_sparse_streaming, FIRST_VALUE, SPARSE, FOLLOWING, true)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, last_following_sparse_legacy, LAST_VALUE, SPARSE, FOLLOWING, false)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, last_following_sparse_streaming, LAST_VALUE, SPARSE, FOLLOWING, true)
        ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, first_unbounded_sparse_legacy, FIRST_VALUE, SPARSE, UNBOUNDED, false)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, first_unbounded_sparse_streaming, FIRST_VALUE, SPARSE, UNBOUNDED, true)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, last_unbounded_sparse_legacy, LAST_VALUE, SPARSE, UNBOUNDED, false)
        ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_FirstLastValueIgnoreNulls, last_unbounded_sparse_streaming, LAST_VALUE, SPARSE, UNBOUNDED, true)
        ->Unit(benchmark::kMillisecond);

} // namespace
} // namespace starrocks

int main(int argc, char** argv) {
    // Standalone benchmarks need the same config defaults and CPU setup as the LEAD/LAG benchmark.
    if (!starrocks::config::init(nullptr)) return 1;
    starrocks::CpuInfo::init();
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
