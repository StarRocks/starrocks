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

// Benchmark: peak input-buffer retention of `lag(col, 1) IGNORE NULLS` and `lead(col, 1) IGNORE
// NULLS` in the Analytor operator, comparing the legacy whole-partition materializing path against
// the streaming eviction path (config::pipeline_analytic_enable_ignore_nulls_streaming). LAG evicts
// behind a watermark at the most recent non-null; LEAD instead waits until enough future non-nulls
// are buffered, then evicts finished prefixes.
//
// The headline metric is the profile counter `PeakBufferedRows` (high-water-mark of rows retained in
// Analytor::_input_chunks), reported as a custom benchmark counter. Wall-clock time is incidental.
//
// Data patterns (single partition, so the whole input is one partition):
//   ALL_NULL        - column is entirely NULL.  LAG: target never set -> evicts to ~chunk. LEAD
//                     cannot resolve row 0 until EOS -> peak ~ full partition.
//   DENSE           - every row non-null.        LAG: target tracks current -> evicts to ~chunk.
//                     LEAD: wait ~offset rows, then evict prefixes -> peak ~ chunk + offset.
//   SPARSE          - one non-null every G rows.  Both retain ~G rows while the gap is unresolved.
//   HEAD_THEN_NULL  - row 0 non-null, rest NULL.  LAG: target PINNED at 0 -> no eviction (the
//                     documented caveat; only value-caching would fix this one).
//   TAIL_NONNULL    - only the last row is non-null. LEAD waits until that row -> peak ~ full
//                     partition, then prefix eviction after it resolves.
// Legacy (materializing) always retains the whole partition regardless of pattern.
//
// NOTE: exercises the real Analytor with a hand-built analytic TPlanNode. Validated by construction;
// must be compiled/run in a StarRocks BE build. The Thrift construction is isolated in
// thrift_bench_util.h.

#include <benchmark/benchmark.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "bench/thrift_bench_util.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/runtime_profile.h"
#include "common/system/cpu_info.h"
#include "exec/analytor.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

enum Pattern { ALL_NULL = 0, DENSE = 1, SPARSE = 2, HEAD_THEN_NULL = 3, TAIL_NONNULL = 4 };

// LAG looks backwards and LEAD forwards; otherwise the two plans are identical.
enum Fn { LAG = 0, LEAD = 1 };

static constexpr int64_t kRows = 1 << 20; // ~1M rows total
static constexpr int64_t kChunk = 4096;   // rows per chunk fed to the operator
static constexpr int64_t kSparseGap = 64; // one non-null every kSparseGap rows for SPARSE

static TPlanNode make_lead_lag_plan_node(Fn fn, TupleId input_tuple_id, SlotId value_slot_id, int64_t offset) {
    const TTypeDesc int_type = bench::make_scalar_type(TPrimitiveType::INT);
    TExpr agg = bench::make_builtin_aggregate_expr(
            fn == LAG ? "lag" : "lead", {int_type}, int_type,
            {bench::make_slot_ref_expr(input_tuple_id, value_slot_id, int_type),
             bench::make_bigint_literal_expr(offset), bench::make_null_literal_expr(int_type)},
            true);
    const auto boundary_type =
            fn == LAG ? TAnalyticWindowBoundaryType::PRECEDING : TAnalyticWindowBoundaryType::FOLLOWING;
    TAnalyticWindow window =
            bench::make_rows_window(std::nullopt, bench::make_rows_offset_boundary(boundary_type, offset));
    return bench::make_analytic_plan_node({agg}, input_tuple_id, window);
}

// A nullable INT32 column for global rows [begin, begin+count) under the given pattern.
static ColumnPtr make_value_chunk_column(Pattern pattern, int64_t begin, int64_t count) {
    auto data = Int32Column::create();
    auto nulls = NullColumn::create();
    for (int64_t i = 0; i < count; ++i) {
        const int64_t g = begin + i;
        bool is_null;
        switch (pattern) {
        case ALL_NULL:
            is_null = true;
            break;
        case DENSE:
            is_null = false;
            break;
        case SPARSE:
            is_null = (g % kSparseGap != 0);
            break;
        case TAIL_NONNULL:
            is_null = (g != kRows - 1);
            break;
        case HEAD_THEN_NULL:
        default:
            is_null = (g != 0);
            break;
        }
        data->append(is_null ? 0 : static_cast<int32_t>(g));
        nulls->append(is_null ? 1 : 0);
    }
    return NullableColumn::create(std::move(data), std::move(nulls));
}

static void BM_LeadLagIgnoreNulls(benchmark::State& bstate, Fn fn, Pattern pattern, bool streaming) {
    for (auto _ : bstate) {
        bstate.PauseTiming();
        config::pipeline_analytic_enable_ignore_nulls_streaming = streaming;
        // Fine-grained eviction so PeakBufferedRows reflects the algorithmic bound rather than the
        // eviction batch size (default 128 chunks would otherwise set a ~128-chunk floor).
        config::pipeline_analytic_removable_chunk_num = 4;

        ObjectPool pool;
        TDescriptorTableBuilder dtb;
        {
            TTupleDescriptorBuilder in_tuple;
            in_tuple.add_slot(TSlotDescriptorBuilder().type(TYPE_INT).nullable(true).column_name("v").build());
            in_tuple.build(&dtb);
            TTupleDescriptorBuilder out_tuple;
            out_tuple.add_slot(TSlotDescriptorBuilder().type(TYPE_INT).nullable(true).column_name("out_v").build());
            out_tuple.build(&dtb);
        }
        auto* state = pool.add(new RuntimeState(TUniqueId(), TQueryOptions(), TQueryGlobals(), nullptr));
        DescriptorTbl* desc_tbl = nullptr;
        CHECK(DescriptorTbl::create(state, &pool, dtb.desc_tbl(), &desc_tbl, config::vector_chunk_size).ok());
        state->set_desc_tbl(desc_tbl);
        state->init_instance_mem_tracker();

        const TupleId in_tuple_id = 0;
        const TupleId out_tuple_id = 1;
        const SlotId col_slot_id = desc_tbl->get_tuple_descriptor(in_tuple_id)->slots()[0]->id();
        TupleDescriptor* result_tuple = desc_tbl->get_tuple_descriptor(out_tuple_id);

        TPlanNode tnode = make_lead_lag_plan_node(fn, in_tuple_id, col_slot_id, 1);
        RuntimeProfile profile("Analytor");

        auto analytor = std::make_shared<Analytor>(tnode, result_tuple, false);
        CHECK(analytor->prepare(state, &pool, &profile).ok());
        CHECK(analytor->open(state).ok());

        bstate.ResumeTiming();

        int64_t fed = 0;
        while (fed < kRows) {
            const int64_t n = std::min<int64_t>(kChunk, kRows - fed);
            auto chunk = std::make_shared<Chunk>();
            chunk->append_column(make_value_chunk_column(pattern, fed, n), col_slot_id);
            CHECK(analytor->process(state, chunk).ok());
            fed += n;
            // Drain output so the OUTPUT buffer does not dominate; we measure INPUT retention.
            while (ChunkPtr out = analytor->poll_chunk_buffer()) {
                benchmark::DoNotOptimize(out);
            }
        }
        CHECK(analytor->finish_process(state).ok());
        while (ChunkPtr out = analytor->poll_chunk_buffer()) {
            benchmark::DoNotOptimize(out);
        }

        bstate.PauseTiming();
        auto* peak = profile.get_counter("PeakBufferedRows");
        auto* evicted = profile.get_counter("RemoveUnusedRowsCount");
        const double peak_rows = peak ? static_cast<double>(peak->value()) : -1;
        bstate.counters["PeakRows"] = peak_rows;
        // nullable int32 ~ 4 bytes data + 1 byte null flag per row
        bstate.counters["PeakMiB"] = peak_rows * 5.0 / (1024.0 * 1024.0);
        bstate.counters["Evictions"] = evicted ? static_cast<double>(evicted->value()) : 0;

        analytor->close(state);
        bstate.ResumeTiming();
    }
}

BENCHMARK_CAPTURE(BM_LeadLagIgnoreNulls, lag_all_null_legacy, LAG, ALL_NULL, false)->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_LeadLagIgnoreNulls, lag_dense_legacy, LAG, DENSE, false)->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_LeadLagIgnoreNulls, lag_sparse_legacy, LAG, SPARSE, false)->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_LeadLagIgnoreNulls, lag_head_then_null_legacy, LAG, HEAD_THEN_NULL, false)
        ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(BM_LeadLagIgnoreNulls, lag_all_null_streaming, LAG, ALL_NULL, true)
        ->Unit(benchmark::kMillisecond)
        ->Iterations(1);
BENCHMARK_CAPTURE(BM_LeadLagIgnoreNulls, lag_dense_streaming, LAG, DENSE, true)->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_LeadLagIgnoreNulls, lag_sparse_streaming, LAG, SPARSE, true)->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_LeadLagIgnoreNulls, lag_head_then_null_streaming, LAG, HEAD_THEN_NULL, true)
        ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(BM_LeadLagIgnoreNulls, lead_all_null_legacy, LEAD, ALL_NULL, false)->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_LeadLagIgnoreNulls, lead_dense_legacy, LEAD, DENSE, false)->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_LeadLagIgnoreNulls, lead_sparse_legacy, LEAD, SPARSE, false)->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_LeadLagIgnoreNulls, lead_tail_nonnull_legacy, LEAD, TAIL_NONNULL, false)
        ->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(BM_LeadLagIgnoreNulls, lead_all_null_streaming, LEAD, ALL_NULL, true)->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_LeadLagIgnoreNulls, lead_dense_streaming, LEAD, DENSE, true)->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_LeadLagIgnoreNulls, lead_sparse_streaming, LEAD, SPARSE, true)->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_LeadLagIgnoreNulls, lead_tail_nonnull_streaming, LEAD, TAIL_NONNULL, true)
        ->Unit(benchmark::kMillisecond);

} // namespace starrocks

int main(int argc, char** argv) {
    // Standalone benchmarks do not run the BE's GlobalEnv initialization, which normally loads
    // config defaults. Analytor::open() allocates its aggregate state from a MemPool, and its
    // processing mode depends on those defaults, so a failure here must not be ignored.
    // config::init() expands ${STARROCKS_HOME} and ${UDF_RUNTIME_DIR} in the defaults; export both
    // before running this benchmark.
    if (!starrocks::config::init(nullptr)) return 1;
    starrocks::CpuInfo::init();

    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
