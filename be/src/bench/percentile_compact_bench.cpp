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

// Benchmarks the percentile_approx pass-through (streaming) serialization path:
// legacy (a full TDigest blob per row) vs the compact RAW format
// (enable_percentile_compact_intermediate). Two axes:
//   - convert: convert_to_serialize_format only (serialize-side CPU);
//   - roundtrip: convert + merge every record into a fresh state (the cost a
//     split aggregation pays in the pass-through + merge phases).
// rows_per_s lets the two be compared directly; bytes_per_chunk shows the wire
// size (~77 B/row legacy vs 17 B/row compact).
//
// Build & run:
//   cmake -DWITH_BENCH=ON -DCMAKE_BUILD_TYPE=Release -S be -B be/build_Release
//   cmake --build be/build_Release --target percentile_compact_bench
//   ./be/build_Release/src/bench/percentile_compact_bench

#include <benchmark/benchmark.h>

#include <random>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "exprs/function_context.h"
#include "runtime/mem_pool.h"
#include "runtime/memory/counting_allocator.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

static const AggregateFunction* percentile_fn() {
    return get_aggregate_function("percentile_approx", TYPE_DOUBLE, TYPE_DOUBLE, /*is_nullable=*/false);
}

// A column of n pseudo-random values, each a distinct group key in pass-through.
static ColumnPtr make_values(size_t n) {
    auto col = DoubleColumn::create();
    std::mt19937 rng(0xC0FFEE);
    std::uniform_real_distribution<double> dist(0.0, 1.0e6);
    for (size_t i = 0; i < n; ++i) {
        col->append(dist(rng));
    }
    return ColumnPtr(std::move(col));
}

static std::unique_ptr<FunctionContext> make_ctx(RuntimeState* rs) {
    std::vector<TypeDescriptor> arg_types{TypeDescriptor::from_logical_type(TYPE_DOUBLE),
                                          TypeDescriptor::from_logical_type(TYPE_DOUBLE)};
    auto ret = TypeDescriptor::from_logical_type(TYPE_DOUBLE);
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context(std::move(arg_types), ret));
    Columns const_columns{ColumnHelper::create_const_column<TYPE_DOUBLE>(0, 1),
                          ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1)};
    ctx->set_constant_columns(std::move(const_columns));
    ctx->set_runtime_state(rs);
    return ctx;
}

static void run_convert(benchmark::State& state, bool compact) {
    CountingAllocatorWithHook allocator;
    tls_agg_state_allocator = &allocator;
    const size_t n = state.range(0);
    const AggregateFunction* fn = percentile_fn();

    TQueryOptions opts;
    opts.__set_enable_percentile_compact_intermediate(compact);
    RuntimeState rs(TUniqueId(), opts, TQueryGlobals(), nullptr);
    auto ctx = make_ctx(&rs);
    auto quantile = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1);
    Columns src{make_values(n), quantile};

    size_t bytes = 0;
    for (auto _ : state) {
        MutableColumnPtr out = BinaryColumn::create();
        fn->convert_to_serialize_format(ctx.get(), src, n, out);
        bytes = down_cast<BinaryColumn*>(out.get())->get_bytes().size();
        benchmark::DoNotOptimize(out);
    }
    state.counters["bytes_per_chunk"] = bytes;
    state.counters["rows_per_s"] =
            benchmark::Counter(static_cast<double>(n) * state.iterations(), benchmark::Counter::kIsRate);
    tls_agg_state_allocator = nullptr;
}

static void run_roundtrip(benchmark::State& state, bool compact) {
    CountingAllocatorWithHook allocator;
    tls_agg_state_allocator = &allocator;
    const size_t n = state.range(0);
    const AggregateFunction* fn = percentile_fn();

    TQueryOptions opts;
    opts.__set_enable_percentile_compact_intermediate(compact);
    RuntimeState rs(TUniqueId(), opts, TQueryGlobals(), nullptr);
    auto ctx = make_ctx(&rs);
    auto quantile = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.5, 1);
    Columns src{make_values(n), quantile};

    for (auto _ : state) {
        MutableColumnPtr out = BinaryColumn::create();
        fn->convert_to_serialize_format(ctx.get(), src, n, out);

        MemPool pool;
        AggDataPtr agg_state = pool.allocate_aligned(fn->size(), fn->alignof_size());
        fn->create(ctx.get(), agg_state);
        for (size_t i = 0; i < out->size(); ++i) {
            fn->merge(ctx.get(), out.get(), agg_state, i);
        }
        fn->destroy(ctx.get(), agg_state);
        benchmark::DoNotOptimize(out);
    }
    state.counters["rows_per_s"] =
            benchmark::Counter(static_cast<double>(n) * state.iterations(), benchmark::Counter::kIsRate);
    tls_agg_state_allocator = nullptr;
}

static void BM_PercentileConvert_Legacy(benchmark::State& state) {
    run_convert(state, /*compact=*/false);
}
static void BM_PercentileConvert_Compact(benchmark::State& state) {
    run_convert(state, /*compact=*/true);
}
static void BM_PercentileRoundtrip_Legacy(benchmark::State& state) {
    run_roundtrip(state, /*compact=*/false);
}
static void BM_PercentileRoundtrip_Compact(benchmark::State& state) {
    run_roundtrip(state, /*compact=*/true);
}

BENCHMARK(BM_PercentileConvert_Legacy)->Arg(1024)->Arg(4096);
BENCHMARK(BM_PercentileConvert_Compact)->Arg(1024)->Arg(4096);
BENCHMARK(BM_PercentileRoundtrip_Legacy)->Arg(1024)->Arg(4096);
BENCHMARK(BM_PercentileRoundtrip_Compact)->Arg(1024)->Arg(4096);

} // namespace starrocks

BENCHMARK_MAIN();
