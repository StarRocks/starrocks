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

// Benchmark comparing the strict-mode (AllowThrowException) overflow-checked cast unary function
// implementations:
//
//   * OLD: VectorizedInputCheckUnaryFunction<OP, CheckThrow>
//          - DealNullableColumnUnaryFunction strips the null column, then
//            ProduceNullUnaryFunction runs OP AND the check on EVERY row (including null rows).
//
//   * NEW: NullAwareInputCheckUnaryFunction<OP, CheckThrow, CheckNoThrow>
//          - keeps the null column; the hot loop stays branchless (convert every row, detect
//            overflow with the NON-throwing check AND-ed with the not-null mask and OR-accumulated)
//            so it vectorizes; the throwing check runs only on a cold path entered when a genuine
//            non-null row overflowed. It also avoids materializing/scanning a separate NULL mask
//            column the way the old ProduceNullUnaryFunction path does.
//
// The goal is to confirm the new implementation does not regress against the old one. All input
// values are kept inside the INT range so neither implementation throws (a fair, throw-free
// comparison of the per-row work).
//
// Results (Release, single core; cast BIGINT -> INT, 4096 rows):
//
//   --------------------------------------------------------------------------------------
//   Benchmark                            Time             CPU   Iterations UserCounters...
//   --------------------------------------------------------------------------------------
//   BM_New_NonNullable/4096           2870 ns         2870 ns       243895 items_per_second=1.42707G/s
//   BM_Old_NonNullable/4096           4023 ns         4023 ns       173994 items_per_second=1.01807G/s
//   BM_New_Nullable_NoNull/4096       3193 ns         3193 ns       219406 items_per_second=1.28277G/s
//   BM_Old_Nullable_NoNull/4096       4434 ns         4433 ns       157783 items_per_second=923.89M/s
//   BM_New_Nullable_30/4096           2719 ns         2719 ns       257483 items_per_second=1.50636G/s
//   BM_Old_Nullable_30/4096           4408 ns         4408 ns       159118 items_per_second=929.254M/s
//   BM_New_Nullable_90/4096           2719 ns         2719 ns       257638 items_per_second=1.50662G/s
//   BM_Old_Nullable_90/4096           4413 ns         4412 ns       158922 items_per_second=928.291M/s
//
// The NEW implementation is ~1.4-1.6x faster than OLD across every shape, and its nullable-with-
// nulls cases are independent of the null ratio (no branch misprediction). Note the with-nulls
// case (2719 ns) even beats the non-nullable case (2870 ns): the with-nulls hot loop uses the
// non-throwing check and vectorizes, whereas the non-nullable path still inlines the throwing
// check, which blocks vectorization.

#include <benchmark/benchmark.h>

#include <random>

#include "util/numeric_types.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "exprs/unary_function.h"

namespace starrocks {

// Mirror cast_expr.cpp's ImplicitToNumber / NumberCheck / NumberCheckWithThrowException so the
// benchmark exercises the same per-row work as a real cast(BIGINT AS INT).
DEFINE_UNARY_FN_WITH_IMPL(BenchImplicitToNumber, value) {
    return value;
}

DEFINE_UNARY_FN_WITH_IMPL(BenchNumberCheck, value) {
    return check_signed_number_overflow<Type, ResultType>(value);
}

DEFINE_UNARY_FN_WITH_IMPL(BenchNumberCheckThrow, value) {
    const auto overflow = BenchNumberCheck::apply<Type, ResultType>(value);
    if (overflow) {
        throw std::runtime_error("cast overflow");
    }
    return overflow;
}

using OldCastThrow = VectorizedInputCheckUnaryFunction<BenchImplicitToNumber, BenchNumberCheckThrow>;
using NewCastThrow = NullAwareInputCheckUnaryFunction<BenchImplicitToNumber, BenchNumberCheckThrow, BenchNumberCheck>;

// Build a BIGINT column of `n` rows. When `nullable` is false a plain column is returned;
// otherwise a NullableColumn with `null_percent`% of its rows flagged null. Every data slot
// (including null rows) holds an in-INT-range value so the old implementation, which checks all
// rows, never throws.
static ColumnPtr make_input(int n, bool nullable, int null_percent) {
    auto data_col = RunTimeColumnType<TYPE_BIGINT>::create();
    auto& data = data_col->get_data();
    data.resize(n);

    std::mt19937 rng(0x9E3779B9u);
    std::uniform_int_distribution<int64_t> value_dist(-1000, 1000);

    if (!nullable) {
        for (int i = 0; i < n; ++i) {
            data[i] = value_dist(rng);
        }
        return data_col;
    }

    auto null_col = NullColumn::create();
    auto& nulls = null_col->get_data();
    nulls.resize(n);

    std::uniform_int_distribution<int> coin(0, 99);
    bool any_null = false;
    for (int i = 0; i < n; ++i) {
        data[i] = value_dist(rng);
        if (null_percent > 0 && coin(rng) < null_percent) {
            nulls[i] = 1;
            any_null = true;
        } else {
            nulls[i] = 0;
        }
    }

    auto nullable_col = NullableColumn::create(std::move(data_col), std::move(null_col));
    nullable_col->set_has_null(any_null);
    return nullable_col;
}

template <typename FN>
static void run(benchmark::State& state, const ColumnPtr& input) {
    size_t items = 0;
    for (auto _ : state) {
        ColumnPtr result = FN::template evaluate<TYPE_BIGINT, TYPE_INT>(input);
        benchmark::DoNotOptimize(result);
        items += input->size();
    }
    state.SetItemsProcessed(items);
}

// ---- non-nullable input ----
static void BM_Old_NonNullable(benchmark::State& state) {
    run<OldCastThrow>(state, make_input(state.range(0), /*nullable=*/false, 0));
}
static void BM_New_NonNullable(benchmark::State& state) {
    run<NewCastThrow>(state, make_input(state.range(0), /*nullable=*/false, 0));
}

// ---- nullable input, but no actual null (exercises the has_null() fast path) ----
static void BM_Old_Nullable_NoNull(benchmark::State& state) {
    run<OldCastThrow>(state, make_input(state.range(0), /*nullable=*/true, 0));
}
static void BM_New_Nullable_NoNull(benchmark::State& state) {
    run<NewCastThrow>(state, make_input(state.range(0), /*nullable=*/true, 0));
}

// ---- nullable input, 30% null ----
static void BM_Old_Nullable_30(benchmark::State& state) {
    run<OldCastThrow>(state, make_input(state.range(0), /*nullable=*/true, 30));
}
static void BM_New_Nullable_30(benchmark::State& state) {
    run<NewCastThrow>(state, make_input(state.range(0), /*nullable=*/true, 30));
}

// ---- nullable input, 90% null. The new impl converts/checks every row regardless of the null
//      ratio (branchless), so this should match the 30% case and confirm null-ratio independence. ----
static void BM_Old_Nullable_90(benchmark::State& state) {
    run<OldCastThrow>(state, make_input(state.range(0), /*nullable=*/true, 90));
}
static void BM_New_Nullable_90(benchmark::State& state) {
    run<NewCastThrow>(state, make_input(state.range(0), /*nullable=*/true, 90));
}

static constexpr int kRows = 4096;

BENCHMARK(BM_New_NonNullable)->Arg(kRows);
BENCHMARK(BM_Old_NonNullable)->Arg(kRows);
BENCHMARK(BM_New_Nullable_NoNull)->Arg(kRows);
BENCHMARK(BM_Old_Nullable_NoNull)->Arg(kRows);
BENCHMARK(BM_New_Nullable_30)->Arg(kRows);
BENCHMARK(BM_Old_Nullable_30)->Arg(kRows);
BENCHMARK(BM_New_Nullable_90)->Arg(kRows);
BENCHMARK(BM_Old_Nullable_90)->Arg(kRows);

} // namespace starrocks

BENCHMARK_MAIN();