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

#include <benchmark/benchmark.h>

#include "bench.h"
#include "simdutf.h"
#include "util/utf8_check.h"

namespace starrocks {

static void run_validate_utf8() {
    (void)simdutf::validate_utf8(nullptr, 0);
}

static void run_validate_utf8_sr() {
    (void)starrocks::validate_utf8(nullptr, 0);
}

static std::map<int, std::function<void()>> entrypoints = {{0, run_validate_utf8}, {1, run_validate_utf8_sr}};

static void BM_UTF8Bench_Arg(benchmark::internal::Benchmark* b) {
    b->Args({0});
    b->Args({1});
    b->Iterations(10000);
}

static void BM_UTF8Bench(benchmark::State& state) {
    int key = state.range(0);
    std::function<void()> func = entrypoints[key];
    if (!func) {
        return;
    }

    for (auto _ : state) {
        func();
    }
}

BENCHMARK(BM_UTF8Bench)->Apply(BM_UTF8Bench_Arg);

} // namespace starrocks

BENCHMARK_MAIN();
