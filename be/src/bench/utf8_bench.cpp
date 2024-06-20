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
#include "util/random_utf8.h"
#include "util/utf8_check.h"

namespace starrocks {

static std::map<int, std::vector<uint8_t>> random_data;

static void run_validate_utf8(const char* data, int len) {
    (void)simdutf::validate_utf8(data, len);
}

static void run_validate_utf8_sr(const char* data, int len) {
    (void)starrocks::validate_utf8(data, len);
}

static void DoSetup(const benchmark::State& state) {
    uint32_t seed{1234};
    simdutf::tests::helpers::random_utf8 random(seed, 1, 1, 1, 1);
    std::vector<int> random_length{1 << 6, 1 << 10, 1 << 12, 1 << 14, 1 << 16, 1 << 18, 1 << 20};
    for (auto len : random_length) {
        random_data[len] = random.generate(len);
    }
}
static std::map<int, std::function<void(const char*, int)>> entrypoints = {{0, run_validate_utf8},
                                                                           {1, run_validate_utf8_sr}};

static void BM_UTF8Bench_Arg(benchmark::internal::Benchmark* b) {
    b->Args({0, 1 << 6});
    b->Args({1, 1 << 6});
    b->Args({0, 1 << 10});
    b->Args({1, 1 << 10});
    b->Args({0, 1 << 12});
    b->Args({1, 1 << 12});
    b->Args({0, 1 << 14});
    b->Args({1, 1 << 14});
    b->Args({0, 1 << 16});
    b->Args({1, 1 << 16});
    b->Args({0, 1 << 18});
    b->Args({1, 1 << 18});
    b->Args({0, 1 << 20});
    b->Args({1, 1 << 20});
    b->Iterations(10000);
}

static void BM_UTF8Bench(benchmark::State& state) {
    if (state.thread_index == 0) {
        DoSetup(state);
    }
    int key = state.range(0);
    int len = state.range(1);
    auto& func = entrypoints[key];
    auto& data = random_data[len];
    if (!func) {
        return;
    }

    for (auto _ : state) {
        func(reinterpret_cast<const char*>(data.data()), data.size());
    }
}

BENCHMARK(BM_UTF8Bench)->Apply(BM_UTF8Bench_Arg);

} // namespace starrocks

BENCHMARK_MAIN();
