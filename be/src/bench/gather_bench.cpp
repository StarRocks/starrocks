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

#include <cstdint>
#include <numeric>
#include <random>
#include <vector>

#include "base/simd/gather.h"

namespace starrocks {

template <typename T>
static void BM_Gather_Random(benchmark::State& state) {
    const size_t num_rows = state.range(0);
    constexpr size_t pool_size = 1024 * 1024; // 1M elements (~4MB or 8MB, exercises L3/DRAM)

    std::vector<T> src(pool_size);
    for (size_t i = 0; i < pool_size; ++i) {
        src[i] = static_cast<T>(i * 17 + 1);
    }

    std::mt19937 rng(12345);
    std::uniform_int_distribution<uint32_t> dist(0, pool_size - 1);
    std::vector<uint32_t> indexes(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        indexes[i] = dist(rng);
    }

    std::vector<T> dest(num_rows);

    for (auto _ : state) {
        SIMDGather::gather(dest.data(), src.data(), indexes.data(), num_rows);
        benchmark::DoNotOptimize(dest.data());
    }

    state.SetBytesProcessed(state.iterations() * num_rows * sizeof(T));
    state.SetItemsProcessed(state.iterations() * num_rows);
}

template <typename T>
static void BM_Gather_Strided(benchmark::State& state) {
    const size_t num_rows = state.range(0);
    constexpr size_t pool_size = 1024 * 1024;

    std::vector<T> src(pool_size);
    for (size_t i = 0; i < pool_size; ++i) {
        src[i] = static_cast<T>(i * 17 + 1);
    }

    std::vector<uint32_t> indexes(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        indexes[i] = static_cast<uint32_t>((i * 13) % pool_size);
    }

    std::vector<T> dest(num_rows);

    for (auto _ : state) {
        SIMDGather::gather(dest.data(), src.data(), indexes.data(), num_rows);
        benchmark::DoNotOptimize(dest.data());
    }

    state.SetBytesProcessed(state.iterations() * num_rows * sizeof(T));
    state.SetItemsProcessed(state.iterations() * num_rows);
}

template <typename T>
static void BM_Gather_Filtered(benchmark::State& state) {
    const size_t num_rows = state.range(0);
    const int sel_pct = state.range(1);
    constexpr size_t pool_size = 1024 * 1024;

    std::vector<T> src(pool_size);
    for (size_t i = 0; i < pool_size; ++i) {
        src[i] = static_cast<T>(i * 17 + 1);
    }

    std::mt19937 rng(54321);
    std::uniform_int_distribution<uint32_t> dist(0, pool_size - 1);
    std::vector<uint32_t> indexes(num_rows);
    std::vector<uint8_t> is_filtered(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        indexes[i] = dist(rng);
        is_filtered[i] = ((i * 37 + sel_pct) % 100 < sel_pct) ? 0 : 1;
    }

    std::vector<T> dest(num_rows);

    for (auto _ : state) {
        SIMDGather::gather(dest.data(), src.data(), indexes.data(), is_filtered.data(), num_rows);
        benchmark::DoNotOptimize(dest.data());
    }

    state.SetBytesProcessed(state.iterations() * num_rows * sizeof(T));
    state.SetItemsProcessed(state.iterations() * num_rows);
}

BENCHMARK_TEMPLATE(BM_Gather_Random, int32_t)->RangeMultiplier(4)->Range(4096, 65536);
BENCHMARK_TEMPLATE(BM_Gather_Random, int64_t)->RangeMultiplier(4)->Range(4096, 65536);

BENCHMARK_TEMPLATE(BM_Gather_Strided, int32_t)->RangeMultiplier(4)->Range(4096, 65536);
BENCHMARK_TEMPLATE(BM_Gather_Strided, int64_t)->RangeMultiplier(4)->Range(4096, 65536);

BENCHMARK_TEMPLATE(BM_Gather_Filtered, int32_t)->ArgsProduct({{4096, 16384}, {10, 30, 50, 70, 90}});

} // namespace starrocks

BENCHMARK_MAIN();
