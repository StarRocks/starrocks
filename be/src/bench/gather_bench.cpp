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

#include <cstddef>
#include <cstdint>
#include <random>
#include <vector>

#include "base/simd/gather.h"

namespace starrocks {

namespace {

// The source pool size is a benchmark argument (state.range(1)). 4M elements is much larger than L2
// but may stay in L3; 64M elements is DRAM-bound. Index values are always < pool_size.
constexpr int64_t kSmallPool = 4 * 1024 * 1024;
constexpr int64_t kBigPool = 64 * 1024 * 1024;

template <typename T>
std::vector<T> make_source(size_t pool_size) {
    std::vector<T> src(pool_size);
    for (size_t i = 0; i < pool_size; ++i) {
        src[i] = static_cast<T>(i * 17 + 1);
    }
    return src;
}

// Verifies dest[i] == src[indexes[i]] once, outside the timed loop.
template <typename T>
bool self_check(const std::vector<T>& dest, const std::vector<T>& src, const std::vector<uint32_t>& indexes) {
    for (size_t i = 0; i < indexes.size(); ++i) {
        if (dest[i] != src[indexes[i]]) {
            return false;
        }
    }
    return true;
}

template <typename T>
void run_gather(benchmark::State& state, const std::vector<T>& src, const std::vector<uint32_t>& indexes) {
    const size_t num_rows = indexes.size();
    std::vector<T> dest(num_rows);

    SIMDGather::gather(dest.data(), src.data(), indexes.data(), num_rows);
    if (!self_check(dest, src, indexes)) {
        state.SkipWithError("gathered value mismatch");
        return;
    }

    for (auto _ : state) {
        SIMDGather::gather(dest.data(), src.data(), indexes.data(), num_rows);
        benchmark::DoNotOptimize(dest.data());
        benchmark::ClobberMemory();
    }

    state.SetBytesProcessed(state.iterations() * num_rows * sizeof(T));
    state.SetItemsProcessed(state.iterations() * num_rows);
}

} // namespace

template <typename T>
static void BM_Gather_Random(benchmark::State& state) {
    const size_t num_rows = state.range(0);
    const size_t pool_size = state.range(1);
    std::vector<T> src = make_source<T>(pool_size);

    std::mt19937 rng(12345);
    std::uniform_int_distribution<uint32_t> dist(0, static_cast<uint32_t>(pool_size - 1));
    std::vector<uint32_t> indexes(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        indexes[i] = dist(rng);
    }
    run_gather<T>(state, src, indexes);
}

template <typename T>
static void BM_Gather_Strided(benchmark::State& state) {
    const size_t num_rows = state.range(0);
    const size_t pool_size = state.range(1);
    std::vector<T> src = make_source<T>(pool_size);

    std::vector<uint32_t> indexes(num_rows);
    for (size_t i = 0; i < num_rows; ++i) {
        indexes[i] = static_cast<uint32_t>((i * 13) % pool_size);
    }
    run_gather<T>(state, src, indexes);
}

// Args are {num_rows, pool_size}.
#define STARROCKS_GATHER_SIZES \
    ArgsProduct({{1024, 8192, 65536, 1048576}, {kSmallPool, kBigPool}})->ArgNames({"rows", "pool"})

BENCHMARK_TEMPLATE(BM_Gather_Random, int32_t)->STARROCKS_GATHER_SIZES;
BENCHMARK_TEMPLATE(BM_Gather_Random, int64_t)->STARROCKS_GATHER_SIZES;
BENCHMARK_TEMPLATE(BM_Gather_Strided, int32_t)->STARROCKS_GATHER_SIZES;
BENCHMARK_TEMPLATE(BM_Gather_Strided, int64_t)->STARROCKS_GATHER_SIZES;

} // namespace starrocks

BENCHMARK_MAIN();
