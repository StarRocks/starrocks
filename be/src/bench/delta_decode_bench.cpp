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

#include "base/simd/delta_decode.h"

namespace starrocks {

#ifdef __AVX512F__

static void BM_int32_avx512_prefix_sum(benchmark::State& state) {
    int64_t size = state.range(0);
    std::vector<int32_t> elements(size);
    int32_t last_value = 0;
    for (auto _ : state) {
        delta_decode_chain_int32_avx512(elements.data(), size, 0, last_value);
    }
}
BENCHMARK(BM_int32_avx512_prefix_sum)->RangeMultiplier(2)->Range(4 * 1024, 32 * 1024);

#endif

#ifdef __AVX2__
static void BM_int32_avx2_prefix_sum(benchmark::State& state) {
    int64_t size = state.range(0);
    std::vector<int32_t> elements(size);
    int32_t last_value = 0;
    for (auto _ : state) {
        delta_decode_chain_int32_avx2(elements.data(), size, 0, last_value);
    }
}
BENCHMARK(BM_int32_avx2_prefix_sum)->RangeMultiplier(2)->Range(4 * 1024, 32 * 1024);
#endif

#if defined(__AVX512F__) && defined(__AVX512VL__)
static void BM_int32_avx2x_prefix_sum(benchmark::State& state) {
    int64_t size = state.range(0);
    std::vector<int32_t> elements(size);
    int32_t last_value = 0;
    for (auto _ : state) {
        delta_decode_chain_int32_avx2x(elements.data(), size, 0, last_value);
    }
}
BENCHMARK(BM_int32_avx2x_prefix_sum)->RangeMultiplier(2)->Range(4 * 1024, 32 * 1024);

#endif

static void BM_int32_native_prefix_sum(benchmark::State& state) {
    int64_t size = state.range(0);
    std::vector<int32_t> elements(size);
    int32_t last_value = 0;
    for (auto _ : state) {
        delta_decode_chain_scalar_prefetch<int32_t>(elements.data(), size, 0, last_value);
    }
}

#ifdef __AVX512F__
static void BM_int64_avx512_prefix_sum(benchmark::State& state) {
    int64_t size = state.range(0);
    std::vector<int64_t> elements(size);
    int64_t last_value = 0;
    for (auto _ : state) {
        delta_decode_chain_int64_avx512(elements.data(), size, 0, last_value);
    }
}
BENCHMARK(BM_int64_avx512_prefix_sum)->RangeMultiplier(2)->Range(4 * 1024, 32 * 1024);

#endif

static void BM_int64_native_prefix_sum(benchmark::State& state) {
    int64_t size = state.range(0);
    std::vector<int64_t> elements(size);
    int64_t last_value = 0;
    for (auto _ : state) {
        delta_decode_chain_scalar_prefetch<int64_t>(elements.data(), size, 0, last_value);
    }
}

BENCHMARK(BM_int32_native_prefix_sum)->RangeMultiplier(2)->Range(4 * 1024, 32 * 1024);
BENCHMARK(BM_int64_native_prefix_sum)->RangeMultiplier(2)->Range(4 * 1024, 32 * 1024);

} // namespace starrocks

BENCHMARK_MAIN();
