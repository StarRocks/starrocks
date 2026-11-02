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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <new>
#include <random>
#include <utility>
#include <vector>

#include "base/simd/gather.h"

namespace starrocks {

namespace {

// The source pool size is a benchmark argument (state.range(1)). 4M elements is much larger than L2
// but may stay in L3; 64M elements is DRAM-bound. Index values are always < pool_size.
constexpr int64_t kSmallPool = 4 * 1024 * 1024;
constexpr int64_t kBigPool = 64 * 1024 * 1024;

// Pre-generate at least 4M indexes (16 MiB of uint32_t) and rotate through disjoint num_rows slices
// across timed iterations so small-batch runs do not stay warm in L1/L2/LLC after the untimed self-check.
constexpr size_t kMinIndexRingSize = 4 * 1024 * 1024;

size_t index_ring_size(size_t num_rows) {
    const size_t min_slices = std::max<size_t>(2, (kMinIndexRingSize + num_rows - 1) / num_rows);
    return min_slices * num_rows;
}

template <typename T>
std::vector<T> make_source(size_t pool_size) {
    std::vector<T> src(pool_size);
    for (size_t i = 0; i < pool_size; ++i) {
        src[i] = static_cast<T>(i * 17 + 1);
    }
    return src;
}

// Pools are immutable and identical for a given (type, size); build each once per process.
template <typename T>
const std::vector<T>& cached_source(size_t pool_size) {
    static std::map<size_t, std::vector<T>> cache;
    auto it = cache.find(pool_size);
    if (it == cache.end()) {
        it = cache.emplace(pool_size, make_source<T>(pool_size)).first;
    }
    return it->second;
}

const std::vector<uint32_t>& cached_random_indexes(size_t pool_size, size_t num_rows) {
    const size_t ring_size = index_ring_size(num_rows);
    static std::map<std::pair<size_t, size_t>, std::vector<uint32_t>> cache;
    auto key = std::make_pair(pool_size, ring_size);
    auto it = cache.find(key);
    if (it == cache.end()) {
        std::mt19937 rng(12345);
        std::uniform_int_distribution<uint32_t> dist(0, static_cast<uint32_t>(pool_size - 1));
        std::vector<uint32_t> indexes(ring_size);
        for (size_t i = 0; i < ring_size; ++i) {
            indexes[i] = dist(rng);
        }
        it = cache.emplace(key, std::move(indexes)).first;
    }
    return it->second;
}

const std::vector<uint32_t>& cached_strided_indexes(size_t pool_size, size_t num_rows) {
    const size_t ring_size = index_ring_size(num_rows);
    static std::map<std::pair<size_t, size_t>, std::vector<uint32_t>> cache;
    auto key = std::make_pair(pool_size, ring_size);
    auto it = cache.find(key);
    if (it == cache.end()) {
        std::vector<uint32_t> indexes(ring_size);
        for (size_t i = 0; i < ring_size; ++i) {
            indexes[i] = static_cast<uint32_t>((i * 13) % pool_size);
        }
        it = cache.emplace(key, std::move(indexes)).first;
    }
    return it->second;
}

// Verifies dest[i] == src[indexes[i]] once, outside the timed loop.
template <typename T>
bool self_check(const std::vector<T>& dest, const std::vector<T>& src, const uint32_t* indexes, size_t num_rows) {
    for (size_t i = 0; i < num_rows; ++i) {
        if (dest[i] != src[indexes[i]]) {
            return false;
        }
    }
    return true;
}

template <typename T>
void run_gather(benchmark::State& state, const std::vector<T>& src, const std::vector<uint32_t>& index_ring,
                size_t num_rows) {
    if (num_rows == 0 || index_ring.size() < num_rows) {
        state.SkipWithError("invalid index ring size for num_rows");
        return;
    }
    std::vector<T> dest(num_rows);

    SIMDGather::gather(dest.data(), src.data(), index_ring.data(), num_rows);
    if (!self_check(dest, src, index_ring.data(), num_rows)) {
        state.SkipWithError("gathered value mismatch");
        return;
    }

    const size_t ring_size = index_ring.size();
    size_t offset = (num_rows < ring_size) ? num_rows : 0;
    for (auto _ : state) {
        const uint32_t* indexes = index_ring.data() + offset;
        SIMDGather::gather(dest.data(), src.data(), indexes, num_rows);
        benchmark::DoNotOptimize(dest.data());
        benchmark::ClobberMemory();
        offset += num_rows;
        if (offset + num_rows > ring_size) {
            offset = 0;
        }
    }

    state.SetBytesProcessed(state.iterations() * num_rows * sizeof(T));
    state.SetItemsProcessed(state.iterations() * num_rows);
}

} // namespace

template <typename T>
static void BM_Gather_Random(benchmark::State& state) {
    const size_t num_rows = state.range(0);
    const size_t pool_size = state.range(1);
    if (num_rows == 0 || pool_size == 0 || pool_size - 1 > std::numeric_limits<uint32_t>::max()) {
        state.SkipWithError("benchmark arguments out of range for uint32 indexes");
        return;
    }
    const std::vector<T>* src;
    const std::vector<uint32_t>* index_ring;
    try {
        src = &cached_source<T>(pool_size);
        index_ring = &cached_random_indexes(pool_size, num_rows);
    } catch (const std::bad_alloc&) {
        state.SkipWithError("out of memory building benchmark pools");
        return;
    }

    run_gather<T>(state, *src, *index_ring, num_rows);
}

template <typename T>
static void BM_Gather_Strided(benchmark::State& state) {
    const size_t num_rows = state.range(0);
    const size_t pool_size = state.range(1);
    if (num_rows == 0 || pool_size == 0 || pool_size - 1 > std::numeric_limits<uint32_t>::max()) {
        state.SkipWithError("benchmark arguments out of range for uint32 indexes");
        return;
    }
    const std::vector<T>* src;
    const std::vector<uint32_t>* index_ring;
    try {
        src = &cached_source<T>(pool_size);
        index_ring = &cached_strided_indexes(pool_size, num_rows);
    } catch (const std::bad_alloc&) {
        state.SkipWithError("out of memory building benchmark pools");
        return;
    }

    run_gather<T>(state, *src, *index_ring, num_rows);
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
