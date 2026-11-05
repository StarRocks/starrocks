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

#ifdef __AVX2__
#include <emmintrin.h>
#include <immintrin.h>
#endif

#if defined(__x86_64__) || defined(_M_X64)
#include <xmmintrin.h>
#endif

#include "base/simd/gather.h"

// Read prefetch into all cache levels. Never faults, even for invalid addresses.
#ifndef STARROCKS_PREFETCH
#if defined(__x86_64__) || defined(_M_X64)
#define STARROCKS_PREFETCH(addr) _mm_prefetch(reinterpret_cast<const char*>(addr), _MM_HINT_T0)
#elif defined(__aarch64__) || defined(__arm__)
#define STARROCKS_PREFETCH(addr) __builtin_prefetch(static_cast<const void*>(addr), 0, 3)
#elif defined(__GNUC__) || defined(__clang__)
#define STARROCKS_PREFETCH(addr) __builtin_prefetch(static_cast<const void*>(addr), 0, 3)
#else
#define STARROCKS_PREFETCH(addr) ((void)0)
#endif
#endif

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
inline void gather_unrolled(T* __restrict__ dest, const T* __restrict__ src, const uint32_t* __restrict__ indexes,
                            size_t num_rows) {
    size_t i = 0;
    for (; i + 8 <= num_rows; i += 8) {
        T v0 = src[indexes[i + 0]];
        T v1 = src[indexes[i + 1]];
        T v2 = src[indexes[i + 2]];
        T v3 = src[indexes[i + 3]];
        T v4 = src[indexes[i + 4]];
        T v5 = src[indexes[i + 5]];
        T v6 = src[indexes[i + 6]];
        T v7 = src[indexes[i + 7]];

        dest[i + 0] = v0;
        dest[i + 1] = v1;
        dest[i + 2] = v2;
        dest[i + 3] = v3;
        dest[i + 4] = v4;
        dest[i + 5] = v5;
        dest[i + 6] = v6;
        dest[i + 7] = v7;
    }
    for (; i < num_rows; ++i) {
        dest[i] = src[indexes[i]];
    }
}

template <typename T, size_t Lookahead>
inline void gather_prefetch(T* __restrict__ dest, const T* __restrict__ src, const uint32_t* __restrict__ indexes,
                            size_t num_rows) {
    for (size_t p = 0; p < Lookahead && p < num_rows; ++p) {
        STARROCKS_PREFETCH(src + indexes[p]);
    }
    size_t i = 0;
    static constexpr size_t kStep = (Lookahead >= 8) ? 8 : 4;
    if constexpr (kStep == 8) {
        for (; i + Lookahead + 8 <= num_rows; i += 8) {
            STARROCKS_PREFETCH(src + indexes[i + Lookahead + 0]);
            STARROCKS_PREFETCH(src + indexes[i + Lookahead + 1]);
            STARROCKS_PREFETCH(src + indexes[i + Lookahead + 2]);
            STARROCKS_PREFETCH(src + indexes[i + Lookahead + 3]);
            STARROCKS_PREFETCH(src + indexes[i + Lookahead + 4]);
            STARROCKS_PREFETCH(src + indexes[i + Lookahead + 5]);
            STARROCKS_PREFETCH(src + indexes[i + Lookahead + 6]);
            STARROCKS_PREFETCH(src + indexes[i + Lookahead + 7]);

            T v0 = src[indexes[i + 0]];
            T v1 = src[indexes[i + 1]];
            T v2 = src[indexes[i + 2]];
            T v3 = src[indexes[i + 3]];
            T v4 = src[indexes[i + 4]];
            T v5 = src[indexes[i + 5]];
            T v6 = src[indexes[i + 6]];
            T v7 = src[indexes[i + 7]];

            dest[i + 0] = v0;
            dest[i + 1] = v1;
            dest[i + 2] = v2;
            dest[i + 3] = v3;
            dest[i + 4] = v4;
            dest[i + 5] = v5;
            dest[i + 6] = v6;
            dest[i + 7] = v7;
        }
    } else {
        for (; i + Lookahead + 4 <= num_rows; i += 4) {
            STARROCKS_PREFETCH(src + indexes[i + Lookahead + 0]);
            STARROCKS_PREFETCH(src + indexes[i + Lookahead + 1]);
            STARROCKS_PREFETCH(src + indexes[i + Lookahead + 2]);
            STARROCKS_PREFETCH(src + indexes[i + Lookahead + 3]);

            T v0 = src[indexes[i + 0]];
            T v1 = src[indexes[i + 1]];
            T v2 = src[indexes[i + 2]];
            T v3 = src[indexes[i + 3]];

            dest[i + 0] = v0;
            dest[i + 1] = v1;
            dest[i + 2] = v2;
            dest[i + 3] = v3;
        }
    }
    for (; i < num_rows; ++i) {
        dest[i] = src[indexes[i]];
    }
}

template <typename T>
inline void gather_avx2(T* __restrict__ dest, const T* __restrict__ src, const uint32_t* __restrict__ indexes,
                        size_t num_rows) {
#ifdef __AVX2__
    if constexpr (sizeof(T) == 4) {
        size_t i = 0;
        for (; i + 8 <= num_rows; i += 8) {
            __m256i idx = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(indexes + i));
            __m256i gathered = _mm256_i32gather_epi32(reinterpret_cast<const int*>(src), idx, 4);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), gathered);
        }
        for (; i < num_rows; ++i) {
            dest[i] = src[indexes[i]];
        }
        _mm256_zeroupper();
    } else if constexpr (sizeof(T) == 8) {
        size_t i = 0;
        for (; i + 4 <= num_rows; i += 4) {
            __m128i idx = _mm_loadu_si128(reinterpret_cast<const __m128i*>(indexes + i));
            __m256i gathered = _mm256_i32gather_epi64(reinterpret_cast<const long long int*>(src), idx, 8);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), gathered);
        }
        for (; i < num_rows; ++i) {
            dest[i] = src[indexes[i]];
        }
        _mm256_zeroupper();
    } else {
        for (size_t i = 0; i < num_rows; ++i) {
            dest[i] = src[indexes[i]];
        }
    }
#else
    for (size_t i = 0; i < num_rows; ++i) {
        dest[i] = src[indexes[i]];
    }
#endif
}

template <typename T, typename GatherFn>
void run_gather_fn(benchmark::State& state, const std::vector<T>& src, const std::vector<uint32_t>& index_ring,
                   size_t num_rows, GatherFn&& gather_fn) {
    if (num_rows == 0 || index_ring.size() < num_rows) {
        state.SkipWithError("invalid index ring size for num_rows");
        return;
    }
    std::vector<T> dest(num_rows);

    gather_fn(dest.data(), src.data(), index_ring.data(), num_rows);
    if (!self_check(dest, src, index_ring.data(), num_rows)) {
        state.SkipWithError("gathered value mismatch");
        return;
    }

    const size_t ring_size = index_ring.size();
    size_t offset = (num_rows < ring_size) ? num_rows : 0;
    for (auto _ : state) {
        const uint32_t* indexes = index_ring.data() + offset;
        gather_fn(dest.data(), src.data(), indexes, num_rows);
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
static void BM_Gather_Baseline(benchmark::State& state) {
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

    run_gather_fn(state, *src, *index_ring, num_rows,
                  [](T* d, const T* s, const uint32_t* idx, size_t n) { SIMDGather::gather(d, s, idx, n); });
}

template <typename T>
static void BM_Gather_Unrolled(benchmark::State& state) {
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

    run_gather_fn(state, *src, *index_ring, num_rows, gather_unrolled<T>);
}

template <typename T, size_t Lookahead>
static void BM_Gather_Prefetch(benchmark::State& state) {
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

    run_gather_fn(state, *src, *index_ring, num_rows, gather_prefetch<T, Lookahead>);
}

template <typename T>
static void BM_Gather_AVX2(benchmark::State& state) {
#ifndef __AVX2__
    state.SkipWithError("AVX2 not supported on this platform");
#else
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

    run_gather_fn(state, *src, *index_ring, num_rows, gather_avx2<T>);
#endif
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

    run_gather_fn(state, *src, *index_ring, num_rows,
                  [](T* d, const T* s, const uint32_t* idx, size_t n) { SIMDGather::gather(d, s, idx, n); });
}

// Args are {num_rows, pool_size}.
#define STARROCKS_GATHER_SIZES \
    ArgsProduct({{1024, 4096, 8192, 65536}, {kSmallPool, kBigPool}})->ArgNames({"rows", "pool"})

BENCHMARK_TEMPLATE(BM_Gather_Baseline, int32_t)->STARROCKS_GATHER_SIZES;
BENCHMARK_TEMPLATE(BM_Gather_Baseline, int64_t)->STARROCKS_GATHER_SIZES;

BENCHMARK_TEMPLATE(BM_Gather_Unrolled, int32_t)->STARROCKS_GATHER_SIZES;
BENCHMARK_TEMPLATE(BM_Gather_Unrolled, int64_t)->STARROCKS_GATHER_SIZES;

BENCHMARK_TEMPLATE(BM_Gather_Prefetch, int32_t, 4)->STARROCKS_GATHER_SIZES;
BENCHMARK_TEMPLATE(BM_Gather_Prefetch, int32_t, 8)->STARROCKS_GATHER_SIZES;
BENCHMARK_TEMPLATE(BM_Gather_Prefetch, int32_t, 16)->STARROCKS_GATHER_SIZES;

BENCHMARK_TEMPLATE(BM_Gather_Prefetch, int64_t, 4)->STARROCKS_GATHER_SIZES;
BENCHMARK_TEMPLATE(BM_Gather_Prefetch, int64_t, 8)->STARROCKS_GATHER_SIZES;
BENCHMARK_TEMPLATE(BM_Gather_Prefetch, int64_t, 16)->STARROCKS_GATHER_SIZES;

BENCHMARK_TEMPLATE(BM_Gather_AVX2, int32_t)->STARROCKS_GATHER_SIZES;
BENCHMARK_TEMPLATE(BM_Gather_AVX2, int64_t)->STARROCKS_GATHER_SIZES;

BENCHMARK_TEMPLATE(BM_Gather_Strided, int32_t)->STARROCKS_GATHER_SIZES;
BENCHMARK_TEMPLATE(BM_Gather_Strided, int64_t)->STARROCKS_GATHER_SIZES;

} // namespace starrocks

BENCHMARK_MAIN();
