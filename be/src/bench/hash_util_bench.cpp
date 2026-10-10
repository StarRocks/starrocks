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

#include <cstring>
#include <mutex>
#include <vector>

#include "base/hash/hash_util.hpp"

namespace starrocks {

static char __attribute__((aligned((64)))) g_bench_buf[2048];
static std::once_flag g_bench_buf_init;

static void init_bench_buffer() {
    for (size_t i = 0; i < sizeof(g_bench_buf); ++i) {
        g_bench_buf[i] = static_cast<char>((i * 31 + 17) & 0xff);
    }
}

static void BM_HashUtil_Hash32(benchmark::State& state) {
    std::call_once(g_bench_buf_init, init_bench_buffer);
    int32_t len = state.range(0);
    uint32_t seed = 0x811C9DC5;
    uint32_t res = 0;
    for (auto _ : state) {
        res ^= HashUtil::hash(g_bench_buf, len, seed);
        benchmark::DoNotOptimize(res);
    }
    state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(len));
}

static void BM_HashUtil_Hash64(benchmark::State& state) {
    std::call_once(g_bench_buf_init, init_bench_buffer);
    int32_t len = state.range(0);
    uint64_t seed = 0x1234567890abcdefULL;
    uint64_t res = 0;
    for (auto _ : state) {
        res ^= HashUtil::hash64(g_bench_buf, len, seed);
        benchmark::DoNotOptimize(res);
    }
    state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(len));
}

static void BM_HashUtil_CrcHash32(benchmark::State& state) {
    std::call_once(g_bench_buf_init, init_bench_buffer);
    int32_t len = state.range(0);
    uint32_t seed = 0x13579bdu;
    uint32_t res = 0;
    for (auto _ : state) {
        res ^= HashUtil::crc_hash(g_bench_buf, len, seed);
        benchmark::DoNotOptimize(res);
    }
    state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(len));
}

static void BM_HashUtil_CrcHash64(benchmark::State& state) {
    std::call_once(g_bench_buf_init, init_bench_buffer);
    int32_t len = state.range(0);
    uint64_t seed = 0x12345678abcdef90ULL;
    uint64_t res = 0;
    for (auto _ : state) {
        res ^= HashUtil::crc_hash64(g_bench_buf, len, seed);
        benchmark::DoNotOptimize(res);
    }
    state.SetBytesProcessed(int64_t(state.iterations()) * int64_t(len));
}

struct RowsetIdMock {
    int64_t hi{0x1234567890abcdefULL};
    int64_t mi{0x0fedcba987654321ULL};
    int64_t lo{0x55aa55aa33cc33ccULL};
};

static void BM_HashUtil_RowsetIdHash64(benchmark::State& state) {
    RowsetIdMock rowset_id;
    uint64_t res = 0;
    for (auto _ : state) {
        uint64_t seed = 0;
        seed = HashUtil::hash64(&rowset_id.hi, sizeof(rowset_id.hi), seed);
        seed = HashUtil::hash64(&rowset_id.mi, sizeof(rowset_id.mi), seed);
        seed = HashUtil::hash64(&rowset_id.lo, sizeof(rowset_id.lo), seed);
        res ^= seed;
        benchmark::DoNotOptimize(res);
    }
    state.SetBytesProcessed(int64_t(state.iterations()) * 24);
}

BENCHMARK(BM_HashUtil_Hash32)->Arg(0)->Arg(8)->Arg(32)->Arg(128)->Arg(1024);
BENCHMARK(BM_HashUtil_Hash64)->Arg(0)->Arg(8)->Arg(32)->Arg(128)->Arg(1024);
BENCHMARK(BM_HashUtil_CrcHash32)->Arg(0)->Arg(8)->Arg(32)->Arg(128)->Arg(1024);
BENCHMARK(BM_HashUtil_CrcHash64)->Arg(0)->Arg(8)->Arg(32)->Arg(128)->Arg(1024);
BENCHMARK(BM_HashUtil_RowsetIdHash64);

} // namespace starrocks

BENCHMARK_MAIN();
