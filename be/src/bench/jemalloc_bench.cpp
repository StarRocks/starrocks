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
#include <glog/logging.h>
#include <jemalloc/jemalloc.h>

#include <cstdio>
#include <cstring>

#include "gutil/strings/fastmem.h"

namespace starrocks {

namespace {

// Reset jemalloc arena state by purging unused memory
static void reset_arena_state() {
#define JEMALLOC_CTL je_mallctl
    char buffer[100];
    // MALLCTL_ARENAS_ALL is defined as 4096 in jemalloc.h, representing all arenas
    // Use the value directly if the macro is not available
#ifndef MALLCTL_ARENAS_ALL
    constexpr unsigned int MALLCTL_ARENAS_ALL_VALUE = 4096;
#else
    constexpr unsigned int MALLCTL_ARENAS_ALL_VALUE = MALLCTL_ARENAS_ALL;
#endif
    int res = snprintf(buffer, sizeof(buffer), "arena.%u.purge", MALLCTL_ARENAS_ALL_VALUE);
    if (res > 0 && res < static_cast<int>(sizeof(buffer))) {
        buffer[res] = '\0';
        JEMALLOC_CTL(buffer, nullptr, nullptr, nullptr, 0);
    }
#undef JEMALLOC_CTL
}

// Benchmark: je_realloc vs je_xallocx + je_malloc + memcpy + je_free
void BM_jemalloc_realloc_je_realloc(benchmark::State& state) {
    const size_t old_size = static_cast<size_t>(state.range(0));
    const size_t new_size = static_cast<size_t>(state.range(1));
    CHECK_LT(old_size, new_size);

    // Reset arena state before each test
    reset_arena_state();

    void* ptr = je_malloc(old_size);
    CHECK(ptr != nullptr);
    std::memset(ptr, 0, old_size);

    int64_t in_place_count = 0;

    for (auto _ : state) {
        void* old_ptr = ptr;
        ptr = je_realloc(ptr, new_size);
        CHECK(ptr != nullptr);

        // Check if realloc was in-place (pointer didn't change)
        if (ptr == old_ptr) {
            in_place_count++;
        }

        benchmark::DoNotOptimize(ptr);
    }

    je_free(ptr);

    // Report statistics as percentages using SetLabel
    const double total_iterations = static_cast<double>(state.iterations());
    const double in_place_pct = (static_cast<double>(in_place_count) / total_iterations) * 100.0;

    char label[128];
    snprintf(label, sizeof(label), "in_place=%.2f%%", in_place_pct);
    state.SetLabel(label);

    // Reset arena state after each test
    reset_arena_state();
}

void BM_jemalloc_realloc_xallocx_malloc_memcpy_free(benchmark::State& state) {
    const size_t old_size = static_cast<size_t>(state.range(0));
    const size_t new_size = static_cast<size_t>(state.range(1));
    CHECK_LT(old_size, new_size);

    // Reset arena state before each test
    reset_arena_state();

    void* ptr = je_malloc(old_size);
    CHECK(ptr != nullptr);
    std::memset(ptr, 0, old_size);

    int64_t in_place_count = 0;

    for (auto _ : state) {
        // Try to extend in-place first
        if (je_xallocx(ptr, new_size, 0, 0) >= new_size) {
            in_place_count++;
            benchmark::DoNotOptimize(ptr);
        } else {
            // Fall back to malloc + memcpy + free
            void* new_ptr = je_malloc(new_size);
            CHECK(new_ptr != nullptr);
            std::memcpy(new_ptr, ptr, old_size);
            je_free(ptr);
            ptr = new_ptr;
            benchmark::DoNotOptimize(ptr);
        }
    }

    je_free(ptr);

    // Report statistics as percentages using SetLabel
    const double total_iterations = static_cast<double>(state.iterations());
    const double in_place_pct = (static_cast<double>(in_place_count) / total_iterations) * 100.0;

    char label[128];
    snprintf(label, sizeof(label), "in_place=%.2f%%", in_place_pct);
    state.SetLabel(label);

    // Reset arena state after each test
    reset_arena_state();
}

void BM_jemalloc_realloc_xallocx_malloc_memcpy_inlined_free(benchmark::State& state) {
    const size_t old_size = static_cast<size_t>(state.range(0));
    const size_t new_size = static_cast<size_t>(state.range(1));
    CHECK_LT(old_size, new_size);

    // Reset arena state before each test
    reset_arena_state();

    void* ptr = je_malloc(old_size);
    CHECK(ptr != nullptr);
    std::memset(ptr, 0, old_size);

    int64_t in_place_count = 0;

    for (auto _ : state) {
        // Try to extend in-place first
        if (je_xallocx(ptr, new_size, 0, 0) >= new_size) {
            in_place_count++;
            benchmark::DoNotOptimize(ptr);
        } else {
            // Fall back to malloc + memcpy_inlined + free
            void* new_ptr = je_malloc(new_size);
            CHECK(new_ptr != nullptr);
            strings::memcpy_inlined(new_ptr, ptr, old_size);
            je_free(ptr);
            ptr = new_ptr;
            benchmark::DoNotOptimize(ptr);
        }
    }

    je_free(ptr);

    // Report statistics as percentages using SetLabel
    const double total_iterations = static_cast<double>(state.iterations());
    const double in_place_pct = (static_cast<double>(in_place_count) / total_iterations) * 100.0;

    char label[128];
    snprintf(label, sizeof(label), "in_place=%.2f%%", in_place_pct);
    state.SetLabel(label);

    // Reset arena state after each test
    reset_arena_state();
}

void process_realloc_args(benchmark::internal::Benchmark* b) {
    b->ArgNames({"old_size", "new_size"});
    int64_t old_size = 8;
    constexpr int64_t kMaxOldSize = 2048LL * 1024 * 1024;
    while (old_size <= kMaxOldSize) {
        b->Args({old_size, old_size * 2});
        old_size *= 2;
    }
}

} // namespace

BENCHMARK(BM_jemalloc_realloc_je_realloc)->Name("realloc")->Apply(process_realloc_args);
BENCHMARK(BM_jemalloc_realloc_xallocx_malloc_memcpy_free)->Name("xallocx")->Apply(process_realloc_args);
BENCHMARK(BM_jemalloc_realloc_xallocx_malloc_memcpy_inlined_free)
        ->Name("xallocx_memcpy_inlined")
        ->Apply(process_realloc_args);

} // namespace starrocks

int main(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}

// ---------------------------------------------------------------------------------------------------------
// Benchmark                                                               Time             CPU   Iterations
// ---------------------------------------------------------------------------------------------------------
// realloc/old_size:8/new_size:16                                       29.0 ns         29.0 ns     24043617 in_place=100.00%
// realloc/old_size:16/new_size:32                                      28.5 ns         28.5 ns     24686538 in_place=100.00%
// realloc/old_size:32/new_size:64                                      28.4 ns         28.4 ns     24269021 in_place=100.00%
// realloc/old_size:64/new_size:128                                     28.8 ns         28.8 ns     23902198 in_place=100.00%
// realloc/old_size:128/new_size:256                                    28.6 ns         28.6 ns     24014948 in_place=100.00%
// realloc/old_size:256/new_size:512                                    29.0 ns         29.0 ns     24233069 in_place=100.00%
// realloc/old_size:512/new_size:1024                                   29.4 ns         29.4 ns     23844304 in_place=100.00%
// realloc/old_size:1024/new_size:2048                                  30.8 ns         30.8 ns     22720814 in_place=100.00%
// realloc/old_size:2048/new_size:4096                                  33.3 ns         33.3 ns     20965363 in_place=100.00%
// realloc/old_size:4096/new_size:8192                                  43.1 ns         43.1 ns     16027505 in_place=100.00%
// realloc/old_size:8192/new_size:16384                                 44.0 ns         44.0 ns     15900415 in_place=100.00%
// realloc/old_size:16384/new_size:32768                                56.3 ns         56.3 ns     12431435 in_place=100.00%
// realloc/old_size:32768/new_size:65536                                80.6 ns         80.6 ns      8662645 in_place=100.00%
// realloc/old_size:65536/new_size:131072                               80.6 ns         80.6 ns      8661201 in_place=100.00%
// realloc/old_size:131072/new_size:262144                              80.7 ns         80.7 ns      8685874 in_place=100.00%
// realloc/old_size:262144/new_size:524288                              80.6 ns         80.6 ns      8664442 in_place=100.00%
// realloc/old_size:524288/new_size:1048576                             80.6 ns         80.6 ns      8688944 in_place=100.00%
// realloc/old_size:1048576/new_size:2097152                            80.6 ns         80.6 ns      8681650 in_place=100.00%
// realloc/old_size:2097152/new_size:4194304                            82.9 ns         82.9 ns      8344051 in_place=100.00%
// realloc/old_size:4194304/new_size:8388608                            81.1 ns         81.1 ns      8426847 in_place=100.00%
// realloc/old_size:8388608/new_size:16777216                           83.4 ns         83.4 ns      7767460 in_place=100.00%
// realloc/old_size:16777216/new_size:33554432                          82.7 ns         82.7 ns      7601347 in_place=100.00%
// realloc/old_size:33554432/new_size:67108864                          89.3 ns         89.2 ns      6700879 in_place=100.00%
// realloc/old_size:67108864/new_size:134217728                         90.9 ns         90.9 ns      6121683 in_place=100.00%
// realloc/old_size:134217728/new_size:268435456                        93.2 ns         93.2 ns      7125982 in_place=100.00%
// realloc/old_size:268435456/new_size:536870912                         116 ns          116 ns      4794507 in_place=100.00%
// realloc/old_size:536870912/new_size:1073741824                        230 ns          230 ns      2323725 in_place=100.00%
// realloc/old_size:1073741824/new_size:2147483648                 682916077 ns    682798067 ns            1 in_place=0.00%
// realloc/old_size:2147483648/new_size:4294967296                1363934042 ns   1363772947 ns            1 in_place=0.00%
// xallocx/old_size:8/new_size:16                                       16.1 ns         16.1 ns     43474366 in_place=100.00%
// xallocx/old_size:16/new_size:32                                      16.1 ns         16.1 ns     43428740 in_place=100.00%
// xallocx/old_size:32/new_size:64                                      16.1 ns         16.1 ns     43428839 in_place=100.00%
// xallocx/old_size:64/new_size:128                                     16.1 ns         16.1 ns     43400980 in_place=100.00%
// xallocx/old_size:128/new_size:256                                    16.1 ns         16.1 ns     43432486 in_place=100.00%
// xallocx/old_size:256/new_size:512                                    16.1 ns         16.1 ns     43440884 in_place=100.00%
// xallocx/old_size:512/new_size:1024                                   16.1 ns         16.1 ns     43436714 in_place=100.00%
// xallocx/old_size:1024/new_size:2048                                  16.1 ns         16.1 ns     43441791 in_place=100.00%
// xallocx/old_size:2048/new_size:4096                                  16.1 ns         16.1 ns     43391112 in_place=100.00%
// xallocx/old_size:4096/new_size:8192                                  21.9 ns         21.9 ns     33197001 in_place=100.00%
// xallocx/old_size:8192/new_size:16384                                 22.0 ns         22.0 ns     31739928 in_place=100.00%
// xallocx/old_size:16384/new_size:32768                                22.0 ns         22.0 ns     31546638 in_place=100.00%
// xallocx/old_size:32768/new_size:65536                                22.0 ns         22.0 ns     31756671 in_place=100.00%
// xallocx/old_size:65536/new_size:131072                               21.9 ns         21.9 ns     31965565 in_place=100.00%
// xallocx/old_size:131072/new_size:262144                              22.0 ns         22.0 ns     31949265 in_place=100.00%
// xallocx/old_size:262144/new_size:524288                              22.1 ns         22.1 ns     31793137 in_place=100.00%
// xallocx/old_size:524288/new_size:1048576                             21.9 ns         21.9 ns     31832159 in_place=100.00%
// xallocx/old_size:1048576/new_size:2097152                            21.9 ns         21.9 ns     31958397 in_place=100.00%
// xallocx/old_size:2097152/new_size:4194304                            22.0 ns         22.0 ns     31781098 in_place=100.00%
// xallocx/old_size:4194304/new_size:8388608                            22.1 ns         22.1 ns     31219324 in_place=100.00%
// xallocx/old_size:8388608/new_size:16777216                           22.1 ns         22.1 ns     31210110 in_place=100.00%
// xallocx/old_size:16777216/new_size:33554432                          22.3 ns         22.3 ns     30400076 in_place=100.00%
// xallocx/old_size:33554432/new_size:67108864                          22.8 ns         22.8 ns     28987953 in_place=100.00%
// xallocx/old_size:67108864/new_size:134217728                         23.9 ns         23.9 ns     26247515 in_place=100.00%
// xallocx/old_size:134217728/new_size:268435456                        25.9 ns         25.9 ns     22476564 in_place=100.00%
// xallocx/old_size:268435456/new_size:536870912                        31.9 ns         31.8 ns     17673648 in_place=100.00%
// xallocx/old_size:536870912/new_size:1073741824                       59.2 ns         59.2 ns      9293843 in_place=100.00%
// xallocx/old_size:1073741824/new_size:2147483648                 685996613 ns    685968320 ns            1 in_place=0.00%
// xallocx/old_size:2147483648/new_size:4294967296                1335500051 ns   1335391513 ns            1 in_place=0.00%
// xallocx_memcpy_inlined/old_size:8/new_size:16                        16.1 ns         16.1 ns     43454090 in_place=100.00%
// xallocx_memcpy_inlined/old_size:16/new_size:32                       16.1 ns         16.1 ns     43227064 in_place=100.00%
// xallocx_memcpy_inlined/old_size:32/new_size:64                       16.1 ns         16.1 ns     43447477 in_place=100.00%
// xallocx_memcpy_inlined/old_size:64/new_size:128                      16.1 ns         16.1 ns     43432817 in_place=100.00%
// xallocx_memcpy_inlined/old_size:128/new_size:256                     16.1 ns         16.1 ns     43417755 in_place=100.00%
// xallocx_memcpy_inlined/old_size:256/new_size:512                     16.1 ns         16.1 ns     43427394 in_place=100.00%
// xallocx_memcpy_inlined/old_size:512/new_size:1024                    16.1 ns         16.1 ns     43436510 in_place=100.00%
// xallocx_memcpy_inlined/old_size:1024/new_size:2048                   16.1 ns         16.1 ns     43458507 in_place=100.00%
// xallocx_memcpy_inlined/old_size:2048/new_size:4096                   16.1 ns         16.1 ns     43436902 in_place=100.00%
// xallocx_memcpy_inlined/old_size:4096/new_size:8192                   21.1 ns         21.1 ns     33202786 in_place=100.00%
// xallocx_memcpy_inlined/old_size:8192/new_size:16384                  22.0 ns         22.0 ns     31826956 in_place=100.00%
// xallocx_memcpy_inlined/old_size:16384/new_size:32768                 22.0 ns         22.0 ns     31766669 in_place=100.00%
// xallocx_memcpy_inlined/old_size:32768/new_size:65536                 23.8 ns         23.8 ns     31751488 in_place=100.00%
// xallocx_memcpy_inlined/old_size:65536/new_size:131072                22.3 ns         22.3 ns     31472561 in_place=100.00%
// xallocx_memcpy_inlined/old_size:131072/new_size:262144               22.2 ns         22.2 ns     31455443 in_place=100.00%
// xallocx_memcpy_inlined/old_size:262144/new_size:524288               22.0 ns         22.0 ns     31710749 in_place=100.00%
// xallocx_memcpy_inlined/old_size:524288/new_size:1048576              22.1 ns         22.1 ns     31502021 in_place=100.00%
// xallocx_memcpy_inlined/old_size:1048576/new_size:2097152             22.1 ns         22.0 ns     31505982 in_place=100.00%
// xallocx_memcpy_inlined/old_size:2097152/new_size:4194304             22.1 ns         22.1 ns     31591949 in_place=100.00%
// xallocx_memcpy_inlined/old_size:4194304/new_size:8388608             22.1 ns         22.1 ns     31529501 in_place=100.00%
// xallocx_memcpy_inlined/old_size:8388608/new_size:16777216            22.1 ns         22.1 ns     31345980 in_place=100.00%
// xallocx_memcpy_inlined/old_size:16777216/new_size:33554432           22.3 ns         22.3 ns     30648112 in_place=100.00%
// xallocx_memcpy_inlined/old_size:33554432/new_size:67108864           22.6 ns         22.6 ns     29545883 in_place=100.00%
// xallocx_memcpy_inlined/old_size:67108864/new_size:134217728          23.4 ns         23.4 ns     27933071 in_place=100.00%
// xallocx_memcpy_inlined/old_size:134217728/new_size:268435456         25.3 ns         25.3 ns     22278381 in_place=100.00%
// xallocx_memcpy_inlined/old_size:268435456/new_size:536870912         29.8 ns         29.8 ns     17643570 in_place=100.00%
// xallocx_memcpy_inlined/old_size:536870912/new_size:1073741824        44.8 ns         44.8 ns     11861912 in_place=100.00%
// xallocx_memcpy_inlined/old_size:1073741824/new_size:2147483648  536697698 ns    536662731 ns            1 in_place=0.00%
// xallocx_memcpy_inlined/old_size:2147483648/new_size:4294967296 1060514059 ns   1060450895 ns            1 in_place=0.00%
