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
#include <testutil/assert.h>

#include <memory>

#include "column/datum_tuple.h"
#include "common/config.h"
#include "runtime/chunk_cursor.h"
#include "runtime/current_thread.h"
#include "runtime/memory/counting_allocator.h"
#include "runtime/memory/mem_hook_allocator.h"
#include "runtime/memory/roaring_hook.h"
#include "runtime/runtime_state.h"
#include "types/bitmap_value.h"
#include "types/bitmap_value_detail.h"
#include "util/random.h"

namespace starrocks {

class RoaringBitmapAllocTest {
public:
    void SetUp() {}
    void TearDown() {}

    RoaringBitmapAllocTest(size_t start, size_t end, size_t shift_width)
            : _start(start), _end(end), _shift_width(shift_width) {}
    template <class Alloc>
    void do_bench(benchmark::State& state);

private:
    std::unique_ptr<detail::Roaring64Map> _bitmap;
    std::unique_ptr<Allocator> _allocator;

    size_t _start = 0;
    size_t _end = 0;
    size_t _shift_width = 0;
};

template <class Alloc>
void RoaringBitmapAllocTest::do_bench(benchmark::State& state) {
    init_roaring_hook();
    _bitmap = std::make_unique<detail::Roaring64Map>();
    _allocator = std::make_unique<Alloc>();
    ThreadLocalRoaringAllocatorSetter setter(_allocator.get());
    state.ResumeTiming();
    for (size_t i = _start; i < _end; i++) {
        _bitmap->add(i << _shift_width);
    }
    state.PauseTiming();
}

static void BM_mem_hook_allocator(benchmark::State& state) {
    size_t start = state.range(0);
    size_t end = state.range(1);
    size_t shift_width = state.range(2);
    int num_threads = state.threads;
    for (auto _ : state) {
        std::vector<std::thread> threads;
        for (int i = 0; i < num_threads; i++) {
            threads.emplace_back([&]() {
                RoaringBitmapAllocTest perf(start, end, shift_width);
                perf.do_bench<MemHookAllocator>(state);
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
    }
}

static void BM_counting_allocator(benchmark::State& state) {
    size_t start = state.range(0);
    size_t end = state.range(1);
    size_t shift_width = state.range(2);
    int num_threads = state.threads;
    for (auto _ : state) {
        std::vector<std::thread> threads;
        for (int i = 0; i < num_threads; i++) {
            threads.emplace_back([&]() {
                RoaringBitmapAllocTest perf(start, end, shift_width);
                perf.do_bench<CountingAllocatorWithHook>(state);
            });
        }
        for (auto& thread : threads) {
            thread.join();
        }
    }
}

static void process_args(benchmark::internal::Benchmark* b) {
    int64_t start = 0;
    int64_t end = 1 << 20;
    int64_t iterations = 2;
    std::vector<size_t> shift_width = {0, 1, 2, 4, 8, 16, 32};
    for (auto i : shift_width) {
        b->Args({start, end, i})->Iterations(iterations);
    }
}

BENCHMARK(BM_mem_hook_allocator)
        ->Apply(process_args)
        ->Unit(benchmark::kMillisecond)
        ->Threads(1)
        ->Threads(4)
        ->Threads(8)
        ->Threads(16)
        ->Threads(32);
BENCHMARK(BM_counting_allocator)
        ->Apply(process_args)
        ->Unit(benchmark::kMillisecond)
        ->Threads(1)
        ->Threads(4)
        ->Threads(8)
        ->Threads(16)
        ->Threads(32);

} // namespace starrocks

BENCHMARK_MAIN();
