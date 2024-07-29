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
#include "runtime/runtime_state.h"
#include "types/bitmap_value.h"
#include "types/bitmap_value_detail.h"
#include "util/random.h"
#include "runtime/memory/roaring_allocator.h"
#include "runtime/memory/debug_allocator.h"
#include "runtime/memory/counting_allocator.h"


namespace starrocks {
class RoaringBitmapMemTest {
public:
    void SetUp() {}
    void TearDown() {}

    RoaringBitmapMemTest(size_t value_count, size_t start, size_t end)
            : _value_count(value_count), _start(start), _end(end), _rand(0) {
        _tracker = std::make_unique<MemTracker>();
    }

    void do_bench(benchmark::State& state);

private:
    std::vector<detail::Roaring64Map> _bitmap;
    std::unique_ptr<MemTracker> _tracker;

    size_t _value_count = 0;
    size_t _start = 0;
    size_t _end = 0;
    Random _rand;
};

// case 1
// 32bit高位都不一样，每个id在不同的桶，全是小内存分配, 放2亿个
// 对比测试：32bit高位一样，低位不一样，都集中在一个roaring

// case 2
// 32bit高位一样，中间16bit不一样，在不同的container里
class RoaringBitmapPerfTest {
public:
    void SetUp() {}
    void TearDown() {}

    RoaringBitmapPerfTest(size_t start, size_t end, size_t shift_width):
        _start(start), _end(end), _shift_width(shift_width) {}
    template<class Alloc>
    void do_bench(benchmark::State& state);
private:
    std::unique_ptr<detail::Roaring64Map> _bitmap;
    std::unique_ptr<Allocator> _allocator;

    size_t _start = 0;
    size_t _end = 0;
    size_t _shift_width = 0;
};

template <class Alloc>
void RoaringBitmapPerfTest::do_bench(benchmark::State& state) {
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
        for (int i = 0;i < num_threads;i++) {
            threads.emplace_back([&]() {
                RoaringBitmapPerfTest perf(start, end, shift_width);
                perf.do_bench<MemHookAllocator>(state);
            });
        }
        for (auto& thread: threads) {
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
        for (int i = 0;i < num_threads;i++) {
            threads.emplace_back([&] () {
                RoaringBitmapPerfTest perf(start, end, shift_width);
                perf.do_bench<CountingAllocatorWithHook>(state);
            });
        }
        for(auto& thread: threads) {
            thread.join();
        }
   }
}

static void process_args(benchmark::internal::Benchmark* b) {
    int64_t start = 0;
    int64_t end = 1 << 20;
    int64_t iterations = 2;
    for (auto i : {0, 1, 2, 4, 8, 16,  32}) {
        b->Args({start, end, i})->Iterations(iterations);
    }
}

BENCHMARK(BM_mem_hook_allocator)->Apply(process_args)->Unit(benchmark::kMillisecond)->Threads(1)->Threads(4)->Threads(8)->Threads(16)->Threads(32);
BENCHMARK(BM_counting_allocator)->Apply(process_args)->Unit(benchmark::kMillisecond)->Threads(1)->Threads(4)->Threads(8)->Threads(16)->Threads(32);

} // namespace starrocks

BENCHMARK_MAIN();