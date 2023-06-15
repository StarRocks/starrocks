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
#include "runtime/runtime_state.h"
#include "types/bitmap_value.h"
#include "util/random.h"

namespace starrocks {
class RoaringBitmapMemTest {
public:
    void SetUp() {}
    void TearDown() {}

    RoaringBitmapMemTest(size_t value_count, size_t value_unique_count, size_t bitmap_count, size_t step)
            : _rand(0),
              _value_count(value_count),
              _value_unique_count(value_unique_count),
              _bitmap_count(bitmap_count),
              _step(step) {}

    void do_bench(benchmark::State& state);

private:
    std::vector<detail::Roaring64Map> _bitmap;
    Random _rand;
    std::unique_ptr<MemTracker> _tracker;

    size_t _value_count = 0;
    size_t _value_unique_count = 0;
    size_t _bitmap_count = 0;
    size_t _step = 0;
};

void RoaringBitmapMemTest::do_bench(benchmark::State& state) {
    _tracker = std::make_unique<MemTracker>();
    CurrentThread::current().set_mem_tracker(_tracker.get());

    LOG(ERROR) << "BEFORE: " << CurrentThread::current().mem_tracker()->consumption();

    _bitmap.resize(1000);
    uint32_t v = 0;
    for (size_t i = 0; i < _value_count; i++) {
        for (size_t j = 0; j < 1000; j++) {
            _bitmap[j].add(v);
        }
        v += _step;
    }

    LOG(ERROR) << "AFTER: " << CurrentThread::current().mem_tracker()->consumption();
    CurrentThread::current().set_mem_tracker(nullptr);
}

static void bench_func(benchmark::State& state) {
    size_t value_count = state.range(0);
    size_t value_unique_count = state.range(1);
    size_t bitmap_count = state.range(2);
    size_t step = state.range(3);

    RoaringBitmapMemTest perf(value_count, value_unique_count, bitmap_count, step);
    perf.do_bench(state);
}

static void process_args(benchmark::internal::Benchmark* b) {
    b->Args({100000, 30000000, 1000, 1})->Iterations(1);
    b->Args({100000, 30000000, 1000, 2})->Iterations(1);
    b->Args({100000, 30000000, 1000, 8})->Iterations(1);
    b->Args({100000, 30000000, 1000, 16})->Iterations(1);
    b->Args({100000, 30000000, 1000, 32})->Iterations(1);
    b->Args({100000, 30000000, 1000, 64})->Iterations(1);

    b->Args({10000, 30000000, 1000, 10})->Iterations(1);
    b->Args({10000, 30000000, 1000, 20})->Iterations(1);
    b->Args({10000, 30000000, 1000, 80})->Iterations(1);
    b->Args({10000, 30000000, 1000, 160})->Iterations(1);
    b->Args({10000, 30000000, 1000, 320})->Iterations(1);
    b->Args({10000, 30000000, 1000, 640})->Iterations(1);
}

BENCHMARK(bench_func)->Apply(process_args);

} // namespace starrocks

BENCHMARK_MAIN();