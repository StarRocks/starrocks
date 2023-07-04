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

/* test result
|value_count|start|end|mem usage|
|----|----|---|----|
|10000|1|5000000000|350K|
|100000|1|5000000000|2.1M|
|1000000|1|5000000000|4.9M|
|10000|1|200000000|104K|
|100000|1|200000000|377K|
|1000000|1|200000000|2.8M|
|10000|1|100000000|63K|
|100000|1|100000000|293K|
|1000000|1|100000000|2.3M|
|10000|1|10000000|20K|
|100000|1|10000000|230K|
|1000000|1|10000000|1.2M|
 */

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

void RoaringBitmapMemTest::do_bench(benchmark::State& state) {
    CurrentThread::current().set_mem_tracker(_tracker.get());

    size_t start_size = CurrentThread::current().mem_tracker()->consumption();

    _bitmap.resize(100);
    size_t size = _end - _start;
    for (size_t i = 0; i < _value_count; i++) {
        uint64_t v = _rand.Next64() % size + _start;
        for (size_t j = 0; j < 100; j++) {
            _bitmap[j].add(v);
        }
    }

    size_t end_size = CurrentThread::current().mem_tracker()->consumption();

    LOG(INFO) << "MEM_USAGE: " << (end_size - start_size) / 100;
    CurrentThread::current().set_mem_tracker(nullptr);
}

static void bench_func(benchmark::State& state) {
    size_t value_count = state.range(0);
    size_t start = state.range(1);
    size_t end = state.range(2);

    RoaringBitmapMemTest perf(value_count, start, end);
    perf.do_bench(state);
}

static void process_args(benchmark::internal::Benchmark* b) {
    b->Args({10000, 1, 5000000000})->Iterations(1);
    b->Args({100000, 1, 5000000000})->Iterations(1);
    b->Args({1000000, 1, 5000000000})->Iterations(1);

    b->Args({10000, 1, 200000000})->Iterations(1);
    b->Args({100000, 1, 200000000})->Iterations(1);
    b->Args({1000000, 1, 200000000})->Iterations(1);

    b->Args({10000, 1, 100000000})->Iterations(1);
    b->Args({100000, 1, 100000000})->Iterations(1);
    b->Args({1000000, 1, 100000000})->Iterations(1);

    b->Args({10000, 1, 10000000})->Iterations(1);
    b->Args({100000, 1, 10000000})->Iterations(1);
    b->Args({1000000, 1, 10000000})->Iterations(1);
}

BENCHMARK(bench_func)->Apply(process_args);

} // namespace starrocks

BENCHMARK_MAIN();