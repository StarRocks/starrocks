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

#include "column/binary_column.h"
#include "util/random.h"

namespace starrocks {

/*
 * -----------------------------------------------------------------------------
 * Benchmark                                   Time             CPU   Iterations
 * -----------------------------------------------------------------------------
 * bench_func/1/4096/iterations:10        210789 ns       210535 ns            0
 * bench_func/2/4096/iterations:10        142274 ns       142005 ns            0
 * bench_func/3/4096/iterations:10        120352 ns       120120 ns            0
 *
 * bench_func/1/40960/iterations:10      1892398 ns      1891968 ns            0
 * bench_func/2/40960/iterations:10      1830962 ns      1830739 ns            0
 * bench_func/3/40960/iterations:10      1054354 ns      1054389 ns            0
 *
 * bench_func/1/409600/iterations:10    18589689 ns     18587890 ns            0
 * bench_func/2/409600/iterations:10    13372115 ns     13370693 ns            0
 * bench_func/3/409600/iterations:10    14461226 ns     14459599 ns            0
 *
 * bench_func/1/4096000/iterations:10  241755592 ns    241716458 ns            0
 * bench_func/2/4096000/iterations:10  201392434 ns    201372857 ns            0
 * bench_func/3/4096000/iterations:10  199784156 ns    199756156 ns            0
 */

class BinaryColumnCopyBench {
public:
    void SetUp() {}
    void TearDown() {}

    BinaryColumnCopyBench(int mode, int chunk_size) : _mode(mode), _rand(_rd()), _chunk_size(chunk_size) {}

    void do_bench(benchmark::State& state);

private:
    std::string _rand_str();
    BinaryColumn::MutablePtr _gen_binary_column();

    int _mode = 1;
    std::random_device _rd;
    std::default_random_engine _rand;
    int _chunk_size = 4096;
};

void BinaryColumnCopyBench::do_bench(benchmark::State& state) {
    auto column = _gen_binary_column();
    BinaryColumn dest_column;

    if (_mode == 1) {
        state.ResumeTiming();

        auto& data = column->get_data();
        for (size_t i = 0; i < _chunk_size; i++) {
            dest_column.append(data[i]);
        }

        state.PauseTiming();
    } else if (_mode == 2) {
        auto& data = column->get_data();
        state.ResumeTiming();

        for (size_t i = 0; i < _chunk_size; i++) {
            dest_column.append(data[i]);
        }

        state.PauseTiming();
    } else {
        state.ResumeTiming();

        for (size_t i = 0; i < _chunk_size; i++) {
            dest_column.append(column->get_slice(i));
        }

        state.PauseTiming();
    }
}

std::string BinaryColumnCopyBench::_rand_str() {
    char tmp;
    std::string str;

    int length = _rand() % 32 + 1;

    for (int j = 0; j < length; j++) {
        tmp = _rand() % 26;
        tmp += 'A';
        str += tmp;
    }
    return str;
}

BinaryColumn::MutablePtr BinaryColumnCopyBench::_gen_binary_column() {
    BinaryColumn::MutablePtr column = BinaryColumn::create();

    for (size_t i = 0; i < _chunk_size; i++) {
        std::string str = _rand_str();
        column->append_string(str);
    }
    return column;
}

static void bench_func(benchmark::State& state) {
    int mode = state.range(0);
    int chunk_size = state.range(1);

    BinaryColumnCopyBench perf(mode, chunk_size);

    perf.do_bench(state);
}

static void process_args(benchmark::internal::Benchmark* b) {
    b->Args({1, 4096})->Iterations(100);
    b->Args({2, 4096})->Iterations(100);
    b->Args({3, 4096})->Iterations(100);

    b->Args({1, 40960})->Iterations(100);
    b->Args({2, 40960})->Iterations(100);
    b->Args({3, 40960})->Iterations(100);

    b->Args({1, 409600})->Iterations(10);
    b->Args({2, 409600})->Iterations(10);
    b->Args({3, 409600})->Iterations(10);

    b->Args({1, 4096000})->Iterations(10);
    b->Args({2, 4096000})->Iterations(10);
    b->Args({3, 4096000})->Iterations(10);
}

BENCHMARK(bench_func)->Apply(process_args);
} // namespace starrocks

BENCHMARK_MAIN();
