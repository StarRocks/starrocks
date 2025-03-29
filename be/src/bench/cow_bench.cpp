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
#include <fmt/format.h>
#include <gutil/strings/substitute.h>
#include <testutil/assert.h>

#include "column/binary_column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/datum.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "types/logical_type.h"
#include "util/random.h"

namespace starrocks {

// Run on (104 X 3200.01 MHz CPU s)
// CPU Caches:
//   L1 Data 32 KiB (x52)
//   L1 Instruction 32 KiB (x52)
//   L2 Unified 1024 KiB (x52)
//   L3 Unified 36608 KiB (x2)
// Load Average: 16.93, 28.85, 28.00
// dst_column size: 4096
// -------------------------------------------------------------------------------------
// Benchmark                                           Time             CPU   Iterations
// -------------------------------------------------------------------------------------
// bench_func_str_10/1/4096/iterations:100         34244 ns        33013 ns            0
// dst_column size: 4096
// bench_func_str_10/2/4096/iterations:100           975 ns          652 ns            0
// dst_column size: 4096
// bench_func_str_10/3/4096/iterations:100           434 ns          402 ns            0
// dst_column size: 4096
// bench_func_str_100/1/4096/iterations:100       224996 ns       224828 ns            0
// dst_column size: 4096
// bench_func_str_100/2/4096/iterations:100         2162 ns         1186 ns            0
// dst_column size: 4096
// bench_func_str_100/3/4096/iterations:100         2380 ns         1022 ns            0
// dst_column size: 4096
// bench_func_str_1000/1/4096/iterations:100     2230253 ns      2219392 ns            0
// dst_column size: 4096
// bench_func_str_1000/2/4096/iterations:100        3089 ns         1692 ns            0
// dst_column size: 4096
// bench_func_str_1000/3/4096/iterations:100        2622 ns         1242 ns            0
// dst_column size: 4096
// bench_func_decimal/1/4096/iterations:100        23362 ns        23267 ns            0
// dst_column size: 4096
// bench_func_decimal/2/4096/iterations:100          684 ns          664 ns            0
// dst_column size: 4096
// bench_func_decimal/3/4096/iterations:100          322 ns          299 ns            0
// dst_column size: 4096
// bench_func_bigint/1/4096/iterations:100          2027 ns         2028 ns            0
// dst_column size: 4096
// bench_func_bigint/2/4096/iterations:100           699 ns          503 ns            0
// dst_column size: 4096
// bench_func_bigint/3/4096/iterations:100           338 ns          302 ns            0
// dst_column size: 4096
// bench_func_json/1/4096/iterations:100           47890 ns        47770 ns            0
// dst_column size: 4096
// bench_func_json/2/4096/iterations:100            1077 ns          722 ns            0
// dst_column size: 4096
// bench_func_json/3/4096/iterations:100             717 ns          480 ns            0
// dst_column size: 4096
// bench_func_struct/1/4096/iterations:100         24799 ns        24549 ns            0
// dst_column size: 4096
// bench_func_struct/2/4096/iterations:100          1931 ns         1374 ns            0
// dst_column size: 4096
// bench_func_struct/3/4096/iterations:100          1119 ns          566 ns            0
// dst_column size: 10
// bench_func_map/1/4096/iterations:100             2269 ns         2201 ns            0
// dst_column size: 10
// bench_func_map/2/4096/iterations:100             1345 ns         1337 ns            0
// dst_column size: 10
// bench_func_map/3/4096/iterations:100              434 ns          413 ns            0
// dst_column size: 4096
// bench_func_str_100/1/4096/iterations:100        60600 ns        60087 ns            0
// dst_column size: 4096
// bench_func_str_100/2/4096/iterations:100         1885 ns          994 ns            0
// dst_column size: 4096
// bench_func_str_100/3/4096/iterations:100         1377 ns          820 ns            0
// dst_column size: 40960
// bench_func_str_100/1/40960/iterations:100      775279 ns       774957 ns            0
// dst_column size: 40960
// bench_func_str_100/2/40960/iterations:100        4461 ns         2352 ns            0
// dst_column size: 40960
// bench_func_str_100/3/40960/iterations:100        4794 ns         2484 ns            0
// dst_column size: 409600
// bench_func_str_100/1/409600/iterations:10    25968634 ns     25931662 ns            0
// dst_column size: 409600
// bench_func_str_100/2/409600/iterations:10        3600 ns         2364 ns            0
// dst_column size: 409600
// bench_func_str_100/3/409600/iterations:10        5136 ns         2706 ns            0
// dst_column size: 4096000
// bench_func_str_100/1/4096000/iterations:10  257056564 ns    256827582 ns            0
// dst_column size: 4096000
// bench_func_str_100/2/4096000/iterations:10       4249 ns         2692 ns            0
// dst_column size: 4096000
// bench_func_str_100/3/4096000/iterations:10       4867 ns         3313 ns            0

class COWBench {
public:
    void SetUp() {}
    void TearDown() {}

    COWBench(int mode, int chunk_size) : _mode(mode), _rand(_rd()), _chunk_size(chunk_size) {}
    COWBench(int mode, int chunk_size, int length)
            : _mode(mode), _rand(_rd()), _chunk_size(chunk_size), _length(length) {}

    void do_bench(benchmark::State& state, LogicalType type);

private:
    std::string _rand_str(int length);
    ColumnPtr _gen_binary_column();
    ColumnPtr _gen_decimal_column();
    ColumnPtr _gen_bigint_column();
    ColumnPtr _gen_json_column();
    ColumnPtr _gen_struct_column();
    ColumnPtr _gen_map_column();

    int _mode = 1;
    std::random_device _rd;
    std::default_random_engine _rand;
    int _chunk_size = 4096;
    int _length = 32;
};

void COWBench::do_bench(benchmark::State& state, LogicalType type) {
    ColumnPtr column;
    switch (type) {
    case TYPE_VARCHAR:
        column = _gen_binary_column();
        break;
    case TYPE_DECIMAL:
        column = _gen_decimal_column();
        break;
    case TYPE_JSON:
        column = _gen_json_column();
        break;
    case TYPE_STRUCT:
        column = _gen_struct_column();
        break;
    case TYPE_MAP:
        column = _gen_map_column();
        break;
    default:
        column = _gen_bigint_column();
    }
    ColumnPtr dst_column;

    if (_mode == 1) {
        state.ResumeTiming();
        dst_column = column->clone();
        state.PauseTiming();
    } else if (_mode == 2) {
        state.ResumeTiming();
        dst_column = (std::move(*column)).mutate();
        state.PauseTiming();
    } else {
        state.ResumeTiming();
        dst_column = Column::mutate(std::move(column));
        state.PauseTiming();
    }
    // To avoid optimizing out the benchmark
    std::cout << "dst_column size: " << dst_column->size() << std::endl;
}

std::string COWBench::_rand_str(int length) {
    char tmp;
    std::string str;
    for (int j = 0; j < length; j++) {
        tmp = _rand() % 26;
        tmp += 'A';
        str += tmp;
    }
    return str;
}

ColumnPtr COWBench::_gen_binary_column() {
    auto column = BinaryColumn::create();
    for (size_t i = 0; i < _chunk_size; i++) {
        std::string str = _rand_str(_length);
        column->append_string(str);
    }
    return column;
}

ColumnPtr COWBench::_gen_decimal_column() {
    auto column = DecimalColumn::create();
    for (size_t i = 0; i < _chunk_size; i++) {
        column->append(DecimalV2Value(_rand()));
    }
    return column;
}

ColumnPtr COWBench::_gen_bigint_column() {
    auto column = Int64Column::create();
    for (size_t i = 0; i < _chunk_size; i++) {
        column->append(_rand());
    }
    return column;
}

ColumnPtr COWBench::_gen_json_column() {
    auto col = JsonColumn::create();
    for (size_t i = 0; i < _chunk_size; i++) {
        col->append(JsonValue::parse(R"( {"a": "a"} )").value());
    }
    return col;
}

ColumnPtr COWBench::_gen_map_column() {
    MapColumn::Ptr column = MapColumn::create(NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                              NullableColumn::create(Int32Column::create(), NullColumn::create()),
                                              UInt32Column::create());
    for (int32_t i = 0; i < 10; i++) {
        column->append_datum(DatumMap{{i, i + 1}});
    }
    return column;
}

ColumnPtr COWBench::_gen_struct_column() {
    auto binary_col = BinaryColumn::create();
    auto int_col = UInt64Column::create();
    for (size_t i = 0; i < _chunk_size; i++) {
        std::string str = _rand_str(_length);
        binary_col->append(Slice(str));
        int_col->append(i);
    }
    auto c0 = NullableColumn::create(std::move(binary_col), NullColumn::create());
    auto c1 = NullableColumn::create(std::move(int_col), NullColumn::create());
    Columns fields{std::move(c0), std::move(c1)};
    std::vector<std::string> field_name{"c0", "c1"};
    return StructColumn::create(std::move(fields), std::move(field_name));
}

static void bench_func_str_10(benchmark::State& state) {
    int mode = state.range(0);
    int chunk_size = state.range(1);
    COWBench perf(mode, chunk_size, 10);
    perf.do_bench(state, TYPE_VARCHAR);
}

static void bench_func_str_100(benchmark::State& state) {
    int mode = state.range(0);
    int chunk_size = state.range(1);
    COWBench perf(mode, chunk_size, 100);
    perf.do_bench(state, TYPE_VARCHAR);
}

static void bench_func_str_1000(benchmark::State& state) {
    int mode = state.range(0);
    int chunk_size = state.range(1);
    COWBench perf(mode, chunk_size, 1000);
    perf.do_bench(state, TYPE_VARCHAR);
}

static void bench_func_decimal(benchmark::State& state) {
    int mode = state.range(0);
    int chunk_size = state.range(1);
    COWBench perf(mode, chunk_size);
    perf.do_bench(state, TYPE_DECIMAL);
}

static void bench_func_bigint(benchmark::State& state) {
    int mode = state.range(0);
    int chunk_size = state.range(1);
    COWBench perf(mode, chunk_size);
    perf.do_bench(state, TYPE_BIGINT);
}

static void bench_func_json(benchmark::State& state) {
    int mode = state.range(0);
    int chunk_size = state.range(1);
    COWBench perf(mode, chunk_size);
    perf.do_bench(state, TYPE_JSON);
}

static void bench_func_map(benchmark::State& state) {
    int mode = state.range(0);
    int chunk_size = state.range(1);
    COWBench perf(mode, chunk_size);
    perf.do_bench(state, TYPE_MAP);
}

static void bench_func_struct(benchmark::State& state) {
    int mode = state.range(0);
    int chunk_size = state.range(1);
    COWBench perf(mode, chunk_size);
    perf.do_bench(state, TYPE_STRUCT);
}

static void process_args1(benchmark::internal::Benchmark* b) {
    b->Args({1, 4096})->Iterations(100);
    b->Args({2, 4096})->Iterations(100);
    b->Args({3, 4096})->Iterations(100);
}

static void process_args2(benchmark::internal::Benchmark* b) {
    b->Args({1, 40960})->Iterations(100);
    b->Args({2, 40960})->Iterations(100);
    b->Args({3, 40960})->Iterations(100);
}

static void process_args3(benchmark::internal::Benchmark* b) {
    b->Args({1, 409600})->Iterations(10);
    b->Args({2, 409600})->Iterations(10);
    b->Args({3, 409600})->Iterations(10);
}

static void process_args4(benchmark::internal::Benchmark* b) {
    b->Args({1, 4096000})->Iterations(10);
    b->Args({2, 4096000})->Iterations(10);
    b->Args({3, 4096000})->Iterations(10);
}

BENCHMARK(bench_func_str_10)->Apply(process_args1);
BENCHMARK(bench_func_str_100)->Apply(process_args1);
BENCHMARK(bench_func_str_1000)->Apply(process_args1);
BENCHMARK(bench_func_decimal)->Apply(process_args1);
BENCHMARK(bench_func_bigint)->Apply(process_args1);
BENCHMARK(bench_func_json)->Apply(process_args1);
BENCHMARK(bench_func_struct)->Apply(process_args1);
BENCHMARK(bench_func_map)->Apply(process_args1);

BENCHMARK(bench_func_str_100)->Apply(process_args1);
BENCHMARK(bench_func_str_100)->Apply(process_args2);
BENCHMARK(bench_func_str_100)->Apply(process_args3);
BENCHMARK(bench_func_str_100)->Apply(process_args4);

} // namespace starrocks

BENCHMARK_MAIN();
