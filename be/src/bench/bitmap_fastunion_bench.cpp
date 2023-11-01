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
#include <gtest/gtest.h>

#include <iostream>
#include <random>
#include <vector>

#include "types/bitmap_value.h"
#include "types/bitmap_value_detail.h"
#include "util/phmap/phmap.h"

namespace starrocks::vectorized {
// Benchmark_Bitmap_Fast_Union_Eval/{bitmap_num}/{size per bitmap}/{max value}/{fast_union or not}
// -------------------------------------------------------------------------------------------------------------
// Benchmark                                                                   Time             CPU   Iterations
// -------------------------------------------------------------------------------------------------------------
// Benchmark_Bitmap_Fast_Union_Eval/10/1000/10000/0/iterations:10          60464 ns        59632 ns            0
// Benchmark_Bitmap_Fast_Union_Eval/10/1000/10000/1/iterations:10          47691 ns        47680 ns            0
// Benchmark_Bitmap_Fast_Union_Eval/10/1000/1000000/0/iterations:10       112053 ns       111739 ns            0
// Benchmark_Bitmap_Fast_Union_Eval/10/1000/1000000/1/iterations:10        97275 ns        97313 ns            0
// Benchmark_Bitmap_Fast_Union_Eval/10/10000/10000/0/iterations:10          6713 ns         6092 ns            0
// Benchmark_Bitmap_Fast_Union_Eval/10/10000/10000/1/iterations:10          6091 ns         6051 ns            0
// Benchmark_Bitmap_Fast_Union_Eval/10/10000/1000000/0/iterations:10      317602 ns       317235 ns            0
// Benchmark_Bitmap_Fast_Union_Eval/10/10000/1000000/1/iterations:10      268118 ns       268203 ns            0
// Benchmark_Bitmap_Fast_Union_Eval/100/1000/10000/0/iterations:10        139254 ns       138790 ns            0
// Benchmark_Bitmap_Fast_Union_Eval/100/1000/10000/1/iterations:10        145697 ns       145693 ns            0
// Benchmark_Bitmap_Fast_Union_Eval/100/1000/1000000/0/iterations:10     2221573 ns      2221237 ns            0
// Benchmark_Bitmap_Fast_Union_Eval/100/1000/1000000/1/iterations:10     2166279 ns      2166364 ns            0
// Benchmark_Bitmap_Fast_Union_Eval/100/10000/10000/0/iterations:10        50896 ns        50305 ns            0
// Benchmark_Bitmap_Fast_Union_Eval/100/10000/10000/1/iterations:10        31820 ns        31820 ns            0
// Benchmark_Bitmap_Fast_Union_Eval/100/10000/1000000/0/iterations:10    1407210 ns      1406619 ns            0
// Benchmark_Bitmap_Fast_Union_Eval/100/10000/1000000/1/iterations:10    1209283 ns      1209609 ns            0
static void do_benchmark_bitmap_union(benchmark::State& state, const std::vector<const BitmapValue*>& values,
                                      bool fast_union, int64_t expected_num) {
    state.ResumeTiming();
    BitmapValue empty;
    if (!fast_union) {
        for (int i = 0; i < values.size(); i++) {
            empty |= *values[i];
        }
        ASSERT_EQ(empty.cardinality(), expected_num);
    } else {
        empty.fast_union(values);
        ASSERT_EQ(empty.cardinality(), expected_num);
    }
    state.PauseTiming();
}

class Bitmap_Fast_Union_Bench {
public:
    static std::vector<int> generate_random_sequence(int count, int sum) {
        std::vector<int> arr(count, 0); // initialize array with all elements set to 0
        std::random_device rd;
        std::mt19937 gen(rd());

        for (int i = 0; i < count - 1; ++i) {
            arr[i] = gen() % sum;
        }
        arr[count - 1] = sum;

        std::sort(arr.begin(), arr.end());
        for (int i = count - 1; i > 0; --i) {
            arr[i] -= arr[i - 1];
        }

        return arr;
    }

    // alpha is a load factor
    static std::vector<const BitmapValue*> Setup(std::vector<const BitmapValue*>& values, size_t& expected_num,
                                                 size_t bitmap_num, int32_t num_per_bucket, uint64_t max_value) {
        phmap::flat_hash_set<uint64_t> _set;
        for (int i = 0; i < bitmap_num; ++i) {
            BitmapValue* tmp = new BitmapValue();
            for (int j = 0; j < num_per_bucket; ++j) {
                uint64_t v = rand() % max_value;
                _set.insert(v);
                tmp->add(v);
            }
            values.push_back(tmp);
        }
        expected_num = _set.size();
        return values;
    }
};

static void Bitmap_Fast_Union_Arg(benchmark::internal::Benchmark* b) {
    std::vector<int64_t> bm_bm_num = {10, 100};
    std::vector<int64_t> bm_bm_size = {1'000, 10'000};
    std::vector<int64_t> bm_max_value = {10'000, 1'000'000};
    std::vector<int64_t> bm_fast_or_not = {0, 1};
    for (auto& bm_num : bm_bm_num) {
        for (auto& bm_size : bm_bm_size) {
            for (auto& max_value : bm_max_value) {
                for (auto& fast_or_not : bm_fast_or_not) {
                    b->Args({bm_num, bm_size, max_value, fast_or_not});
                }
            }
        }
    }
}

static std::vector<const BitmapValue*> values;
static size_t expected_num;
static void Benchmark_Bitmap_Fast_Union_Eval(benchmark::State& state) {
    auto bm_num = state.range(0);
    auto bm_size = state.range(1);
    auto max_value = state.range(2);
    auto fast_or_not = state.range(3) == 0 ? false : true;

    if (!fast_or_not) {
        // clear bitmap vectors
        for (auto& ptr : values) {
            delete ptr;
        }
        values.clear();
        Bitmap_Fast_Union_Bench::Setup(values, expected_num, bm_num, bm_size, max_value);
    }

    // do benchmark
    do_benchmark_bitmap_union(state, values, fast_or_not, expected_num);
}

BENCHMARK(Benchmark_Bitmap_Fast_Union_Eval)->Iterations(10)->Apply(Bitmap_Fast_Union_Arg);

} // namespace starrocks::vectorized

BENCHMARK_MAIN();
