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
#include <gtest/gtest.h>

#include <memory>
#include <numeric>
#include <random>
#include <vector>

#include "column/column_hash.h"
#include "exprs/string_functions.h"
#include "util/memcmp.h"

namespace starrocks {

static std::string kAlphaNumber =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

static size_t ROUND_NUM = 1000;

static size_t PADDED_BYTES = 15;

template <bool padded, bool equal>
static void do_MemEqual(benchmark::State& state) {
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> beginning_gen(0, 40);
    std::uniform_int_distribution<int> length_gen(1, 16);

    std::vector<int> beginnings;
    std::vector<int> lengths;

    for (size_t i = 0; i < ROUND_NUM; i++) {
        beginnings.emplace_back(beginning_gen(rng));
        lengths.emplace_back(length_gen(rng));
    }

    auto sum = std::accumulate(lengths.begin(), lengths.end(), 0);
    std::vector<char> buffer_0(sum + PADDED_BYTES);
    std::vector<char> buffer_1(sum + PADDED_BYTES);

    auto acc_begin = 0;
    for (size_t i = 0; i < ROUND_NUM; i++) {
        memcpy(buffer_0.data() + acc_begin, kAlphaNumber.data() + beginnings[i], lengths[i]);
        memcpy(buffer_1.data() + acc_begin, kAlphaNumber.data() + beginnings[i], lengths[i]);
        if constexpr (!equal) {
            std::uniform_int_distribution<int> index_gen(0, lengths[i] - 1);
            auto index = index_gen(rng);
            buffer_1[acc_begin + index] = 'z';
        }
        beginnings[i] = acc_begin;
        acc_begin += lengths[i];
    }

    for (auto _ : state) {
        state.ResumeTiming();
        bool and_res = true;
        bool or_res = false;
        bool res = false;
        for (size_t i = 0; i < ROUND_NUM; i++) {
            if constexpr (padded) {
                res = memequal_padded(buffer_0.data() + beginnings[i], lengths[i], buffer_1.data() + beginnings[i],
                                      lengths[i]);
            } else {
                res = memequal(buffer_0.data() + beginnings[i], lengths[i], buffer_1.data() + beginnings[i],
                               lengths[i]);
            }
            and_res &= res;
            or_res |= res;
        }
        state.PauseTiming();
        if constexpr (equal) {
            ASSERT_TRUE(and_res);
        } else {
            ASSERT_FALSE(or_res);
        }
    }
}

static void process_args(benchmark::internal::Benchmark* b) {
    b->Iterations(100000);
}

static void BM_memequal_padded_equal(benchmark::State& state) {
    do_MemEqual<true, true>(state);
}

static void BM_memequal_padded_notequal(benchmark::State& state) {
    do_MemEqual<true, false>(state);
}

static void BM_memequal_no_padded_equal(benchmark::State& state) {
    do_MemEqual<false, true>(state);
}

static void BM_memequal_no_padded_notequal(benchmark::State& state) {
    do_MemEqual<false, true>(state);
}

BENCHMARK(BM_memequal_padded_equal)->Apply(process_args);
BENCHMARK(BM_memequal_padded_notequal)->Apply(process_args);
BENCHMARK(BM_memequal_no_padded_equal)->Apply(process_args);
BENCHMARK(BM_memequal_no_padded_notequal)->Apply(process_args);

/*
-------------------------------------------------------------------------------------------
Benchmark                                                 Time             CPU   Iterations
-------------------------------------------------------------------------------------------
BM_memequal_padded_equal/iterations:100000             1807 ns         1813 ns       100000
BM_memequal_padded_notequal/iterations:100000          2547 ns         2552 ns       100000
BM_memequal_no_padded_equal/iterations:100000          8560 ns         8575 ns       100000
BM_memequal_no_padded_notequal/iterations:100000       8272 ns         8286 ns       100000
*/

/* if we fix the string's length as 8, the result is as below,
 * it makes sense that memequal_no_padded compare uint64 and
 * memequal compare __m128i
-------------------------------------------------------------------------------------------
Benchmark                                                 Time             CPU   Iterations
-------------------------------------------------------------------------------------------
BM_memequal_padded_equal/iterations:100000             2456 ns         2463 ns       100000
BM_memequal_padded_notequal/iterations:100000          2495 ns         2502 ns       100000
BM_memequal_no_padded_equal/iterations:100000          2107 ns         2114 ns       100000
BM_memequal_no_padded_notequal/iterations:100000       2110 ns         2117 ns       100000
 */

} // namespace starrocks

BENCHMARK_MAIN();