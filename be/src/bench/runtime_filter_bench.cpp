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

#include <random>

#include "bench.h"
#include "column/column_helper.h"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_bank.h"
#include "simd/simd.h"
#include "util/time.h"

namespace starrocks {

//   --------------------------------------------------------------------------------------------------------
//   Benchmark                                              Time             CPU   Iterations UserCounters...
//   --------------------------------------------------------------------------------------------------------
//   Benchmark_RuntimeFilter_Eval/20000000/2/3      835640991 ns    835534547 ns            1 compute_hash_time(ms)=230 evalute_time(ms)=140 items_per_second=23.9368M/s
//   Benchmark_RuntimeFilter_Eval/20000000/2/30     966175505 ns    966085792 ns            1 compute_hash_time(ms)=222 evalute_time(ms)=162 items_per_second=20.7021M/s
//   Benchmark_RuntimeFilter_Eval/20000000/2/100    808598462 ns    808448159 ns            1 compute_hash_time(ms)=218 evalute_time(ms)=136 items_per_second=24.7388M/s
//   Benchmark_RuntimeFilter_Eval/20000000/10/3     968277201 ns    968155576 ns            1 compute_hash_time(ms)=221 evalute_time(ms)=162 items_per_second=20.6578M/s
//   Benchmark_RuntimeFilter_Eval/20000000/10/30   1196354237 ns   1196243430 ns            1 compute_hash_time(ms)=222 evalute_time(ms)=201 items_per_second=16.719M/s
//   Benchmark_RuntimeFilter_Eval/20000000/10/100   812834490 ns    812759413 ns            1 compute_hash_time(ms)=211 evalute_time(ms)=136 items_per_second=24.6075M/s
//   Benchmark_RuntimeFilter_Eval/20000000/100/3    730210325 ns    730142433 ns            1 compute_hash_time(ms)=216 evalute_time(ms)=122 items_per_second=27.3919M/s
//   Benchmark_RuntimeFilter_Eval/20000000/100/30   944613552 ns    944525660 ns            1 compute_hash_time(ms)=211 evalute_time(ms)=158 items_per_second=21.1746M/s
//   Benchmark_RuntimeFilter_Eval/20000000/100/100  968723953 ns    968634431 ns            1 compute_hash_time(ms)=213 evalute_time(ms)=163 items_per_second=20.6476M/s

static void do_benchmark_hash_partitioned(benchmark::State& state, TRuntimeFilterBuildJoinMode::type join_mode,
                                          Columns columns, int64_t num_rows, int64_t num_partitions) {
    std::vector<uint32_t> hash_values;
    std::vector<size_t> num_rows_per_partitions(num_partitions, 0);

    hash_values.assign(num_rows, HashUtil::FNV_SEED);
    for (auto& column : columns) {
        column->fnv_hash(hash_values.data(), 0, num_rows);
    }
    for (auto i = 0; i < num_rows; ++i) {
        hash_values[i] %= num_partitions;
        ++num_rows_per_partitions[hash_values[i]];
    }

    RuntimeFilter::RunningContext running_ctx;
    running_ctx.selection.assign(num_rows, 2);
    running_ctx.use_merged_selection = false;
    running_ctx.compatibility = true;

    std::vector<Column*> column_ptrs;
    column_ptrs.reserve(columns.size());
    for (auto& column : columns) {
        column_ptrs.push_back(column.get());
    }

    int32_t num_column = columns.size();
    std::vector<TRuntimeBloomFilter<TYPE_INT>> bfs(num_column * num_partitions);
    std::vector<TRuntimeBloomFilter<TYPE_INT>> gfs(num_column);
    for (int i = 0; i < num_column; i++) {
        auto& column = columns[i];
        for (auto p = 0; p < num_partitions; ++p) {
            auto pp = p + (i * num_partitions);
            bfs[pp].init(num_rows_per_partitions[p]);
        }
        for (auto j = 0; j < num_rows; ++j) {
            auto ele = column->get(j).get_int32();
            auto pp = hash_values[j] + (i * num_partitions);
            bfs[pp].insert(ele);
        }

        for (auto p = 0; p < num_partitions; ++p) {
            auto pp = p + (i * num_partitions);
            gfs[i].concat(&bfs[pp]);
        }
        ASSERT_EQ(gfs[i].size(), num_rows);
        ASSERT_EQ(gfs[i].num_hash_partitions(), num_partitions);
    }
    // compute hash
    {
        RuntimeFilterLayout layout;
        layout.init(1, {});

        int64_t t0 = MonotonicMillis();
        auto& grf = gfs[0];
        grf.set_join_mode(join_mode);
        grf.compute_partition_index(layout, column_ptrs, &running_ctx);
        int64_t t1 = MonotonicMillis();
        state.counters["compute_hash_time(ms)"] = t1 - t0;
        // auto& ctx_hash_values = running_ctx.hash_values;
        // for (auto i = 0; i < num_rows; i++) {
        //     std::cout<<"ctx_hash_values:"<<ctx_hash_values[i]<<", hash_values:"<<hash_values[i]<<std::endl;
        // }
    }

    state.SetItemsProcessed(num_rows);
    state.ResumeTiming();
    int64_t total_evalute_time = 0;
    int64_t iterate_times = 1;
    for (int i = 0; i < num_column; i++) {
        int64_t t0 = MonotonicMillis();
        auto& grf = gfs[i];

        while (state.KeepRunningBatch(1)) {
            grf.set_join_mode(join_mode);
            grf.evaluate(column_ptrs[i], &running_ctx);
        }

        auto true_count = SIMD::count_nonzero(running_ctx.selection.data(), num_rows);
        ASSERT_EQ(true_count, num_rows);
        total_evalute_time += MonotonicMillis() - t0;
    }
    if (!num_column) {
        state.counters["evalute_time(ms)"] = 0;
    } else {
        state.counters["evalute_time(ms)"] = total_evalute_time / iterate_times / num_column;
    }
    state.PauseTiming();
}

static Columns columns;
class RuntimeFilterBench {
public:
    static void Setup(int32_t num_rows, int32_t num_column) {
        if (columns.empty()) {
            for (int i = 0; i < num_column; i++) {
                auto type_desc = TypeDescriptor(TYPE_INT);
                auto column = Bench::create_series_column(type_desc, num_rows);
                columns.push_back(std::move(column));
            }
            std::cout << "generate num_rows:" << num_rows << std::endl;
        }
    }
};

static void RuntimeFilterArg1(benchmark::internal::Benchmark* b) {
    std::vector<int64_t> bm_num_rows = {20000000};
    std::vector<int64_t> bm_num_columns = {2, 10, 100};
    std::vector<int64_t> bm_num_partitions = {3, 30, 100};
    for (auto& num_rows : bm_num_rows) {
        for (auto& num_column : bm_num_columns) {
            for (auto& num_partitions : bm_num_partitions) {
                b->Args({num_rows, num_column, num_partitions});
            }
        }
    }
}

static void Benchmark_RuntimeFilter_Eval(benchmark::State& state) {
    // auto column = gen_random_binary_column(alphabet0, 10, num_rows);
    auto num_rows = state.range(0);
    auto num_column = state.range(1);
    RuntimeFilterBench::Setup(num_rows, num_column);
    do_benchmark_hash_partitioned(state, TRuntimeFilterBuildJoinMode::PARTITIONED, columns, num_rows, state.range(2));
}

BENCHMARK(Benchmark_RuntimeFilter_Eval)->Apply(RuntimeFilterArg1);

} // namespace starrocks

BENCHMARK_MAIN();
