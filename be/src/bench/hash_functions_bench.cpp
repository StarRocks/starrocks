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
#include <testutil/assert.h>

#include <memory>
#include <vector>

#include "bench.h"
#include "exprs/hash_functions.h"

namespace starrocks {

class HashFunctionsBench {
public:
    void SetUp();
    void TearDown() {}

    HashFunctionsBench(size_t num_column, size_t num_rows) : _num_column(num_column), _num_rows(num_rows) {}

    void do_bench(benchmark::State& state, size_t num_column, bool test_default_hash);

private:
    const TypeDescriptor type_desc = TypeDescriptor(TYPE_VARCHAR);
    size_t _num_column = 0;
    size_t _num_rows = 0;
    Columns _columns{};
};

void HashFunctionsBench::SetUp() {
    for (int i = 0; i < _num_column; i++) {
        auto columnPtr = Bench::create_random_column(type_desc, _num_rows, false, false, 32);
        _columns.push_back(std::move(columnPtr));
    }
}

void HashFunctionsBench::do_bench(benchmark::State& state, size_t num_rows, bool test_default_hash) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    if (test_default_hash) {
        ColumnPtr result = HashFunctions::murmur_hash3_32(ctx.get(), _columns).value();
        auto column = ColumnHelper::cast_to<TYPE_INT>(result);
    } else {
        ColumnPtr result = HashFunctions::xx_hash3_64(ctx.get(), _columns).value();
        auto column = ColumnHelper::cast_to<TYPE_BIGINT>(result);
    }
}

static void BM_HashFunctions_Eval_Arg(benchmark::internal::Benchmark* b) {
    b->Args({10, true});
    b->Args({10, false});
    b->Args({100, true});
    b->Args({100, false});
    b->Args({10000, true});
    b->Args({10000, false});
    b->Args({1000000, true});
    b->Args({1000000, false});
    b->Iterations(10000);
}

static void BM_HashFunctions_Eval(benchmark::State& state) {
    size_t num_rows = state.range(0);
    bool test_default_hash = state.range(1);

    HashFunctionsBench hashFunctionsBench(1, num_rows);
    hashFunctionsBench.SetUp();

    for (auto _ : state) {
        hashFunctionsBench.do_bench(state, num_rows, test_default_hash);
    }
}

BENCHMARK(BM_HashFunctions_Eval)->Apply(BM_HashFunctions_Eval_Arg);

} // namespace starrocks

BENCHMARK_MAIN();