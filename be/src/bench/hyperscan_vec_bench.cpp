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
#include <hs/hs.h>
#include <testutil/assert.h>

#include <memory>
#include <random>
#include <vector>

#include "bench.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exprs/string_functions.h"

namespace starrocks {

class HyperScanBench {
public:
    void SetUp();
    void TearDown() {}

    HyperScanBench(size_t ratio) : _ratio(ratio) {}

    StatusOr<ColumnPtr> do_bench(benchmark::State& state, bool test_default_hs);

private:
    const TypeDescriptor type_desc = TypeDescriptor(TYPE_VARCHAR);
    size_t _ratio = 0;
    Columns _columns{};
    std::shared_ptr<StringFunctionsState> _state;
    std::string _rpl_value = "";
    size_t _num_rows = 4096;
};

void HyperScanBench::SetUp() {
    auto column = Bench::create_random_column(type_desc, _num_rows, false, false, 20);
    auto binary = down_cast<BinaryColumn*>(column.get());
    Bytes& data = binary->get_bytes();
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> dist(0, 99);
    for (size_t i = 0; i < data.size(); i++) {
        int random_number = dist(rng);
        if (random_number < 100 / _ratio) {
            data[i] = '-';
        }
    }
    _columns.push_back(std::move(column));
    MutableColumnPtr pattern_data = ColumnHelper::create_column(type_desc, false);
    pattern_data->append_datum(Datum(Slice("-")));
    auto pattern_column = ConstColumn::create(std::move(pattern_data), _num_rows);
    _columns.push_back(std::move(pattern_column));
    MutableColumnPtr rpl_data = ColumnHelper::create_column(type_desc, false);
    rpl_data->append_datum(Datum(Slice("")));
    auto rpl_column = ConstColumn::create(std::move(rpl_data), _num_rows);
    _columns.push_back(std::move(rpl_column));
    _state = std::make_shared<StringFunctionsState>();
    _state->options = std::make_unique<re2::RE2::Options>();
    _state->options->set_log_errors(false);
    _state->options->set_longest_match(true);
    _state->options->set_dot_nl(true);
    _state->const_pattern = true;
    std::string pattern = "-";
    _state->pattern = pattern;
    _state->use_hyperscan = true;
    _state->size_of_pattern = int(pattern.size());
    if (hs_compile(pattern.c_str(), HS_FLAG_ALLOWEMPTY | HS_FLAG_DOTALL | HS_FLAG_UTF8 | HS_FLAG_SOM_LEFTMOST,
                   HS_MODE_BLOCK, nullptr, &_state->database, &_state->compile_err) != HS_SUCCESS) {
        std::stringstream error;
        error << "Invalid regex expression: "
              << "-"
              << ": " << _state->compile_err->message;
        hs_free_compile_error(_state->compile_err);
        std::cout << error.str();
        return;
    }

    if (hs_alloc_scratch(_state->database, &_state->scratch) != HS_SUCCESS) {
        std::stringstream error;
        error << "ERROR: Unable to allocate scratch space.";
        hs_free_database(_state->database);
        std::cout << error.str();
        return;
    }
}

StatusOr<ColumnPtr> HyperScanBench::do_bench(benchmark::State& state, bool test_default_hyperscan) {
    if (test_default_hyperscan) {
        return StringFunctions::regexp_replace_use_hyperscan(_state.get(), _columns);
    } else {
        return StringFunctions::regexp_replace_use_hyperscan_vec(_state.get(), _columns);
    }
}

static void BM_HyperScan_Eval_Arg(benchmark::internal::Benchmark* b) {
    b->Args({5, true});
    b->Args({5, false});
    b->Args({10, true});
    b->Args({10, false});
    b->Args({20, true});
    b->Args({20, false});
    b->Args({50, true});
    b->Args({50, false});
    b->Args({100, true});
    b->Args({100, false});
    b->Iterations(10000);
}

static void BM_HyperScan_Eval(benchmark::State& state) {
    size_t ratio = state.range(0);
    bool test_default_hs = state.range(1);

    HyperScanBench hyperScanBench(ratio);
    hyperScanBench.SetUp();

    for (auto _ : state) {
        state.ResumeTiming();
        auto st = hyperScanBench.do_bench(state, test_default_hs);
        state.PauseTiming();
        ASSERT_TRUE(st.ok());
    }
}

BENCHMARK(BM_HyperScan_Eval)->Apply(BM_HyperScan_Eval_Arg);
/*
    -----------------------------------------------------------------------------------
    Benchmark                                         Time             CPU   Iterations
    -----------------------------------------------------------------------------------
    BM_HyperScan_Eval/5/1/iterations:10000      1427727 ns      1427629 ns        10000
    BM_HyperScan_Eval/5/0/iterations:10000      1082300 ns      1082400 ns        10000
    BM_HyperScan_Eval/10/1/iterations:10000     1002910 ns      1002870 ns        10000
    BM_HyperScan_Eval/10/0/iterations:10000      587874 ns       587929 ns        10000
    BM_HyperScan_Eval/20/1/iterations:10000      752652 ns       752632 ns        10000
    BM_HyperScan_Eval/20/0/iterations:10000      359769 ns       359775 ns        10000
    BM_HyperScan_Eval/50/1/iterations:10000      566782 ns       566772 ns        10000
    BM_HyperScan_Eval/50/0/iterations:10000      183832 ns       183852 ns        10000
    BM_HyperScan_Eval/100/1/iterations:10000     497867 ns       497860 ns        10000
    BM_HyperScan_Eval/100/0/iterations:10000     100563 ns       100588 ns        10000
     */

} // namespace starrocks

BENCHMARK_MAIN();