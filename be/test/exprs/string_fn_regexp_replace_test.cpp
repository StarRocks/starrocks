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

#include <base/testutil/assert.h>
#include <gtest/gtest.h>
#include <hs/hs.h>

#include <memory>
#include <random>
#include <vector>

#include "base/utility/defer_op.h"
#include "bench/bench_util.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "exprs/string_functions.h"

namespace starrocks {

class StringFunctionRegexpReplaceTest : public ::testing::Test {
public:
    void init(size_t ratio) {
        _columns.clear();
        _ratio = ratio;
        auto column = BenchUtil::create_random_string_column(_num_rows, 20);
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
        _state->use_hyperscan_vec = true;
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

private:
    const TypeDescriptor type_desc = TypeDescriptor(TYPE_VARCHAR);
    size_t _ratio = 0;
    Columns _columns{};
    std::shared_ptr<StringFunctionsState> _state;
    std::string _rpl_value = "";
    size_t _num_rows = 4096;
};

TEST_F(StringFunctionRegexpReplaceTest, testHyperscanVec) {
    std::vector<size_t> ratio_cases = {5, 10, 20, 50, 100};
    for (size_t r : ratio_cases) {
        init(r);
        auto r_vec = StringFunctions::regexp_replace_use_hyperscan_vec(_state.get(), _columns);
        ASSERT_TRUE(r_vec.ok());
        auto r_ori = StringFunctions::regexp_replace_use_hyperscan(_state.get(), _columns);
        ASSERT_TRUE(r_ori.ok());
        auto ori = r_ori.value();
        auto vec = r_vec.value();
        for (size_t i = 0; i < _num_rows; i++) {
            ASSERT_EQ(vec->debug_item(i), ori->debug_item(i));
        }
    }
}

TEST_F(StringFunctionRegexpReplaceTest, testMultipleRowsWithPackagePattern) {
    // Test case for the reported issue with '_package_.*' pattern
    const TypeDescriptor type_desc = TypeDescriptor(TYPE_VARCHAR);
    Columns columns;

    // Create input column with two rows
    auto str_column = BinaryColumn::create();
    str_column->append("activity_60_package_2");
    str_column->append("activity_50_package_7");
    columns.push_back(std::move(str_column));

    // Create pattern column (constant)
    MutableColumnPtr pattern_data = ColumnHelper::create_column(type_desc, false);
    pattern_data->append_datum(Datum(Slice("_package_.*")));
    auto pattern_column = ConstColumn::create(std::move(pattern_data), 2);
    columns.push_back(std::move(pattern_column));

    // Create replacement column (constant empty string)
    MutableColumnPtr rpl_data = ColumnHelper::create_column(type_desc, false);
    rpl_data->append_datum(Datum(Slice("")));
    auto rpl_column = ConstColumn::create(std::move(rpl_data), 2);
    columns.push_back(std::move(rpl_column));

    // Setup state with hyperscan
    auto state = std::make_shared<StringFunctionsState>();
    state->options = std::make_unique<re2::RE2::Options>();
    state->options->set_log_errors(false);
    state->options->set_longest_match(true);
    state->options->set_dot_nl(true);
    state->const_pattern = true;
    std::string pattern = "_package_.*";
    state->pattern = pattern;
    state->use_hyperscan = false;
    state->global_mode = true; // Pattern doesn't start with ^ or end with $
    state->size_of_pattern = int(pattern.size());

    if (hs_compile(pattern.c_str(), HS_FLAG_ALLOWEMPTY | HS_FLAG_DOTALL | HS_FLAG_UTF8 | HS_FLAG_SOM_LEFTMOST,
                   HS_MODE_BLOCK, nullptr, &state->database, &state->compile_err) != HS_SUCCESS) {
        std::stringstream error;
        error << "Invalid regex expression: " << pattern << ": " << state->compile_err->message;
        hs_free_compile_error(state->compile_err);
        LOG(FATAL) << error.str();
    }

    if (hs_alloc_scratch(state->database, &state->scratch) != HS_SUCCESS) {
        hs_free_database(state->database);
        LOG(FATAL) << "ERROR: Unable to allocate scratch space.";
    }

    // Ensure cleanup happens even if assertions fail
    DeferOp cleanup([&] {
        if (state->scratch != nullptr) {
            hs_free_scratch(state->scratch);
            state->scratch = nullptr;
        }
        if (state->database != nullptr) {
            hs_free_database(state->database);
            state->database = nullptr;
        }
    });

    // Test non-vectorized version (expected to be correct)
    auto r_ori = StringFunctions::regexp_replace_use_hyperscan(state.get(), columns);
    ASSERT_TRUE(r_ori.ok());
    auto ori_result = r_ori.value();

    // Both should produce "activity_60" and "activity_50"
    ASSERT_EQ(ori_result->debug_item(0), "'activity_60'");
    ASSERT_EQ(ori_result->debug_item(1), "'activity_50'");
}

} // namespace starrocks
