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

#include "exprs/binary_functions.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "column/fixed_length_column.h"
#include "exprs/math_functions.h"
#include "exprs/mock_vectorized_expr.h"
#include "testutil/column_test_helper.h"

namespace starrocks {

class BinaryFunctionsTest : public ::testing::Test {
public:
    BinaryFunctionsTest() {
        auto ctx_ptr = FunctionContext::create_test_context();
        ctx = std::unique_ptr<FunctionContext>(ctx_ptr);
        state = std::make_unique<BinaryFormatState>();
        ctx->set_function_state(FunctionContext::THREAD_LOCAL, state.get());
    }

    StatusOr<ColumnPtr> test_to_binary(const std::string& input, BinaryFormatType type) {
        Columns columns;
        columns.emplace_back(BinaryColumn::create());
        auto* arg1 = ColumnHelper::as_raw_column<BinaryColumn>(columns[0]);
        arg1->append(input);
        state->to_binary_type = type;
        return BinaryFunctions::to_binary(ctx.get(), columns);
    }

    StatusOr<ColumnPtr> test_from_binary(const Slice& input, BinaryFormatType type) {
        Columns columns;
        columns.emplace_back(BinaryColumn::create());
        auto* arg1 = ColumnHelper::as_raw_column<BinaryColumn>(columns[0]);
        arg1->append(input);
        state->to_binary_type = type;
        return BinaryFunctions::from_binary(ctx.get(), columns);
    }

    std::string hex_binary(const Slice& str) {
        std::stringstream ss;
        ss << std::hex << std::uppercase << std::setfill('0');
        for (int i = 0; i < str.size; ++i) {
            ss << std::setw(2) << (static_cast<int32_t>(str.data[i]) & 0xFF);
        }
        return ss.str();
    }

protected:
    std::unique_ptr<FunctionContext> ctx;
    std::unique_ptr<BinaryFormatState> state;
};

TEST_F(BinaryFunctionsTest, TestToBinaryNormal) {
    std::vector<std::tuple<BinaryFormatType, std::string, std::string>> good_cases = {
            {BinaryFormatType::HEX, "abab", "ABAB"},
            {BinaryFormatType::HEX, "00", "00"},
            {BinaryFormatType::ENCODE64, "QUJBQg==", "ABAB"},
            {BinaryFormatType::ENCODE64, "MDA=", "00"},
            {BinaryFormatType::UTF8, "abab", "abab"},
            {BinaryFormatType::UTF8, "abab", "abab"},
            {BinaryFormatType::UTF8, "0", "0"},
            {BinaryFormatType::UTF8, "0Xx", "0Xx"},
    };

    for (auto& c : good_cases) {
        auto [binary_type, arg, expect] = c;
        auto result = test_to_binary(arg, binary_type);
        ASSERT_TRUE(result.ok());

        // const auto cv = ColumnHelper::as_column<ConstColumn>(result.value());
        auto v = ColumnHelper::as_column<BinaryColumn>(result.value());
        ASSERT_TRUE(!v->is_null(0));
        ASSERT_EQ(v->size(), 1);
        if (binary_type == BinaryFormatType::HEX) {
            ASSERT_EQ(Slice(expect).to_string(), hex_binary(v->get_data()[0]));
        } else {
            ASSERT_EQ(Slice(expect), v->get_data()[0]);
        }
    }

    std::vector<std::tuple<BinaryFormatType, std::string>> bad_cases = {
            {BinaryFormatType::HEX, "0"},
            {BinaryFormatType::HEX, "AX"},
    };
    for (auto& c : bad_cases) {
        auto [binary_type, arg] = c;
        auto result = test_to_binary(arg, binary_type);
        ASSERT_TRUE(result.ok());
        auto v = ColumnHelper::as_column<BinaryColumn>(result.value());
        // TODO: Return null if input is invalid.
        ASSERT_FALSE(v->is_null(0));
        ASSERT_EQ(Slice(""), v->get_data()[0]);
    }
}

TEST_F(BinaryFunctionsTest, TestToBinaryNull) {
    auto arg = ColumnHelper::create_const_null_column(2);
    state->to_binary_type = BinaryFormatType::HEX;
    auto result = BinaryFunctions::to_binary(ctx.get(), {std::move(arg)});
    ASSERT_TRUE(result.ok());
    const auto v = ColumnHelper::as_column<ConstColumn>(result.value());
    ASSERT_EQ(v->size(), 2);
    ASSERT_TRUE(v->is_null(0));
    ASSERT_TRUE(v->is_null(1));
}

TEST_F(BinaryFunctionsTest, TestFromToBinaryNormal) {
    std::vector<std::tuple<BinaryFormatType, std::string, std::string>> good_cases = {
            {BinaryFormatType::HEX, "abab", "ABAB"},
            {BinaryFormatType::HEX, "00", "00"},
            {BinaryFormatType::ENCODE64, "QUJBQg==", "QUJBQg=="},
            {BinaryFormatType::ENCODE64, "MDA=", "MDA="},
            {BinaryFormatType::UTF8, "abab", "abab"},
            {BinaryFormatType::UTF8, "abab", "abab"},
            {BinaryFormatType::UTF8, "0", "0"},
            {BinaryFormatType::UTF8, "0Xx", "0Xx"},
    };

    for (auto& c : good_cases) {
        auto [binary_type, arg, expect] = c;
        auto result = test_to_binary(arg, binary_type);
        ASSERT_TRUE(result.ok());

        auto v = ColumnHelper::as_column<BinaryColumn>(result.value());
        ASSERT_TRUE(!v->is_null(0));
        ASSERT_EQ(v->size(), 1);

        auto result_vv = test_from_binary(v->get_data()[0], binary_type);
        auto vv = ColumnHelper::as_column<BinaryColumn>(result_vv.value());
        ASSERT_TRUE(!vv->is_null(0));
        ASSERT_EQ(vv->size(), 1);
        ASSERT_EQ(Slice(expect), vv->get_data()[0]);
    }
}

} // namespace starrocks
