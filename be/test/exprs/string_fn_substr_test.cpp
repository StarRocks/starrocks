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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <random>

#include "butil/time.h"
#include "exprs/mock_vectorized_expr.h"
#include "exprs/string_functions.h"

namespace starrocks {

class StringFunctionSubstrTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    }

public:
    TExprNode expr_node;
};

TEST_F(StringFunctionSubstrTest, substringNormalTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto str = BinaryColumn::create();
    auto pos = Int32Column::create();
    auto len = Int32Column::create();
    for (int j = 0; j < 20; ++j) {
        str->append("test" + std::to_string(j));
        pos->append(5);
        len->append(2);
    }

    columns.emplace_back(std::move(str));
    columns.emplace_back(std::move(pos));
    columns.emplace_back(std::move(len));

    ColumnPtr result = StringFunctions::substring(ctx.get(), columns).value();

    ASSERT_TRUE(result->is_binary());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<BinaryColumn>(result);
    for (int k = 0; k < 20; ++k) {
        ASSERT_EQ(std::to_string(k), v->get_data()[k].to_string());
    }
}

TEST_F(StringFunctionSubstrTest, substringChineseTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto str = BinaryColumn::create();
    auto pos = Int32Column::create();
    auto len = Int32Column::create();
    for (int j = 0; j < 20; ++j) {
        str->append("我是中文字符串！！！" + std::to_string(j));
        pos->append(3);
        len->append(2);
    }

    columns.emplace_back(std::move(str));
    columns.emplace_back(std::move(pos));
    columns.emplace_back(std::move(len));

    ColumnPtr result = StringFunctions::substring(ctx.get(), columns).value();

    ASSERT_TRUE(result->is_binary());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<BinaryColumn>(result);
    for (int k = 0; k < 20; ++k) {
        ASSERT_EQ(Slice("中文"), v->get_data()[k]);
    }
}

TEST_F(StringFunctionSubstrTest, substringleftTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto str = BinaryColumn::create();
    auto pos = Int32Column::create();
    auto len = Int32Column::create();
    for (int j = 0; j < 10; ++j) {
        str->append("我是中文字符串" + std::to_string(j));
        pos->append(-2);
        len->append(2);
    }

    columns.emplace_back(std::move(str));
    columns.emplace_back(std::move(pos));
    columns.emplace_back(std::move(len));

    ColumnPtr result = StringFunctions::substring(ctx.get(), columns).value();

    ASSERT_TRUE(result->is_binary());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<BinaryColumn>(result);
    for (int k = 0; k < 10; ++k) {
        ASSERT_EQ(Slice(std::string("串") + std::to_string(k)), v->get_data()[k]);
    }
}

TEST_F(StringFunctionSubstrTest, substrConstASCIITest) {
    auto str = BinaryColumn::create();
    str->append("123456789");
    str->append("");
    std::vector<std::tuple<int, int, std::string>> cases = {
            {1, 2, "12"},          {1, 0, ""},   {2, 100, "23456789"}, {9, 1, "9"},
            {9, 100, "9"},         {10, 1, ""},  {-9, 1, "1"},         {-9, 9, "123456789"},
            {-9, 10, "123456789"}, {-4, 1, "6"}, {-4, 4, "6789"},      {-4, 5, "6789"},
            {-1, 1, "9"},          {-1, 2, "9"}, {0, 1, ""},           {1, INT_MAX, "123456789"},
            {1, -2, ""},           {-3, -2, ""}, {-3, INT_MIN, ""},    {1, INT_MIN, ""},
    };

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto state = std::make_unique<SubstrState>();
    ctx->set_function_state(FunctionContext::FRAGMENT_LOCAL, state.get());
    starrocks::Columns columns;
    columns.emplace_back(std::move(str));
    for (auto& e : cases) {
        auto [offset, len, expect] = e;
        state->is_const = true;
        state->pos = offset;
        state->len = len;
        ColumnPtr result = StringFunctions::substring(ctx.get(), columns).value();
        auto* binary = down_cast<BinaryColumn*>(result.get());
        ASSERT_EQ(binary->size(), 2);
        ASSERT_EQ(binary->get_slice(0).to_string(), expect);
        ASSERT_EQ(binary->get_slice(1).to_string(), "");
    }
}

TEST_F(StringFunctionSubstrTest, substrConstZhTest) {
    auto str = BinaryColumn::create();
    str->append("壹贰叁肆伍陆柒捌玖");
    str->append("");

    std::vector<std::tuple<int, int, std::string>> cases = {
            {1, 2, "壹贰"},
            {1, 0, ""},
            {2, 100, "贰叁肆伍陆柒捌玖"},
            {9, 1, "玖"},
            {9, 100, "玖"},
            {10, 1, ""},
            {-9, 1, "壹"},
            {-9, 9, "壹贰叁肆伍陆柒捌玖"},
            {-9, 10, "壹贰叁肆伍陆柒捌玖"},
            {-4, 1, "陆"},
            {-4, 4, "陆柒捌玖"},
            {-4, 5, "陆柒捌玖"},
            {-1, 1, "玖"},
            {-1, 2, "玖"},
            {0, 1, ""},
            {INT_MIN, 1, ""},
            {1, -1, ""},
            {1, -10, ""},
            {-2, -1, ""},
            {-2, -10, ""},
            {-2, INT_MIN, ""},
            {1, INT_MAX, "壹贰叁肆伍陆柒捌玖"},
            {1, INT_MIN, ""},
    };
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto state = std::make_unique<SubstrState>();
    ctx->set_function_state(FunctionContext::FRAGMENT_LOCAL, state.get());
    starrocks::Columns columns;
    columns.emplace_back(std::move(str));
    for (auto& e : cases) {
        auto [offset, len, expect] = e;
        state->is_const = true;
        state->pos = offset;
        state->len = len;
        ColumnPtr result = StringFunctions::substring(ctx.get(), columns).value();
        auto* binary = down_cast<BinaryColumn*>(result.get());
        ASSERT_EQ(binary->get_slice(0).to_string(), expect);
        ASSERT_EQ(binary->get_slice(1).to_string(), "");
    }
}

TEST_F(StringFunctionSubstrTest, substrConstUtf8Test) {
    auto str = BinaryColumn::create();
    std::string s;
    s.append("\x7f");
    s.append("\xdf\xbf");
    s.append("\xe0\xbf\xbf");
    s.append("\xf3\xbf\xbf\xbf");
    s.append("\x7f");
    s.append("\xdf\xbf");
    s.append("\xe0\xbf\xbf");
    s.append("\xf3\xbf\xbf\xbf");
    s.append("\x7f");
    s.append("\xdf\xbf");
    s.append("\xe0\xbf\xbf");
    s.append("\xf3\xbf\xbf\xbf");

    str->append(s);
    str->append("");

    std::vector<std::tuple<int, int, std::string>> cases = {
            {1, 1, "\x7f"},
            {1, 2, "\x7f\xdf\xbf"},
            {1, 3, "\x7f\xdf\xbf\xe0\xbf\xbf"},
            {1, 4, "\x7f\xdf\xbf\xe0\xbf\xbf\xf3\xbf\xbf\xbf"},
            {1, 100, s},
            {2, 1, "\xdf\xbf"},
            {2, 2, "\xdf\xbf\xe0\xbf\xbf"},
            {2, 3, "\xdf\xbf\xe0\xbf\xbf\xf3\xbf\xbf\xbf"},
            {2, 4, "\xdf\xbf\xe0\xbf\xbf\xf3\xbf\xbf\xbf\x7f"},
            {2, 100, s.substr(1)},
            {3, 1, "\xe0\xbf\xbf"},
            {3, 2, "\xe0\xbf\xbf\xf3\xbf\xbf\xbf"},
            {3, 3, "\xe0\xbf\xbf\xf3\xbf\xbf\xbf\x7f"},
            {3, 4, "\xe0\xbf\xbf\xf3\xbf\xbf\xbf\x7f\xdf\xbf"},
            {3, 100, s.substr(3)},
            {4, 2, "\xf3\xbf\xbf\xbf\x7f"},
            {4, 3, "\xf3\xbf\xbf\xbf\x7f\xdf\xbf"},
            {4, 4, "\xf3\xbf\xbf\xbf\x7f\xdf\xbf\xe0\xbf\xbf"},
            {4, 100, s.substr(6)},
            {5, 1, "\x7f"},
            {5, 2, "\x7f\xdf\xbf"},
            {5, 3, "\x7f\xdf\xbf\xe0\xbf\xbf"},
            {5, 4, "\x7f\xdf\xbf\xe0\xbf\xbf\xf3\xbf\xbf\xbf"},
            {5, 100, s.substr(10)},
            {-12, 2, s.substr(0, 3)},
            {-11, 3, s.substr(1, 9)},
            {-10, 4, s.substr(3, 10)},
            {-9, 5, s.substr(6, 14)},
            {-8, 6, s.substr(10, 13)},
            {-7, 7, s.substr(11, 19)},
            {-6, 8, s.substr(13, 17)},
            {-5, 9, s.substr(16, 14)},
            {-4, 10, s.substr(20, 10)},
            {-3, 11, s.substr(21, 9)},
            {-2, 12, s.substr(23, 7)},
            {-1, 13, s.substr(26, 4)},
            {0, 1, ""},
            {1, -1, ""},
            {-1, -1, ""},
            {1, -100, ""},
            {-1, -100, ""},
            {-1, INT_MIN, ""},
            {1, INT_MAX, s},
            {1, INT_MIN, ""},
    };

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto state = std::make_unique<SubstrState>();
    ctx->set_function_state(FunctionContext::FRAGMENT_LOCAL, state.get());
    starrocks::Columns columns;
    columns.emplace_back(std::move(str));
    for (auto& e : cases) {
        auto [offset, len, expect] = e;
        state->is_const = true;
        state->pos = offset;
        state->len = len;
        ColumnPtr result = StringFunctions::substring(ctx.get(), columns).value();
        auto* binary = down_cast<BinaryColumn*>(result.get());
        ASSERT_EQ(binary->get_slice(0).to_string(), expect);
        ASSERT_EQ(binary->get_slice(1).to_string(), "");
    }
}

TEST_F(StringFunctionSubstrTest, substringOverleftTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto str = BinaryColumn::create();
    auto pos = Int32Column::create();
    auto len = Int32Column::create();
    for (int j = 0; j < 20; ++j) {
        str->append("我是中文字符串" + std::to_string(j));
        pos->append(-100);
        len->append(2);
    }

    columns.emplace_back(std::move(str));
    columns.emplace_back(std::move(pos));
    columns.emplace_back(std::move(len));

    ColumnPtr result = StringFunctions::substring(ctx.get(), columns).value();

    ASSERT_TRUE(result->is_binary());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<BinaryColumn>(result);
    for (int k = 0; k < 20; ++k) {
        ASSERT_EQ("", v->get_data()[k].to_string());
    }
}

TEST_F(StringFunctionSubstrTest, substringConstTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto str = BinaryColumn::create();
    auto pos = Int32Column::create();
    auto len = Int32Column::create();
    pos->append(5);
    len->append(2);

    for (int j = 0; j < 20; ++j) {
        str->append("test" + std::to_string(j));
    }

    columns.emplace_back(std::move(str));
    columns.emplace_back(ConstColumn::create(std::move(pos), 1));
    columns.emplace_back(ConstColumn::create(std::move(len), 1));

    ColumnPtr result = StringFunctions::substring(ctx.get(), columns).value();

    ASSERT_TRUE(result->is_binary());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<BinaryColumn>(result);
    for (int k = 0; k < 20; ++k) {
        ASSERT_EQ(std::to_string(k), v->get_data()[k].to_string());
    }
}

TEST_F(StringFunctionSubstrTest, substringNullTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto pos = Int32Column::create();
    auto len = Int32Column::create();
    pos->append(5);
    len->append(2);

    ColumnBuilder<TYPE_VARCHAR> b(config::vector_chunk_size);

    for (int j = 0; j < 20; ++j) {
        b.append("test" + std::to_string(j), j % 2 == 0);
    }

    columns.emplace_back(b.build(false));
    columns.emplace_back(ConstColumn::create(std::move(pos), 1));
    columns.emplace_back(ConstColumn::create(std::move(len), 1));

    ColumnPtr result = StringFunctions::substring(ctx.get(), columns).value();

    ASSERT_FALSE(result->is_binary());
    ASSERT_TRUE(result->is_nullable());

    auto nv = ColumnHelper::as_column<NullableColumn>(result);
    auto v = ColumnHelper::as_column<BinaryColumn>(nv->data_column());

    for (int k = 0; k < 20; ++k) {
        if (k % 2 == 0) {
            ASSERT_TRUE(nv->is_null(k));
        } else {
            ASSERT_EQ(std::to_string(k), v->get_data()[k].to_string());
        }
    }
}

TEST_F(StringFunctionSubstrTest, leftTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto str = BinaryColumn::create();
    auto inx = Int32Column::create();
    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j) + "TEST");
        inx->append(j);
    }

    columns.emplace_back(std::move(str));
    columns.emplace_back(std::move(inx));

    ColumnPtr result = StringFunctions::left(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::as_column<BinaryColumn>(result);

    for (int k = 0; k < 20; ++k) {
        std::string s = std::to_string(k) + "TEST";
        if (k < s.size()) {
            ASSERT_EQ(0, strncmp(s.c_str(), v->get_data()[k].to_string().c_str(), k));
        } else {
            ASSERT_EQ(s, v->get_data()[k].to_string());
        }
    }
}

TEST_F(StringFunctionSubstrTest, rightTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto str = BinaryColumn::create();
    auto inx = Int32Column::create();
    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j) + "TEST");
        inx->append(j);
    }

    columns.emplace_back(std::move(str));
    columns.emplace_back(std::move(inx));

    ColumnPtr result = StringFunctions::right(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::as_column<BinaryColumn>(result);

    for (int k = 0; k < 20; ++k) {
        std::string s = std::to_string(k) + "TEST";
        if (k < s.size()) {
            ASSERT_EQ(0, strncmp(s.c_str() + s.size() - k, v->get_data()[k].to_string().c_str(), k));
        } else {
            ASSERT_EQ(s, v->get_data()[k].to_string());
        }
    }
}

static void test_left_and_right_not_const(
        std::vector<std::tuple<std::string, int, std::string, std::string>> const& cases) {
    // left_not_const and right_not_const
    std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context());
    Columns columns;
    BinaryColumn::Ptr str_col = BinaryColumn::create();
    Int32Column::Ptr len_col = Int32Column::create();
    for (auto& c : cases) {
        auto s = std::get<0>(c);
        auto len = std::get<1>(c);
        str_col->append(Slice(s));
        len_col->append(len);
    }
    columns.push_back(str_col);
    columns.push_back(len_col);
    ColumnPtr left_result = StringFunctions::left(context.get(), columns).value();
    ColumnPtr right_result = StringFunctions::right(context.get(), columns).value();
    auto* binary_left_result = down_cast<BinaryColumn*>(left_result.get());
    auto* binary_right_result = down_cast<BinaryColumn*>(right_result.get());
    ASSERT_TRUE(binary_left_result != nullptr);
    ASSERT_TRUE(binary_right_result != nullptr);
    const auto size = cases.size();
    ASSERT_TRUE(binary_right_result != nullptr);
    ASSERT_EQ(binary_left_result->size(), size);
    ASSERT_EQ(binary_right_result->size(), size);
    auto state = std::make_unique<SubstrState>();
    for (auto i = 0; i < size; ++i) {
        auto left_expect = std::get<2>(cases[i]);
        auto right_expect = std::get<3>(cases[i]);
        auto left_actual = binary_left_result->get_slice(i).to_string();
        auto right_actual = binary_right_result->get_slice(i).to_string();
        ASSERT_EQ(left_actual, left_expect);
        ASSERT_EQ(right_actual, right_expect);
    }

    // left_const and right_const
    for (auto i = 0; i < size; ++i) {
        auto [s, len, left_expect, right_expect] = cases[i];
        str_col->resize(0);
        len_col->resize(0);
        str_col->append(Slice(s));
        len_col->append(len);
        columns.resize(0);
        columns.push_back(str_col);
        columns.push_back(ConstColumn::create(len_col, 1));

        auto substr_state = std::make_unique<SubstrState>();
        context->set_function_state(FunctionContext::FRAGMENT_LOCAL, substr_state.get());
        substr_state->is_const = true;
        substr_state->pos = 1;
        substr_state->len = len;
        left_result = StringFunctions::left(context.get(), columns).value();
        substr_state->pos = -len;
        right_result = StringFunctions::right(context.get(), columns).value();
        binary_left_result = down_cast<BinaryColumn*>(left_result.get());
        binary_right_result = down_cast<BinaryColumn*>(right_result.get());
        ASSERT_TRUE(binary_left_result != nullptr);
        ASSERT_TRUE(binary_right_result != nullptr);
        ASSERT_EQ(binary_left_result->size(), 1);
        ASSERT_EQ(binary_right_result->size(), 1);
        ASSERT_EQ(binary_left_result->get_slice(0).to_string(), left_expect);
        ASSERT_EQ(binary_right_result->get_slice(0).to_string(), right_expect);
    }
}

TEST_F(StringFunctionSubstrTest, leftAndRightNotConstASCIITest) {
    std::vector<std::tuple<std::string, int, std::string, std::string>> cases{
            {"", 0, "", ""},
            {"", 1, "", ""},
            {"", -1, "", ""},
            {"", INT_MAX, "", ""},
            {"", INT_MIN, "", ""},
            {"a", 0, "", ""},
            {"a", 1, "a", "a"},
            {"a", 10, "a", "a"},
            {"a", INT_MAX, "a", "a"},
            {"a", -1, "", ""},
            {"a", INT_MIN, "", ""},
            {"All fingers are thumbs", 0, "", ""},
            {"All fingers are thumbs", 1, "A", "s"},
            {"All fingers are thumbs", 10, "All finger", "are thumbs"},
            {"All fingers are thumbs", 22, "All fingers are thumbs", "All fingers are thumbs"},
            {"All fingers are thumbs", 23, "All fingers are thumbs", "All fingers are thumbs"},
            {"All fingers are thumbs", 100, "All fingers are thumbs", "All fingers are thumbs"},
            {"All fingers are thumbs", INT_MAX, "All fingers are thumbs", "All fingers are thumbs"},
            {"All fingers are thumbs", -1, "", ""},
            {"All fingers are thumbs", -7, "", ""},
            {"All fingers are thumbs", INT_MIN, "", ""},
    };
    test_left_and_right_not_const(cases);
}
TEST_F(StringFunctionSubstrTest, leftAndRightNotConstUtf8Test) {
    std::vector<std::tuple<std::string, int, std::string, std::string>> cases{
            {"", 0, "", ""},
            {"", 1, "", ""},
            {"", -1, "", ""},
            {"", INT_MAX, "", ""},
            {"", INT_MIN, "", ""},
            {"a", 0, "", ""},
            {"a", 1, "a", "a"},
            {"a", 10, "a", "a"},
            {"a", INT_MAX, "a", "a"},
            {"a", -1, "", ""},
            {"a", INT_MIN, "", ""},
            {"三十年众生牛马，六十年诸佛龙象", 0, "", ""},
            {"三十年众生牛马，六十年诸佛龙象", 1, "三", "象"},
            {"三十年众生牛马，六十年诸佛龙象", 2, "三十", "龙象"},
            {"三十年众生牛马，六十年诸佛龙象", 7, "三十年众生牛马", "六十年诸佛龙象"},
            {"三十年众生牛马，六十年诸佛龙象", 16, "三十年众生牛马，六十年诸佛龙象", "三十年众生牛马，六十年诸佛龙象"},
            {"三十年众生牛马，六十年诸佛龙象", 20, "三十年众生牛马，六十年诸佛龙象", "三十年众生牛马，六十年诸佛龙象"},
            {"三十年众生牛马，六十年诸佛龙象", INT_MAX, "三十年众生牛马，六十年诸佛龙象",
             "三十年众生牛马，六十年诸佛龙象"},
            {"三十年众生牛马，六十年诸佛龙象", -1, "", ""},
            {"三十年众生牛马，六十年诸佛龙象", INT_MIN, "", ""},
            {"a三b十c年d众e生f牛g马", 0, "", ""},
            {"a三b十c年d众e生f牛g马", 1, "a", "马"},
            {"a三b十c年d众e生f牛g马", 7, "a三b十c年d", "众e生f牛g马"},
            {"a三b十c年d众e生f牛g马", 14, "a三b十c年d众e生f牛g马", "a三b十c年d众e生f牛g马"},
            {"a三b十c年d众e生f牛g马", 100, "a三b十c年d众e生f牛g马", "a三b十c年d众e生f牛g马"},
            {"a三b十c年d众e生f牛g马", -1, "", ""},
            {"a三b十c年d众e生f牛g马", -111, "", ""},
            {"a三b十c年d众e生f牛g马", INT_MAX, "a三b十c年d众e生f牛g马", "a三b十c年d众e生f牛g马"},
    };
    test_left_and_right_not_const(cases);
}

static void test_left_and_right_const(
        const ColumnPtr& str_col,
        std::vector<std::tuple<int, std::vector<std::string>, std::vector<std::string>>> const& cases) {
    for (auto& c : cases) {
        auto [len, left_expect, right_expect] = c;
        Columns columns;
        auto len_col = Int32Column::create();
        len_col->append(len);
        columns.emplace_back(std::move(str_col));
        columns.push_back(ConstColumn::create(std::move(len_col), 1));
        auto substr_state = std::make_unique<SubstrState>();
        std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context());
        context->set_function_state(FunctionContext::FRAGMENT_LOCAL, substr_state.get());
        substr_state->is_const = true;
        substr_state->pos = 1;
        substr_state->len = len;
        auto left_result = StringFunctions::left(context.get(), columns).value();
        auto right_result = StringFunctions::right(context.get(), columns).value();
        auto binary_left_result = down_cast<BinaryColumn*>(left_result.get());
        auto binary_right_result = down_cast<BinaryColumn*>(right_result.get());
        ASSERT_TRUE(binary_left_result != nullptr);
        ASSERT_TRUE(binary_right_result != nullptr);
        const auto size = str_col->size();
        ASSERT_EQ(binary_left_result->size(), size);
        ASSERT_EQ(binary_right_result->size(), size);
        for (auto i = 0; i < size; ++i) {
            ASSERT_EQ(binary_left_result->get_slice(i).to_string(), left_expect[i]);
            ASSERT_EQ(binary_right_result->get_slice(i).to_string(), right_expect[i]);
        }
    }
}

TEST_F(StringFunctionSubstrTest, leftAndRightConstASCIITest) {
    auto str_col = BinaryColumn::create();
    str_col->append("");
    str_col->append("a");
    str_col->append("ABCDEFG_HIJKLMN");

    std::vector<std::tuple<int, std::vector<std::string>, std::vector<std::string>>> cases = {
            {0, {"", "", ""}, {"", "", ""}},
            {1, {"", "a", "A"}, {"", "a", "N"}},
            {2, {"", "a", "AB"}, {"", "a", "MN"}},
            {15, {"", "a", "ABCDEFG_HIJKLMN"}, {"", "a", "ABCDEFG_HIJKLMN"}},
            {16, {"", "a", "ABCDEFG_HIJKLMN"}, {"", "a", "ABCDEFG_HIJKLMN"}},
            {INT_MAX, {"", "a", "ABCDEFG_HIJKLMN"}, {"", "a", "ABCDEFG_HIJKLMN"}},
            {-1, {"", "", ""}, {"", "", ""}},
            {-11, {"", "", ""}, {"", "", ""}},
            {-111, {"", "", ""}, {"", "", ""}},
            {INT_MIN, {"", "", ""}, {"", "", ""}},
    };
    test_left_and_right_const(std::move(str_col), cases);
}

TEST_F(StringFunctionSubstrTest, leftAndRightConstUtf8Test) {
    auto str_col = BinaryColumn::create();
    str_col->append("");
    str_col->append("a");
    str_col->append("三十年众生牛马，六十年诸佛龙象");
    str_col->append("a三b十c年d众e生f牛g马");

    std::vector<std::tuple<int, std::vector<std::string>, std::vector<std::string>>> cases = {
            {0, {"", "", "", ""}, {"", "", "", ""}},
            {1, {"", "a", "三", "a"}, {"", "a", "象", "马"}},
            {2, {"", "a", "三十", "a三"}, {"", "a", "龙象", "g马"}},
            {14,
             {"", "a", "三十年众生牛马，六十年诸佛龙", "a三b十c年d众e生f牛g马"},
             {"", "a", "十年众生牛马，六十年诸佛龙象", "a三b十c年d众e生f牛g马"}},
            {15,
             {"", "a", "三十年众生牛马，六十年诸佛龙象", "a三b十c年d众e生f牛g马"},
             {"", "a", "三十年众生牛马，六十年诸佛龙象", "a三b十c年d众e生f牛g马"}},
            {16,
             {"", "a", "三十年众生牛马，六十年诸佛龙象", "a三b十c年d众e生f牛g马"},
             {"", "a", "三十年众生牛马，六十年诸佛龙象", "a三b十c年d众e生f牛g马"}},
            {100,
             {"", "a", "三十年众生牛马，六十年诸佛龙象", "a三b十c年d众e生f牛g马"},
             {"", "a", "三十年众生牛马，六十年诸佛龙象", "a三b十c年d众e生f牛g马"}},
            {INT_MAX,
             {"", "a", "三十年众生牛马，六十年诸佛龙象", "a三b十c年d众e生f牛g马"},
             {"", "a", "三十年众生牛马，六十年诸佛龙象", "a三b十c年d众e生f牛g马"}},
            {-1, {"", "", "", ""}, {"", "", "", ""}},
            {-11, {"", "", "", ""}, {"", "", "", ""}},
            {-111, {"", "", "", ""}, {"", "", "", ""}},
            {INT_MIN, {"", "", "", ""}, {"", "", "", ""}},
    };
    test_left_and_right_const(std::move(str_col), cases);
}

static void test_substr_not_const(std::vector<std::tuple<std::string, int, int, std::string>>& cases) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(cases.begin(), cases.end(), gen);
    std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context());
    auto str_col = BinaryColumn::create();
    auto off_col = Int32Column::create();
    auto len_col = Int32Column::create();
    for (auto& c : cases) {
        str_col->append(Slice(std::get<0>(c)));
        off_col->append(std::get<1>(c));
        len_col->append(std::get<2>(c));
    }
    Columns columns{std::move(str_col), std::move(off_col), std::move(len_col)};
    auto result = StringFunctions::substring(context.get(), columns).value();
    auto* binary_result = down_cast<BinaryColumn*>(result.get());
    const auto size = cases.size();
    ASSERT_TRUE(binary_result != nullptr);
    ASSERT_EQ(binary_result->size(), size);
    for (auto i = 0; i < size; ++i) {
        std::cout << "s=" << std::get<0>(cases[i]) << ", off=" << std::get<1>(cases[i])
                  << ", len=" << std::get<2>(cases[i]) << ", expect=" << std::get<3>(cases[i]) << std::endl;
        ASSERT_EQ(binary_result->get_slice(i).to_string(), std::get<3>(cases[i]));
    }
}

TEST_F(StringFunctionSubstrTest, substrNotConstASCIITest) {
    MutableColumnPtr str_col = BinaryColumn::create();
    std::string ascii_1_9 = "123456789";
    std::vector<std::tuple<std::string, int, int, std::string>> cases = {
            {"", 0, 1, ""},
            {"", 1, 1, ""},
            {"", -1, 1, ""},
            {"", 1, -1, ""},
            {"", INT_MAX, INT_MIN, ""},
            {"", INT_MIN, INT_MAX, ""},
            {"", INT_MAX, INT_MAX, ""},
            {"", INT_MIN, INT_MIN, ""},
            {"a", 0, 1, ""},
            {"a", 1, 1, "a"},
            {"a", 1, 2, "a"},
            {"a", 1, INT_MAX, "a"},
            {"a", 1, INT_MIN, ""},
            {"a", -1, 0, ""},
            {"a", -1, 1, "a"},
            {"a", -1, 2, "a"},
            {"a", -1, -1, ""},
            {"a", -1, INT_MIN, ""},
            {"a", -1, INT_MAX, "a"},
            {ascii_1_9, -1, INT_MIN, ""},
            {ascii_1_9, -1, -1, ""},
            {ascii_1_9, -1, 0, ""},
            {ascii_1_9, -1, 1, "9"},
            {ascii_1_9, -1, INT_MAX, "9"},
            {ascii_1_9, 0, INT_MIN, ""},
            {ascii_1_9, 0, -1, ""},
            {ascii_1_9, 0, 0, ""},
            {ascii_1_9, 0, 1, ""},
            {ascii_1_9, 0, INT_MAX, ""},
            {ascii_1_9, 1, INT_MIN, ""},
            {ascii_1_9, 1, -1, ""},
            {ascii_1_9, 1, 0, ""},
            {ascii_1_9, 1, 1, "1"},
            {ascii_1_9, 1, INT_MAX, ascii_1_9},
            {ascii_1_9, 5, 1, "5"},
            {ascii_1_9, 5, 5, "56789"},
            {ascii_1_9, 5, 6, "56789"},
            {ascii_1_9, 5, INT_MAX, "56789"},
            {ascii_1_9, -4, 1, "6"},
            {ascii_1_9, -4, 3, "678"},
            {ascii_1_9, -4, 4, "6789"},
            {ascii_1_9, -4, 5, "6789"},
            {ascii_1_9, -4, INT_MAX, "6789"},
            {ascii_1_9, -4, INT_MAX, "6789"},
            {ascii_1_9, -9, INT_MIN, ""},
            {ascii_1_9, -9, -1, ""},
            {ascii_1_9, -9, 0, ""},
            {ascii_1_9, -9, 1, "1"},
            {ascii_1_9, -9, 9, ascii_1_9},
            {ascii_1_9, -9, INT_MAX, ascii_1_9},
            {ascii_1_9, -10, INT_MIN, ""},
            {ascii_1_9, -10, -1, ""},
            {ascii_1_9, -10, 0, ""},
            {ascii_1_9, -10, 1, ""},
            {ascii_1_9, -10, 9, ""},
            {ascii_1_9, -10, INT_MAX, ""},
            {ascii_1_9, 9, INT_MIN, ""},
            {ascii_1_9, 9, -1, ""},
            {ascii_1_9, 9, 0, ""},
            {ascii_1_9, 9, 1, "9"},
            {ascii_1_9, 9, 9, "9"},
            {ascii_1_9, 9, INT_MAX, "9"},
            {ascii_1_9, 10, INT_MIN, ""},
            {ascii_1_9, 10, -1, ""},
            {ascii_1_9, 10, 0, ""},
            {ascii_1_9, 10, 1, ""},
            {ascii_1_9, 10, 9, ""},
            {ascii_1_9, 10, INT_MAX, ""},
    };
    test_substr_not_const(cases);
}

TEST_F(StringFunctionSubstrTest, substrNotConstUtf8Test) {
    MutableColumnPtr str_col = BinaryColumn::create();
    std::string zh_1_9 = "壹贰叁肆伍陆柒捌玖";
    std::vector<std::tuple<std::string, int, int, std::string>> cases = {
            {"", 0, 1, ""},
            {"", 1, 1, ""},
            {"", -1, 1, ""},
            {"", 1, -1, ""},
            {"", INT_MAX, INT_MIN, ""},
            {"", INT_MIN, INT_MAX, ""},
            {"", INT_MAX, INT_MAX, ""},
            {"", INT_MIN, INT_MIN, ""},
            {"a", 0, 1, ""},
            {"a", 1, 1, "a"},
            {"a", 1, 2, "a"},
            {"a", 1, INT_MAX, "a"},
            {"a", 1, INT_MIN, ""},
            {"a", -1, 0, ""},
            {"a", -1, 1, "a"},
            {"a", -1, 2, "a"},
            {"a", -1, -1, ""},
            {"a", -1, INT_MIN, ""},
            {"a", -1, INT_MAX, "a"},
            {zh_1_9, -1, INT_MIN, ""},
            {zh_1_9, -1, -1, ""},
            {zh_1_9, -1, 0, ""},
            {zh_1_9, -1, 1, "玖"},
            {zh_1_9, -1, INT_MAX, "玖"},
            {zh_1_9, 0, INT_MIN, ""},
            {zh_1_9, 0, -1, ""},
            {zh_1_9, 0, 0, ""},
            {zh_1_9, 0, 1, ""},
            {zh_1_9, 0, INT_MAX, ""},
            {zh_1_9, 1, INT_MIN, ""},
            {zh_1_9, 1, -1, ""},
            {zh_1_9, 1, 0, ""},
            {zh_1_9, 1, 1, "壹"},
            {zh_1_9, 1, INT_MAX, zh_1_9},
            {zh_1_9, 5, 1, "伍"},
            {zh_1_9, 5, 5, "伍陆柒捌玖"},
            {zh_1_9, 5, 6, "伍陆柒捌玖"},
            {zh_1_9, 5, INT_MAX, "伍陆柒捌玖"},
            {zh_1_9, -4, 1, "陆"},
            {zh_1_9, -4, 3, "陆柒捌"},
            {zh_1_9, -4, 4, "陆柒捌玖"},
            {zh_1_9, -4, 5, "陆柒捌玖"},
            {zh_1_9, -4, INT_MAX, "陆柒捌玖"},
            {zh_1_9, -4, INT_MAX, "陆柒捌玖"},
            {zh_1_9, -9, INT_MIN, ""},
            {zh_1_9, -9, -1, ""},
            {zh_1_9, -9, 0, ""},
            {zh_1_9, -9, 1, "壹"},
            {zh_1_9, -9, 9, zh_1_9},
            {zh_1_9, -9, INT_MAX, zh_1_9},
            {zh_1_9, -10, INT_MIN, ""},
            {zh_1_9, -10, -1, ""},
            {zh_1_9, -10, 0, ""},
            {zh_1_9, -10, 1, ""},
            {zh_1_9, -10, 9, ""},
            {zh_1_9, -10, INT_MAX, ""},
            {zh_1_9, 9, INT_MIN, ""},
            {zh_1_9, 9, -1, ""},
            {zh_1_9, 9, 0, ""},
            {zh_1_9, 9, 1, "玖"},
            {zh_1_9, 9, 9, "玖"},
            {zh_1_9, 9, INT_MAX, "玖"},
            {zh_1_9, 10, INT_MIN, ""},
            {zh_1_9, 10, -1, ""},
            {zh_1_9, 10, 0, ""},
            {zh_1_9, 10, 1, ""},
            {zh_1_9, 10, 9, ""},
            {zh_1_9, 10, INT_MAX, ""},
    };
    test_substr_not_const(cases);
}

} // namespace starrocks
