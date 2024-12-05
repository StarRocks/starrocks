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

#include <memory>
#include <random>

#include "column/array_column.h"
#include "exprs/function_helper.h"
#include "exprs/mock_vectorized_expr.h"
#include "exprs/string_functions.h"
#include "runtime/types.h"
#include "testutil/assert.h"
#include "testutil/parallel_test.h"
#include "types/large_int_value.h"

namespace starrocks {

PARALLEL_TEST(VecStringFunctionsTest, sliceTest) {
    Slice a("abc");
    Slice b("abd");

    ASSERT_FALSE(a == b);
    ASSERT_FALSE(a > b);
    ASSERT_FALSE(a >= b);

    ASSERT_TRUE(a != b);
    ASSERT_TRUE(a <= b);
    ASSERT_TRUE(a < b);
}

PARALLEL_TEST(VecStringFunctionsTest, substringNormalTest) {
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

    columns.emplace_back(str);
    columns.emplace_back(pos);
    columns.emplace_back(len);

    ColumnPtr result = StringFunctions::substring(ctx.get(), columns).value();

    ASSERT_TRUE(result->is_binary());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<BinaryColumn>(result);
    for (int k = 0; k < 20; ++k) {
        ASSERT_EQ(std::to_string(k), v->get_data()[k].to_string());
    }
}

PARALLEL_TEST(VecStringFunctionsTest, substringChineseTest) {
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

    columns.emplace_back(str);
    columns.emplace_back(pos);
    columns.emplace_back(len);

    ColumnPtr result = StringFunctions::substring(ctx.get(), columns).value();

    ASSERT_TRUE(result->is_binary());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<BinaryColumn>(result);
    for (int k = 0; k < 20; ++k) {
        ASSERT_EQ(Slice("中文"), v->get_data()[k]);
    }
}

PARALLEL_TEST(VecStringFunctionsTest, substringleftTest) {
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

    columns.emplace_back(str);
    columns.emplace_back(pos);
    columns.emplace_back(len);

    ColumnPtr result = StringFunctions::substring(ctx.get(), columns).value();

    ASSERT_TRUE(result->is_binary());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<BinaryColumn>(result);
    for (int k = 0; k < 10; ++k) {
        ASSERT_EQ(Slice(std::string("串") + std::to_string(k)), v->get_data()[k]);
    }
}

PARALLEL_TEST(VecStringFunctionsTest, substrConstASCIITest) {
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
    columns.emplace_back(str);
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

PARALLEL_TEST(VecStringFunctionsTest, substrConstZhTest) {
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
    columns.emplace_back(str);
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

PARALLEL_TEST(VecStringFunctionsTest, substrConstUtf8Test) {
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
    columns.emplace_back(str);
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

PARALLEL_TEST(VecStringFunctionsTest, substringOverleftTest) {
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

    columns.emplace_back(str);
    columns.emplace_back(pos);
    columns.emplace_back(len);

    ColumnPtr result = StringFunctions::substring(ctx.get(), columns).value();

    ASSERT_TRUE(result->is_binary());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<BinaryColumn>(result);
    for (int k = 0; k < 20; ++k) {
        ASSERT_EQ("", v->get_data()[k].to_string());
    }
}

PARALLEL_TEST(VecStringFunctionsTest, substringConstTest) {
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

    columns.emplace_back(str);
    columns.emplace_back(ConstColumn::create(pos, 1));
    columns.emplace_back(ConstColumn::create(len, 1));

    ColumnPtr result = StringFunctions::substring(ctx.get(), columns).value();

    ASSERT_TRUE(result->is_binary());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<BinaryColumn>(result);
    for (int k = 0; k < 20; ++k) {
        ASSERT_EQ(std::to_string(k), v->get_data()[k].to_string());
    }
}

PARALLEL_TEST(VecStringFunctionsTest, substringNullTest) {
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
    columns.emplace_back(ConstColumn::create(pos, 1));
    columns.emplace_back(ConstColumn::create(len, 1));

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

PARALLEL_TEST(VecStringFunctionsTest, concatNormalTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str1 = BinaryColumn::create();
    auto str2 = BinaryColumn::create();
    auto str3 = BinaryColumn::create();
    auto str4 = BinaryColumn::create();
    for (int j = 0; j < 20; ++j) {
        str1->append("test");
        str2->append(std::to_string(j));
        str3->append("hello");
        str4->append(std::to_string(j));
    }

    columns.emplace_back(str1);
    columns.emplace_back(str2);
    columns.emplace_back(str3);
    columns.emplace_back(str4);

    ColumnPtr result = StringFunctions::concat(ctx.get(), columns).value();

    ASSERT_TRUE(result->is_binary());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<BinaryColumn>(result);
    for (int k = 0; k < 20; ++k) {
        ASSERT_EQ("test" + std::to_string(k) + "hello" + std::to_string(k), v->get_data()[k].to_string());
    }
}

PARALLEL_TEST(VecStringFunctionsTest, concatConstTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str1 = BinaryColumn::create();
    auto str2 = BinaryColumn::create();
    auto str3 = BinaryColumn::create();
    auto str4 = BinaryColumn::create();
    for (int j = 0; j < 20; ++j) {
        str1->append("test" + std::to_string(j));
    }
    str2->append("_abcd");
    str3->append("_1234");
    str4->append("_道可道,非常道");

    columns.emplace_back(str1);
    columns.emplace_back(ConstColumn::create(str2, 1));
    columns.emplace_back(ConstColumn::create(str3, 1));
    columns.emplace_back(ConstColumn::create(str4, 1));

    ColumnPtr result = StringFunctions::concat(ctx.get(), columns).value();

    ASSERT_TRUE(result->is_binary());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<BinaryColumn>(result);
    for (int k = 0; k < 20; ++k) {
        ASSERT_EQ("test" + std::to_string(k) + "_abcd_1234_道可道,非常道", v->get_data()[k].to_string());
    }
}

PARALLEL_TEST(VecStringFunctionsTest, concatNullTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str1 = BinaryColumn::create();
    auto str2 = BinaryColumn::create();
    auto str3 = BinaryColumn::create();
    auto str4 = BinaryColumn::create();
    auto null = NullColumn::create();
    for (int j = 0; j < 20; ++j) {
        str1->append("test");
        str2->append(std::to_string(j));
        str3->append("hello");
        str4->append(std::to_string(j));
        null->append(j % 2 == 0);
    }

    columns.emplace_back(str1);
    columns.emplace_back(str2);
    columns.emplace_back(str3);
    columns.emplace_back(NullableColumn::create(str4, null));

    ColumnPtr result = StringFunctions::concat(ctx.get(), columns).value();

    ASSERT_FALSE(result->is_binary());
    ASSERT_TRUE(result->is_nullable());

    auto nv = ColumnHelper::as_column<NullableColumn>(result);
    auto v = ColumnHelper::as_column<BinaryColumn>(nv->data_column());

    for (int k = 0; k < 20; ++k) {
        if (k % 2 == 0) {
            ASSERT_TRUE(nv->is_null(k));
        } else {
            ASSERT_EQ("test" + std::to_string(k) + "hello" + std::to_string(k), v->get_data()[k].to_string());
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, lowerNormalTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto str = BinaryColumn::create();
    for (int j = 0; j < 20; ++j) {
        str->append("TEST" + std::to_string(j));
    }

    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::lower(ctx.get(), columns).value();

    ASSERT_TRUE(result->is_binary());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<BinaryColumn>(result);
    for (int k = 0; k < 20; ++k) {
        ASSERT_EQ("test" + std::to_string(k), v->get_data()[k].to_string());
    }
}

PARALLEL_TEST(VecStringFunctionsTest, nullOrEmpty) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto str = BinaryColumn::create();
    str->append("");
    str->append(" ");
    str->append("hello");
    str->append("starrocks");
    str->append("111");
    str->append(".");
    str->append_default();

    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::null_or_empty(ctx.get(), columns).value();

    auto v = ColumnHelper::as_column<BooleanColumn>(result);
    ASSERT_TRUE(v->get_data()[0]);
    ASSERT_FALSE(v->get_data()[1]);
    ASSERT_FALSE(v->get_data()[2]);
    ASSERT_FALSE(v->get_data()[3]);
    ASSERT_FALSE(v->get_data()[4]);
    ASSERT_FALSE(v->get_data()[5]);
    ASSERT_TRUE(v->get_data()[6]);
}

PARALLEL_TEST(VecStringFunctionsTest, split) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto str = BinaryColumn::create();
    auto delim = BinaryColumn::create();
    auto null = NullColumn::create();

    str->append("1,2,3");
    delim->append(",");
    null->append(0);

    str->append("aa.bb.cc");
    delim->append(".");
    null->append(0);

    str->append("a b c");
    delim->append(" ");
    null->append(0);

    str->append("aaa");
    delim->append("aaa");
    null->append(0);

    columns.emplace_back(str);
    columns.emplace_back(delim);
    ColumnPtr result = StringFunctions::split(ctx.get(), columns).value();
    auto* col_array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(result.get()));
    ASSERT_EQ("[['1','2','3'], ['aa','bb','cc'], ['a','b','c'], ['','']]", col_array->debug_string());

    columns.clear();
    str->append("");
    delim->append(",");
    null->append(1);

    auto null_column = NullableColumn::create(str, null);
    columns.emplace_back(null_column);
    columns.emplace_back(delim);
    result = StringFunctions::split(ctx.get(), columns).value();
    ASSERT_EQ("[['1','2','3'], ['aa','bb','cc'], ['a','b','c'], ['',''], NULL]", result->debug_string());

    //two const param
    auto str_const = ConstColumn::create(BinaryColumn::create());
    auto delim_const = ConstColumn::create(BinaryColumn::create());
    str_const->append_datum("a,bc,d,eeee,f");
    delim_const->append_datum(",");
    columns.clear();
    columns.push_back(str_const);
    columns.push_back(delim_const);
    ctx->set_constant_columns(columns);
    ASSERT_TRUE(StringFunctions::split_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    result = StringFunctions::split(ctx.get(), columns).value();
    ASSERT_TRUE(StringFunctions::split_close(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ASSERT_EQ("[['a','bc','d','eeee','f']]", result->debug_string());
}

PARALLEL_TEST(VecStringFunctionsTest, splitConst1) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto str_const = ConstColumn::create(BinaryColumn::create());
    auto delim_const = ConstColumn::create(BinaryColumn::create());
    str_const->append_datum("a,bc,d,eeee,f");
    delim_const->append_datum(",d,");
    columns.clear();
    columns.push_back(str_const);
    columns.push_back(delim_const);
    ctx->set_constant_columns(columns);
    ASSERT_TRUE(StringFunctions::split_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ColumnPtr result = StringFunctions::split(ctx.get(), columns).value();
    ASSERT_TRUE(StringFunctions::split_close(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ASSERT_EQ("[['a,bc','eeee,f']]", result->debug_string());
}

PARALLEL_TEST(VecStringFunctionsTest, splitConst2) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto delim_const = ConstColumn::create(BinaryColumn::create());
    auto str_binary_column = BinaryColumn::create();

    str_binary_column->append("a,b,c");
    str_binary_column->append("aa,bb,cc");
    str_binary_column->append("eeeeeeeeee");
    str_binary_column->append(",");

    delim_const->append_datum(",");
    columns.clear();
    columns.push_back(str_binary_column);
    columns.push_back(delim_const);
    ctx->set_constant_columns(columns);
    ASSERT_TRUE(StringFunctions::split_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
    ColumnPtr result = StringFunctions::split(ctx.get(), columns).value();
    ASSERT_EQ("[['a','b','c'], ['aa','bb','cc'], ['eeeeeeeeee'], ['','']]", result->debug_string());
    ASSERT_TRUE(StringFunctions::split_close(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
}

PARALLEL_TEST(VecStringFunctionsTest, splitChinese) {
    /// non-constant source and delimiter.
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;

        auto str = BinaryColumn::create();
        auto delim = BinaryColumn::create();
        auto null = NullColumn::create();

        str->append("1上海,北,京");
        delim->append(",");
        null->append(0);

        str->append("北京.南京.东京");
        delim->append("");
        null->append(0);

        str->append("北 京南京*东……京");
        delim->append("京");
        null->append(0);

        str->append("北京市北京区北京街道");
        delim->append("北京");
        null->append(0);

        columns.emplace_back(str);
        columns.emplace_back(delim);
        ColumnPtr result = StringFunctions::split(ctx.get(), columns).value();
        auto* col_array = down_cast<ArrayColumn*>(ColumnHelper::get_data_column(result.get()));
        ASSERT_EQ(
                "[['1上海','北','京'], ['北','京','.','南','京','.','东','京'], ['北 ','南','*东……',''], "
                "['','市','区','街道']]",
                col_array->debug_string());
    }
    /// non-constant source and constant delimiter.
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;

        auto delim_const = ConstColumn::create(BinaryColumn::create());
        auto str_binary_column = BinaryColumn::create();

        str_binary_column->append("a地 区b");
        str_binary_column->append("");
        str_binary_column->append("地sh北j 京 g");
        str_binary_column->append(",");

        delim_const->append_datum("");
        columns.clear();
        columns.push_back(str_binary_column);
        columns.push_back(delim_const);
        ctx->set_constant_columns(columns);
        ASSERT_TRUE(
                StringFunctions::split_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
        ColumnPtr result = StringFunctions::split(ctx.get(), columns).value();
        ASSERT_TRUE(StringFunctions::split_close(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
        ASSERT_EQ("[['a','地',' ','区','b'], [], ['地','s','h','北','j',' ','京',' ','g'], [',']]",
                  result->debug_string());
    }

    /// constant source and delimiter
    {
        using CaseType = std::tuple<std::string, std::string, std::string>;
        std::vector<CaseType> cases{
                {"测隔试隔试", "", "[['测','隔','试','隔','试']]"},
                {"测隔试隔试", "隔", "[['测','试','试']]"},
                {"测隔试隔试", "a", "[['测隔试隔试']]"},
                {"测abc隔试隔试", "", "[['测','a','b','c','隔','试','隔','试']]"},
                {"测abc隔试隔试", "隔", "[['测abc','试','试']]"},
                {"测abc隔试abc隔试", "a", "[['测','bc隔试','bc隔试']]"},
                {"a|b|c|d", "", "[['a','|','b','|','c','|','d']]"},
                {"a|b|c|d", "|", "[['a','b','c','d']]"},
                {"a|b|c|d", "隔", "[['a|b|c|d']]"},
        };

        for (const auto& [src, delimiter, expected_out] : cases) {
            std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
            Columns columns;

            auto src_const = ConstColumn::create(BinaryColumn::create());
            auto delim_const = ConstColumn::create(BinaryColumn::create());

            src_const->append_datum(Slice(src));
            delim_const->append_datum(Slice(delimiter));

            columns.clear();
            columns.push_back(src_const);
            columns.push_back(delim_const);

            ctx->set_constant_columns(columns);
            ASSERT_TRUE(StringFunctions::split_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                                .ok());
            ColumnPtr result = StringFunctions::split(ctx.get(), columns).value();
            ASSERT_TRUE(
                    StringFunctions::split_close(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

            ASSERT_EQ(expected_out, result->debug_string());
        }
    }
}

TypeDescriptor array_type(const LogicalType& child_type);

PARALLEL_TEST(VecStringFunctionsTest, str_to_map_v1) {
    // input array<string>
    int chunk_size = 7;
    TypeDescriptor TYPE_ARRAY_VARCHAR = array_type(TYPE_VARCHAR);
    auto array_str_null = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, true);
    // []
    // NULL
    // ['NULL']
    array_str_null->append_datum(Datum(DatumArray{}));
    array_str_null->append_datum(Datum());
    array_str_null->append_datum(Datum(DatumArray{Datum("NULL")}));
    array_str_null->append_datum(Datum(DatumArray{Datum("ab:b"), Datum("ab:b"), Datum("")}));
    array_str_null->append_datum(Datum(DatumArray{Datum("a:b"), Datum("a:b中囸"), Datum("道c:d过’")}));
    array_str_null->append_datum(Datum(DatumArray{Datum("a:c:b:d"), Datum(""), Datum("")}));
    array_str_null->append_datum(Datum(DatumArray{Datum("ab:b"), Datum("ab:b"), Datum("")}));

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    TypeDescriptor string_type_desc = TypeDescriptor::create_varchar_type(10);
    auto string_column = ColumnHelper::create_column(string_type_desc, true);
    string_column->append_datum("a:b,c:d");
    string_column->append_datum("a:1,b:2");

    auto array_str_notnull = ColumnHelper::create_column(TYPE_ARRAY_VARCHAR, false);
    array_str_notnull->append_datum(Datum(DatumArray{}));
    array_str_notnull->append_datum(Datum(DatumArray{Datum("中国:shang海")}));
    array_str_notnull->append_datum(Datum(DatumArray{Datum()}));
    array_str_notnull->append_datum(Datum(DatumArray{Datum("ab:b"), Datum("ab:b"), Datum("")}));
    array_str_notnull->append_datum(
            Datum(DatumArray{Datum("a:b"), Datum("a:b中囸"), Datum("道c:d过’"), Datum("道c:d过")}));
    array_str_notnull->append_datum(Datum(DatumArray{Datum("a:c:b:d"), Datum(""), Datum("")}));
    array_str_notnull->append_datum(Datum(DatumArray{Datum("ab:b"), Datum("ab:b"), Datum("")}));

    auto only_null = ColumnHelper::create_const_null_column(chunk_size);

    // delimiters

    auto map_delimiter_builder_nullable = ColumnBuilder<TYPE_VARCHAR>(chunk_size);
    map_delimiter_builder_nullable.append(":");
    map_delimiter_builder_nullable.append(":");
    map_delimiter_builder_nullable.append("ab");
    map_delimiter_builder_nullable.append(":");
    map_delimiter_builder_nullable.append(":b");
    map_delimiter_builder_nullable.append("");
    map_delimiter_builder_nullable.append_null();
    auto map_delimiter_nullable = map_delimiter_builder_nullable.build_nullable_column();
    auto delimiter_column = ColumnHelper::create_column(string_type_desc, true);
    delimiter_column->append_datum(",");
    delimiter_column->append_datum(",");

    auto map_delimiter_builder_notnull = ColumnBuilder<TYPE_VARCHAR>(chunk_size);
    map_delimiter_builder_notnull.append(":");
    map_delimiter_builder_notnull.append("国");
    map_delimiter_builder_notnull.append("ab");
    map_delimiter_builder_notnull.append(":");
    map_delimiter_builder_notnull.append(":b");
    map_delimiter_builder_notnull.append("");
    map_delimiter_builder_notnull.append("");
    auto map_delimiter_notnull = map_delimiter_builder_notnull.build(false);

    auto empty_col = BinaryColumn::create();
    empty_col->append_datum("");
    auto delim_const_empty = ConstColumn::create(empty_col, chunk_size);

    auto ch_col = BinaryColumn::create();
    ch_col->append_datum("中");
    auto delim_const_ch = ConstColumn::create(ch_col, chunk_size);

    auto const_col = BinaryColumn::create();
    const_col->append_datum(":");
    auto delim_const = ConstColumn::create(const_col, chunk_size);

    {
        Columns columns{string_column, delimiter_column, map_delimiter_nullable};
        ctx->set_constant_columns(columns);
        auto res = StringFunctions::str_to_map(ctx.get(), columns).value();
        ASSERT_EQ(res->debug_string(), "[{'a':'b','c':'d'}, {'a':'1','b':'2'}]");
    }
    {
        auto res = StringFunctions::str_to_map_v1(nullptr, {array_str_null, only_null}).value();
        ASSERT_EQ(res->debug_string(), "CONST: NULL Size : 7");
    }
    {
        auto res = StringFunctions::str_to_map_v1(nullptr, {array_str_null, map_delimiter_nullable}).value();
        ASSERT_EQ(res->debug_string(),
                  "[{'':NULL}, NULL, {'NULL':NULL}, {'ab':'b','':NULL}, {'a':'中囸','道c:d过’':NULL}, "
                  "{'a':':c:b:d','':NULL}, NULL]");
    }
    {
        auto res = StringFunctions::str_to_map_v1(nullptr, {array_str_null, map_delimiter_notnull}).value();
        ASSERT_EQ(res->debug_string(),
                  "[{'':NULL}, NULL, {'NULL':NULL}, {'ab':'b','':NULL}, {'a':'中囸','道c:d过’':NULL}, "
                  "{'a':':c:b:d','':NULL}, {'a':'b:b','':NULL}]");
    }
    {
        auto res = StringFunctions::str_to_map_v1(nullptr, {array_str_null, delim_const_empty}).value();
        ASSERT_EQ(res->debug_string(),
                  "[{'':NULL}, NULL, {'N':'ULL'}, {'a':'b:b','':NULL}, {'a':':b中囸','道':'c:d过’'}, "
                  "{'a':':c:b:d','':NULL}, {'a':'b:b','':NULL}]");
    }
    {
        auto res = StringFunctions::str_to_map_v1(nullptr, {array_str_null, delim_const_ch}).value();
        ASSERT_EQ(res->debug_string(),
                  "[{'':NULL}, NULL, {'NULL':NULL}, {'ab:b':NULL,'':NULL}, {'a:b':'囸','道c:d过’':NULL}, "
                  "{'a:c:b:d':NULL,'':NULL}, {'ab:b':NULL,'':NULL}]");
    }
    {
        auto res = StringFunctions::str_to_map_v1(nullptr, {array_str_null, delim_const}).value();
        ASSERT_EQ(res->debug_string(),
                  "[{'':NULL}, NULL, {'NULL':NULL}, {'ab':'b','':NULL}, {'a':'b中囸','道c':'d过’'}, "
                  "{'a':'c:b:d','':NULL}, {'ab':'b','':NULL}]");
    }
    ///
    {
        auto res = StringFunctions::str_to_map_v1(nullptr, {array_str_notnull, only_null}).value();
        ASSERT_EQ(res->debug_string(), "CONST: NULL Size : 7");
    }
    {
        auto res = StringFunctions::str_to_map_v1(nullptr, {array_str_notnull, map_delimiter_nullable}).value();
        ASSERT_EQ(res->debug_string(),
                  "[{'':NULL}, {'中国':'shang海'}, {'':NULL}, {'ab':'b','':NULL}, "
                  "{'a':'中囸','道c:d过’':NULL,'道c:d过':NULL}, {'a':':c:b:d','':NULL}, NULL]");
    }
    {
        auto res = StringFunctions::str_to_map_v1(nullptr, {array_str_notnull, map_delimiter_notnull}).value();
        ASSERT_EQ(res->debug_string(),
                  "[{'':NULL}, {'中':':shang海'}, {'':NULL}, {'ab':'b','':NULL}, "
                  "{'a':'中囸','道c:d过’':NULL,'道c:d过':NULL}, {'a':':c:b:d','':NULL}, {'a':'b:b','':NULL}]");
    }
    {
        auto res = StringFunctions::str_to_map_v1(nullptr, {array_str_notnull, delim_const_empty}).value();
        ASSERT_EQ(res->debug_string(),
                  "[{'':NULL}, {'中':'国:shang海'}, {'':NULL}, {'a':'b:b','':NULL}, {'a':':b中囸','道':'c:d过'}, "
                  "{'a':':c:b:d','':NULL}, {'a':'b:b','':NULL}]");
    }
    {
        auto res = StringFunctions::str_to_map_v1(nullptr, {array_str_notnull, delim_const_ch}).value();
        ASSERT_EQ(res->debug_string(),
                  "[{'':NULL}, {'':'国:shang海'}, {'':NULL}, {'ab:b':NULL,'':NULL}, "
                  "{'a:b':'囸','道c:d过’':NULL,'道c:d过':NULL}, {'a:c:b:d':NULL,'':NULL}, {'ab:b':NULL,'':NULL}]");
    }
    {
        auto res = StringFunctions::str_to_map_v1(nullptr, {array_str_notnull, delim_const}).value();
        ASSERT_EQ(res->debug_string(),
                  "[{'':NULL}, {'中国':'shang海'}, {'':NULL}, {'ab':'b','':NULL}, {'a':'b中囸','道c':'d过'}, "
                  "{'a':'c:b:d','':NULL}, {'ab':'b','':NULL}]");
    }
    ///
    {
        auto res = StringFunctions::str_to_map_v1(nullptr, {only_null, only_null}).value();
        ASSERT_EQ(res->debug_string(), "CONST: NULL Size : 7");
    }
}

PARALLEL_TEST(VecStringFunctionsTest, splitPart) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto str = BinaryColumn::create();
    auto delim = BinaryColumn::create();
    auto field = Int32Column::create();

    // 0
    str->append("hello word");
    delim->append(" ");
    field->append(1);

    // 1
    str->append("hello word");
    delim->append(" ");
    field->append(2);

    // 2
    str->append("hello word");
    delim->append(" ");
    field->append(3);

    // 3
    str->append("hello word");
    delim->append("hello");
    field->append(1);

    // 4
    str->append("hello word");
    delim->append("hello");
    field->append(2);

    // 5
    str->append("abcdabda");
    delim->append("a");
    field->append(1);

    // 6
    str->append("abcdabda");
    delim->append("a");
    field->append(2);

    // 7
    str->append("abcdabda");
    delim->append("a");
    field->append(3);

    // 8
    str->append("abcdabda");
    delim->append("a");
    field->append(4);

    // 9
    str->append("2019年9月8日");
    delim->append("月");
    field->append(1);

    // 10
    str->append("abcdabda");
    delim->append("");
    field->append(1);

    // 11
    str->append("abc###123###234");
    delim->append("##");
    field->append(2);

    // 12
    str->append("abc###123###234");
    delim->append("##");
    field->append(3);

    // 13
    str->append("abcde");
    delim->append("");
    field->append(2);

    // 14
    str->append("abcde");
    delim->append("");
    field->append(10);

    // 15
    str->append("abcde");
    delim->append("abcdef");
    field->append(10);

    // 16
    str->append("abcde");
    delim->append("abcdef");
    field->append(2);

    // 17
    str->append("");
    delim->append("abcdef");
    field->append(2);

    // 18
    str->append("");
    delim->append("");
    field->append(2);

    // 19
    str->append("abcd");
    delim->append("");
    field->append(5);

    // 20
    str->append("2019年9月8日");
    delim->append("");
    field->append(4);

    // 21
    str->append("2019年9月8日");
    delim->append("");
    field->append(5);

    // 22
    str->append("2019年9月8日");
    delim->append("");
    field->append(6);

    // 23
    str->append("2019年9月8日");
    delim->append("");
    field->append(9);

    // 24
    str->append("2019年9月8日");
    delim->append("");
    field->append(10);

    // 25
    str->append("hello word");
    delim->append(" ");
    field->append(-1);

    // 26
    str->append("hello word");
    delim->append(" ");
    field->append(-2);

    // 27
    str->append("hello word");
    delim->append(" ");
    field->append(-3);

    // 28
    str->append("2019年9月8日");
    delim->append("月");
    field->append(-1);

    columns.emplace_back(str);
    columns.emplace_back(delim);
    columns.emplace_back(field);

    ColumnPtr result = StringFunctions::split_part(ctx.get(), columns).value();
    auto v = ColumnHelper::as_column<NullableColumn>(result);

    ASSERT_EQ("hello", v->get(0).get<Slice>().to_string());
    ASSERT_EQ("word", v->get(1).get<Slice>().to_string());
    ASSERT_TRUE(v->get(2).is_null());
    ASSERT_EQ("", v->get(3).get<Slice>().to_string());
    ASSERT_EQ(" word", v->get(4).get<Slice>().to_string());
    ASSERT_EQ("", v->get(5).get<Slice>().to_string());
    ASSERT_EQ("bcd", v->get(6).get<Slice>().to_string());
    ASSERT_EQ("bd", v->get(7).get<Slice>().to_string());
    ASSERT_EQ("", v->get(8).get<Slice>().to_string());
    ASSERT_EQ("2019年9", v->get(9).get<Slice>().to_string());
    ASSERT_EQ("a", v->get(10).get<Slice>().to_string());
    ASSERT_EQ("#123", v->get(11).get<Slice>().to_string());
    ASSERT_EQ("#234", v->get(12).get<Slice>().to_string());
    ASSERT_EQ("b", v->get(13).get<Slice>().to_string());
    ASSERT_TRUE(v->get(14).is_null());
    ASSERT_TRUE(v->get(15).is_null());
    ASSERT_TRUE(v->get(16).is_null());
    ASSERT_TRUE(v->get(17).is_null());
    ASSERT_TRUE(v->get(18).is_null());
    ASSERT_TRUE(v->get(19).is_null());
    ASSERT_EQ("9", v->get(20).get<Slice>().to_string());
    ASSERT_EQ("年", v->get(21).get<Slice>().to_string());
    ASSERT_EQ("9", v->get(22).get<Slice>().to_string());
    ASSERT_EQ("日", v->get(23).get<Slice>().to_string());
    ASSERT_TRUE(v->get(24).is_null());
    ASSERT_EQ("word", v->get(25).get<Slice>().to_string());
    ASSERT_EQ("hello", v->get(26).get<Slice>().to_string());
    ASSERT_TRUE(v->get(27).is_null());
    ASSERT_EQ("8日", v->get(28).get<Slice>().to_string());
}

PARALLEL_TEST(VecStringFunctionsTest, leftTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto str = BinaryColumn::create();
    auto inx = Int32Column::create();
    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j) + "TEST");
        inx->append(j);
    }

    columns.emplace_back(str);
    columns.emplace_back(inx);

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

PARALLEL_TEST(VecStringFunctionsTest, rightTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto str = BinaryColumn::create();
    auto inx = Int32Column::create();
    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j) + "TEST");
        inx->append(j);
    }

    columns.emplace_back(str);
    columns.emplace_back(inx);

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

PARALLEL_TEST(VecStringFunctionsTest, startsWithTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto prefix = BinaryColumn::create();
    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j) + "TEST");
        prefix->append(std::to_string(j % 10) + "T");
    }

    columns.emplace_back(str);
    columns.emplace_back(prefix);

    ColumnPtr result = StringFunctions::starts_with(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(result);

    for (int k = 0; k < 20; ++k) {
        if (k < 10) {
            ASSERT_TRUE(v->get_data()[k]);
        } else {
            ASSERT_FALSE(v->get_data()[k]);
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, startsWithNullTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto prefix = BinaryColumn::create();
    auto null = NullColumn::create();

    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j) + "TEST");
        if (j > 10) {
            prefix->append(std::to_string(j));
            null->append(false);
        } else {
            prefix->append(std::to_string(j));
            null->append(true);
        }
    }

    columns.emplace_back(str);
    columns.emplace_back(NullableColumn::create(prefix, null));

    ColumnPtr result = StringFunctions::starts_with(ctx.get(), columns).value();

    ASSERT_EQ(20, result->size());
    ASSERT_TRUE(result->is_nullable());
    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    for (int k = 0; k < 20; ++k) {
        if (k > 10) {
            ASSERT_TRUE(v->get_data()[k]);
        } else {
            ASSERT_TRUE(result->is_null(k));
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, endsWithNullTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto suffix = BinaryColumn::create();
    auto null = NullColumn::create();

    for (int j = 0; j < 20; ++j) {
        str->append("TEST" + std::to_string(j));
        if (j > 10) {
            suffix->append(std::to_string(j));
            null->append(false);
        } else {
            suffix->append(std::to_string(j));
            null->append(true);
        }
    }

    columns.emplace_back(str);
    columns.emplace_back(NullableColumn::create(suffix, null));

    ColumnPtr result = StringFunctions::ends_with(ctx.get(), columns).value();

    ASSERT_EQ(20, result->size());
    ASSERT_TRUE(result->is_nullable());
    auto v = ColumnHelper::cast_to<TYPE_BOOLEAN>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    for (int k = 0; k < 20; ++k) {
        if (k > 10) {
            ASSERT_TRUE(v->get_data()[k]);
        } else {
            ASSERT_TRUE(result->is_null(k));
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, appendTrailingCharIfAbsentTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto pad = BinaryColumn::create();

    str->append("qwer");
    pad->append("r");

    str->append("qwe");
    pad->append("r");

    str->append("");
    pad->append("r");

    columns.emplace_back(str);
    columns.emplace_back(pad);

    ColumnPtr result = StringFunctions::append_trailing_char_if_absent(ctx.get(), columns).value();
    ASSERT_EQ(3, result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    ASSERT_EQ("qwer", v->get_data()[0].to_string());
    ASSERT_EQ("qwer", v->get_data()[1].to_string());
    ASSERT_EQ("", v->get_data()[2].to_string());
}

PARALLEL_TEST(VecStringFunctionsTest, appendTrailingCharIfAbsentNullTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto pad = BinaryColumn::create();

    str->append("qwer");
    pad->append("rw");

    str->append("qwe");
    pad->append("er");

    columns.emplace_back(str);
    columns.emplace_back(pad);

    ColumnPtr result = StringFunctions::append_trailing_char_if_absent(ctx.get(), columns).value();
    ASSERT_EQ(2, result->size());

    ASSERT_TRUE(result->is_nullable());
    ASSERT_TRUE(result->is_null(0));
    ASSERT_TRUE(result->is_null(1));
}

PARALLEL_TEST(VecStringFunctionsTest, appendTrailingCharIfAbsentUTF8Test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto pad = BinaryColumn::create();

    str->append("中国");
    pad->append("a");

    str->append("北京");
    pad->append("b");

    columns.emplace_back(str);
    columns.emplace_back(pad);

    ColumnPtr result = StringFunctions::append_trailing_char_if_absent(ctx.get(), columns).value();
    ASSERT_EQ(2, result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    ASSERT_EQ("中国a", v->get_data()[0].to_string());
    ASSERT_EQ("北京b", v->get_data()[1].to_string());
}

PARALLEL_TEST(VecStringFunctionsTest, appendTrailingCharIfAbsentUTF8NullTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto pad = BinaryColumn::create();

    str->append("中国");
    pad->append("国");

    str->append("北京");
    pad->append("京");

    columns.emplace_back(str);
    columns.emplace_back(pad);

    ColumnPtr result = StringFunctions::append_trailing_char_if_absent(ctx.get(), columns).value();
    ASSERT_EQ(2, result->size());

    ASSERT_TRUE(result->is_nullable());
    ASSERT_TRUE(result->is_null(0));
    ASSERT_TRUE(result->is_null(1));
}

PARALLEL_TEST(VecStringFunctionsTest, lengthTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j));
    }

    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::length(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int k = 0; k < 20; ++k) {
        if (k < 10) {
            ASSERT_EQ(1, v->get_data()[k]);
        } else {
            ASSERT_EQ(2, v->get_data()[k]);
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, lengthChineseTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    for (int j = 0; j < 20; ++j) {
        str->append("中文" + std::to_string(j));
    }

    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::length(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int k = 0; k < 20; ++k) {
        if (k < 10) {
            ASSERT_EQ(7, v->get_data()[k]);
        } else {
            ASSERT_EQ(8, v->get_data()[k]);
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, utf8LengthTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j));
    }

    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::utf8_length(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int k = 0; k < 20; ++k) {
        if (k < 10) {
            ASSERT_EQ(1, v->get_data()[k]);
        } else {
            ASSERT_EQ(2, v->get_data()[k]);
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, utf8LengthChineseTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    for (int j = 0; j < 20; ++j) {
        str->append("中文" + std::to_string(j));
    }

    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::utf8_length(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int k = 0; k < 20; ++k) {
        if (k < 10) {
            ASSERT_EQ(3, v->get_data()[k]);
        } else {
            ASSERT_EQ(4, v->get_data()[k]);
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, upperTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    for (int j = 0; j < 20; ++j) {
        str->append("abcd" + std::to_string(j));
    }

    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::upper(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int k = 0; k < 20; ++k) {
        ASSERT_EQ("ABCD" + std::to_string(k), v->get_data()[k].to_string());
    }
}

PARALLEL_TEST(VecStringFunctionsTest, caseToggleTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto src = BinaryColumn::create();
    src->append("");
    src->append("a");
    src->append("1");
    src->append("abcd_efg_higk_lmn_opq_rst_uvw_xyz");
    src->append("ABCD_EFG_HIGK_LMN_OPQ_RST_UVW_XYZ");
    src->append("AbCd_EfG_HiGk_LmN_oPq_RsT_UvW_xYz");
    std::string s;
    s.resize(255);
    for (int i = 0; i < 255; ++i) {
        s[i] = (char)i;
    }
    src->append(s);
    src->append("三aBcD十eFg年HiGk众生LmN牛马oPq六十年RsT诸uVw佛XyZ龙象");
    src->append(
            "φημὶγὰρἐγὼεἶναιτὸABCD_EFG_HIGK_LMNδίκαιονοὐκἄλλοτιOPQRST_"
            "UVWἢτὸτοῦκρείττονοςσυμφέρονXYZ");
    columns.push_back(src);
    auto upper_dst = StringFunctions::upper(ctx.get(), columns).value();
    auto lower_dst = StringFunctions::lower(ctx.get(), columns).value();
    auto binary_upper_dst = down_cast<BinaryColumn*>(upper_dst.get());
    auto binary_lower_dst = down_cast<BinaryColumn*>(lower_dst.get());
    ASSERT_TRUE(binary_upper_dst != nullptr);
    ASSERT_TRUE(binary_lower_dst != nullptr);
    auto size = src->size();
    ASSERT_EQ(binary_upper_dst->size(), size);
    ASSERT_EQ(binary_lower_dst->size(), size);
    for (auto i = 0; i < size; ++i) {
        Slice origin = src->get_slice(i);
        Slice uc = binary_upper_dst->get_slice(i);
        Slice lc = binary_lower_dst->get_slice(i);
        std::string uc1 = origin.to_string();
        std::string lc1 = origin.to_string();
        std::transform(uc1.begin(), uc1.end(), uc1.begin(), [](char c) -> char { return std::toupper(c); });
        std::transform(lc1.begin(), lc1.end(), lc1.begin(), [](char c) -> char { return std::tolower(c); });
        ASSERT_EQ(uc.to_string(), uc1);
        ASSERT_EQ(lc.to_string(), lc1);
    }
}

PARALLEL_TEST(VecStringFunctionsTest, asciiTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();

    str->append("qwer");
    str->append("qwe");
    str->append("");

    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::ascii(ctx.get(), columns).value();
    ASSERT_EQ(3, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    ASSERT_EQ(static_cast<int32_t>('q'), v->get_data()[0]);
    ASSERT_EQ(static_cast<int32_t>('q'), v->get_data()[1]);
    ASSERT_EQ(0, v->get_data()[2]);
}

PARALLEL_TEST(VecStringFunctionsTest, charTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = Int32Column::create();

    str->append(65);
    str->append(66);
    str->append(97);
    str->append(98);
    str->append(33);
    str->append(126);

    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::get_char(ctx.get(), columns).value();
    ASSERT_EQ(6, result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    ASSERT_EQ("A", v->get_data()[0].to_string());
    ASSERT_EQ("B", v->get_data()[1].to_string());
    ASSERT_EQ("a", v->get_data()[2].to_string());
    ASSERT_EQ("b", v->get_data()[3].to_string());
    ASSERT_EQ("!", v->get_data()[4].to_string());
    ASSERT_EQ("~", v->get_data()[5].to_string());
}

PARALLEL_TEST(VecStringFunctionsTest, instrTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto sub = BinaryColumn::create();

    for (int j = 0; j < 20; ++j) {
        str->append("abcd" + std::to_string(j));
        sub->append(std::to_string(j));
    }

    columns.emplace_back(str);
    columns.emplace_back(sub);

    ColumnPtr result = StringFunctions::instr(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < 20; ++j) {
        ASSERT_EQ(5, v->get_data()[j]);
    }
}

PARALLEL_TEST(VecStringFunctionsTest, instrChineseTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto sub = BinaryColumn::create();

    for (int j = 0; j < 20; ++j) {
        str->append("中文字符" + std::to_string(j));
        sub->append(std::to_string(j));
    }

    columns.emplace_back(str);
    columns.emplace_back(sub);

    ColumnPtr result = StringFunctions::instr(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < 20; ++j) {
        ASSERT_EQ(5, v->get_data()[j]);
    }
}

PARALLEL_TEST(VecStringFunctionsTest, locateNullTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto sub = BinaryColumn::create();
    auto null = NullColumn::create();

    for (int j = 0; j < 20; ++j) {
        str->append("abcd" + std::to_string(j));
        sub->append(std::to_string(j));
        null->append(j % 2);
    }

    columns.emplace_back(NullableColumn::create(sub, null));
    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::locate(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());
    ASSERT_TRUE(result->is_nullable());

    auto v = ColumnHelper::cast_to<TYPE_INT>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    for (int j = 0; j < 20; ++j) {
        if (j % 2) {
            ASSERT_TRUE(result->is_null(j));
        } else {
            ASSERT_EQ(5, v->get_data()[j]);
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, locatePosTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto sub = BinaryColumn::create();
    auto pos = Int32Column::create();

    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j) + "abcd" + std::to_string(j));
        sub->append(std::to_string(j));
        pos->append(4);
    }

    columns.emplace_back(sub);
    columns.emplace_back(str);
    columns.emplace_back(pos);

    ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < 20; ++j) {
        if (j < 10) {
            ASSERT_EQ(6, v->get_data()[j]);
        } else {
            ASSERT_EQ(7, v->get_data()[j]);
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, locatePosChineseTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto sub = BinaryColumn::create();
    auto pos = Int32Column::create();

    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j) + "中文字符" + std::to_string(j));
        sub->append(std::to_string(j));
        pos->append(4);
    }

    columns.emplace_back(sub);
    columns.emplace_back(str);
    columns.emplace_back(pos);

    ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < 20; ++j) {
        if (j < 10) {
            ASSERT_EQ(6, v->get_data()[j]);
        } else {
            ASSERT_EQ(7, v->get_data()[j]);
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, concatWsTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto step = BinaryColumn::create();
    auto str1 = BinaryColumn::create();
    auto str2 = BinaryColumn::create();
    auto str3 = BinaryColumn::create();

    auto null = NullColumn::create();

    for (int j = 0; j < 20; ++j) {
        step->append("|");
        str1->append("a");
        str2->append(std::to_string(j));
        str3->append("b");
        null->append(j % 2);
    }

    columns.emplace_back(step);
    columns.emplace_back(str1);
    columns.emplace_back(str2);
    columns.emplace_back(NullableColumn::create(str3, null));

    ColumnPtr result = StringFunctions::concat_ws(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int j = 0; j < 20; ++j) {
        if (j % 2) {
            ASSERT_EQ("a|" + std::to_string(j), v->get_data()[j].to_string());
        } else {
            ASSERT_EQ("a|" + std::to_string(j) + "|b", v->get_data()[j].to_string());
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, concatWs1Test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;

    auto step = BinaryColumn::create();
    auto str1 = BinaryColumn::create();
    auto str2 = BinaryColumn::create();
    auto str3 = BinaryColumn::create();

    auto null = NullColumn::create();

    for (int j = 0; j < 20; ++j) {
        step->append("-----");
        str1->append("a");
        str2->append(std::to_string(j));
        str3->append("b");
        null->append(j % 2);
    }

    columns.emplace_back(step);
    columns.emplace_back(str1);
    columns.emplace_back(str2);
    columns.emplace_back(NullableColumn::create(str3, null));

    ColumnPtr result = StringFunctions::concat_ws(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int j = 0; j < 20; ++j) {
        if (j % 2) {
            ASSERT_EQ("a-----" + std::to_string(j), v->get_data()[j].to_string());
        } else {
            ASSERT_EQ("a-----" + std::to_string(j) + "-----b", v->get_data()[j].to_string());
        }
    }
}

PARALLEL_TEST(VecStringFunctionsTest, findInSetTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto strlist = BinaryColumn::create();

    str->append("b");
    strlist->append("a,b,c");

    str->append("bc");
    strlist->append("ab,cd,bc");

    str->append("bc");
    strlist->append("abc,bcd,efg");

    str->append("abc,");
    strlist->append("abc,bcd,efg");

    str->append("");
    strlist->append("abc");

    str->append("");
    strlist->append(",abc");

    str->append("abc");
    strlist->append("abc");

    str->append("bc");
    strlist->append("abc");

    str->append("bc");
    strlist->append("abc");

    columns.emplace_back(str);
    columns.emplace_back(strlist);

    ColumnPtr result = StringFunctions::find_in_set(ctx.get(), columns).value();
    ASSERT_EQ(9, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    ASSERT_EQ(2, v->get_data()[0]);
    ASSERT_EQ(3, v->get_data()[1]);
    ASSERT_EQ(0, v->get_data()[2]);
    ASSERT_EQ(0, v->get_data()[3]);
    ASSERT_EQ(0, v->get_data()[4]);
    ASSERT_EQ(1, v->get_data()[5]);
    ASSERT_EQ(1, v->get_data()[6]);
    ASSERT_EQ(0, v->get_data()[7]);
    ASSERT_EQ(0, v->get_data()[8]);
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractNullablePattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();
    auto null = NullColumn::create();
    auto index = Int64Column::create();

    std::string strs[] = {"AbCdE", "AbCdrrryE", "hitdeciCsiondlist", "hitdecCisiondlist"};
    int indexs[] = {1, 2, 1, 2};

    std::string res[] = {"", "drrry", "", "td"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        pattern->append(i < 2 ? "([[:lower:]]+)C([[:lower:]]+)" : "(i)(.*?)(e)");
        null->append(i % 2 == 0);
        index->append(indexs[i]);
    }

    columns.push_back(str);
    columns.push_back(NullableColumn::create(pattern, null));
    columns.push_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract(context, columns).value();

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        if (i % 2 == 0) {
            ASSERT_TRUE(result->is_null(i));
        } else {
            ASSERT_FALSE(result->is_null(i));
        }

        ASSERT_EQ(res[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractOnlyNullPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = ColumnHelper::create_const_null_column(1);
    auto index = Int64Column::create();

    int length = 4;

    for (int i = 0; i < length; ++i) {
        str->append("test" + std::to_string(i));
        index->append(1);
    }

    columns.push_back(str);
    columns.push_back(pattern);
    columns.push_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract(context, columns).value();
    for (int i = 0; i < length; ++i) {
        ASSERT_TRUE(result->is_null(i));
    }

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractConstPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("([[:lower:]]+)C([[:lower:]]+)", 1);
    auto index = Int64Column::create();

    std::string strs[] = {"AbCdE", "AbCdrrryE", "hitdeciCsiondlist", "hitdecCisiondlist"};
    int indexs[] = {1, 2, 1, 2};

    std::string res[] = {"b", "drrry", "hitdeci", "isiondlist"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        index->append(indexs[i]);
    }

    columns.push_back(str);
    columns.push_back(pattern);
    columns.push_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract(context, columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtract) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();
    auto index = Int64Column::create();

    std::string strs[] = {"AbCdE", "AbCDdrrryE", "hitdecisiondlist", "hitdecisiondlist"};
    std::string ptns[] = {"([[:lower:]]+)C([[:lower:]]+)", "([[:lower:]]+)CD([[:lower:]]+)", "(i)(.*?)(e)",
                          "(i)(.*?)(s)"};
    int indexs[] = {1, 2, 1, 2};

    std::string res[] = {"b", "drrry", "i", "tdeci"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        pattern->append(ptns[i]);
        index->append(indexs[i]);
    }

    columns.push_back(str);
    columns.push_back(pattern);
    columns.push_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract(context, columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, regexpReplaceNullablePattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();
    auto null = NullColumn::create();
    auto replace = BinaryColumn::create();

    std::string strs[] = {"a b c", "a sdfwe b c"};
    std::string replaces[] = {"-", "<\\1>"};

    std::string res[] = {"a-b-c", "a< >sdfwe< >b< >c"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        replace->append(replaces[i]);
    }

    pattern->append("( )");
    pattern->append("dsdfsf");
    null->append(0);
    null->append(1);

    columns.emplace_back(str);
    columns.emplace_back(NullableColumn::create(pattern, null));
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_replace_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_replace(context, columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    ASSERT_EQ(res[0], v->get_data()[0].to_string());
    ASSERT_TRUE(result->is_null(1));

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, regexpReplaceOnlyNullPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = ColumnHelper::create_const_null_column(1);
    auto replace = BinaryColumn::create();

    std::string strs[] = {"a b c", "a sdfwe b c"};
    std::string replaces[] = {"-", "<\\1>"};

    std::string res[] = {"a-b-c", "a< >sdfwe< >b< >c"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        replace->append(replaces[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_replace_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_replace(context, columns).value();

    ASSERT_TRUE(result->is_null(0));
    ASSERT_TRUE(result->is_null(1));

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, regexpReplaceConstPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto ptn = ColumnHelper::create_const_column<TYPE_VARCHAR>("( )", 1);
    auto replace = BinaryColumn::create();

    std::string strs[] = {"a b c", "a sdfwe b c"};
    std::string replaces[] = {"-", "<\\1>"};

    std::string res[] = {"a-b-c", "a< >sdfwe< >b< >c"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        replace->append(replaces[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(ptn);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_replace_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_replace(context, columns).value();
    auto v = ColumnHelper::as_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());

    // Test Binary input data
    {
        FunctionContext::FunctionStateScope scope = FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL;
        std::unique_ptr<FunctionContext> ctx0(FunctionContext::create_test_context());
        int binary_size = 10;
        std::unique_ptr<char[]> binary_datas = std::make_unique<char[]>(binary_size);
        memset(binary_datas.get(), 0xff, binary_size);

        auto par0 = BinaryColumn::create();
        auto par1 = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(binary_datas.get(), binary_size), 1);

        ctx0->set_constant_columns({par0, par1});

        ASSERT_ERROR(StringFunctions::regexp_replace_prepare(ctx0.get(), scope));
        ASSERT_OK(StringFunctions::regexp_close(ctx0.get(), scope));
    }
}

PARALLEL_TEST(VecStringFunctionsTest, regexpReplace) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto ptn = BinaryColumn::create();
    auto replace = BinaryColumn::create();

    std::string strs[] = {"a b c", "a b c"};
    std::string ptns[] = {" ", "(b)"};
    std::string replaces[] = {"-", "<\\1>"};

    std::string res[] = {"a-b-c", "a <b> c"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        ptn->append(ptns[i]);
        replace->append(replaces[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(ptn);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_replace_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_replace(context, columns).value();
    auto v = ColumnHelper::as_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, regexpReplaceWithEmptyPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto ptn = ColumnHelper::create_const_column<TYPE_VARCHAR>("", 1);
    auto replace = BinaryColumn::create();

    std::string strs[] = {"yyyy-mm-dd", "yyyy-mm-dd"};
    std::string replaces[] = {"CHINA", "CHINA"};

    std::string res[] = {"CHINAyCHINAyCHINAyCHINAyCHINA-CHINAmCHINAmCHINA-CHINAdCHINAdCHINA",
                         "CHINAyCHINAyCHINAyCHINAyCHINA-CHINAmCHINAmCHINA-CHINAdCHINAdCHINA"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        replace->append(replaces[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(ptn);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_replace_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_replace(context, columns).value();
    auto v = ColumnHelper::as_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, replaceNullablePattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();
    auto null = NullColumn::create();
    auto replace = BinaryColumn::create();

    const std::string strs[] = {"a u z", "a sdfwe b c", "a equals c"};
    const std::string replaces[] = {"Ü", " ", ""};

    const std::string res[] = {"a Ü z", "a sdfwe b c", "ac"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        replace->append(replaces[i]);
    }

    pattern->append("u");
    pattern->append("dsdfsf");
    pattern->append(" equals ");
    null->append(0);
    null->append(1);
    null->append(0);

    columns.emplace_back(str);
    columns.emplace_back(NullableColumn::create(pattern, null));
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    const auto result = StringFunctions::replace(context, columns).value();
    const auto v =
            ColumnHelper::cast_to<TYPE_VARCHAR>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    EXPECT_EQ(res[0], v->get_data()[0].to_string());
    EXPECT_TRUE(result->is_null(1));
    EXPECT_EQ(res[2], v->get_data()[2].to_string());

    ASSERT_TRUE(StringFunctions::replace_close(context,
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, replaceOnlyNullPattern1) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = ColumnHelper::create_const_null_column(1);
    auto replace = BinaryColumn::create();

    const std::string strs[] = {"a b c", "a sdfwe b c"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        replace->append(strs[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    const auto result = StringFunctions::replace(context, columns).value();

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        EXPECT_TRUE(result->is_null(i));
    }

    ASSERT_TRUE(StringFunctions::replace_close(context,
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// Test replace when input is only_null column.
PARALLEL_TEST(VecStringFunctionsTest, replaceOnlyNullPattern2) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = ColumnHelper::create_const_null_column(2);
    auto pattern = ColumnHelper::create_const_null_column(1);
    auto replace = ColumnHelper::create_const_null_column(1);

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    const auto result = StringFunctions::replace(context, columns).value();

    EXPECT_EQ(result->size(), 2);
    EXPECT_TRUE(result->only_null());
    EXPECT_TRUE(result->is_constant());

    ASSERT_TRUE(StringFunctions::replace_close(context,
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// Test replace when input is not only_null column but pattern/replace is only_null
PARALLEL_TEST(VecStringFunctionsTest, replaceOnlyNullPattern2) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = ColumnHelper::create_const_column<TYPE_VARCHAR>("a b c", 2);
    auto pattern = ColumnHelper::create_const_null_column(1);
    auto replace = ColumnHelper::create_const_null_column(1);

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    const auto result = StringFunctions::replace(context, columns).value();

    EXPECT_EQ(result->size(), 2);
    EXPECT_TRUE(result->only_null());
    EXPECT_TRUE(result->is_constant());

    ASSERT_TRUE(StringFunctions::replace_close(context,
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, replaceConstPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto ptn = ColumnHelper::create_const_column<TYPE_VARCHAR>(" ", 1);
    auto replace = BinaryColumn::create();

    const std::string strs[] = {"a b c", "a sdfwe b c"};
    const std::string replaces[] = {"-", "< > "};

    const std::string res[] = {"a-b-c", "a< > sdfwe< > b< > c"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        replace->append(replaces[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(ptn);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    const auto result = StringFunctions::replace(context, columns).value();
    const auto v = ColumnHelper::as_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(StringFunctions::replace_close(context,
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// Test replace when input is const column and pattern/replace is not const column
PARALLEL_TEST(VecStringFunctionsTest, replaceConstColumn1) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = ColumnHelper::create_const_column<TYPE_VARCHAR>("a b c", 2);
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>(" ", 1);
    auto replace = BinaryColumn::create();
    const std::string replaces[] = {"-", "+"};
    for (int i = 0; i < sizeof(replaces) / sizeof(replaces[0]); ++i) {
        replace->append(replaces[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    const auto result = StringFunctions::replace(context, columns).value();
    ASSERT_TRUE(!result->is_constant());
    ASSERT_EQ(result->size(), 2);

    const std::string res[] = {"a-b-c", "a+b+c"};
    const auto vv = ColumnHelper::as_column<BinaryColumn>(result);
    for (int i = 0; i < vv->size(); i++) {
        ASSERT_EQ(res[i], vv->get_data()[i].to_string());
    }

    ASSERT_TRUE(StringFunctions::replace_close(context,
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// Test replace when input/pattern/replace are all const columns
PARALLEL_TEST(VecStringFunctionsTest, replaceConstColumn2) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = ColumnHelper::create_const_column<TYPE_VARCHAR>("a b c", 2);
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>(" ", 1);
    auto replace = ColumnHelper::create_const_column<TYPE_VARCHAR>("+", 1);

    columns.emplace_back(str);
    columns.emplace_back(pattern);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    const auto result = StringFunctions::replace(context, columns).value();
    ASSERT_TRUE(result->is_constant());
    ASSERT_EQ(result->size(), 2);
    const auto v = ColumnHelper::as_column<ConstColumn>(result);
    const auto vv = ColumnHelper::as_column<BinaryColumn>(v->data_column());
    ASSERT_EQ("a+b+c", vv->get_data()[0].to_string());

    ASSERT_TRUE(StringFunctions::replace_close(context,
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, replace) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto ptn = BinaryColumn::create();
    auto replace = BinaryColumn::create();

    const std::string strs[] = {"a b c", "a . c", "a b c", "abc?", "xyz"};
    const std::string ptns[] = {" ", ".", "^a", "abc?", "z$"};
    const std::string replaces[] = {"-", "*\\*", " ", "xyz", " "};

    const std::string res[] = {"a-b-c", "a *\\* c", "a b c", "xyz", "xyz"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        ptn->append(ptns[i]);
        replace->append(replaces[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(ptn);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    const auto result = StringFunctions::replace(context, columns).value();
    const auto v = ColumnHelper::as_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(StringFunctions::replace_close(context,
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, replaceWithEmptyPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto ptn = ColumnHelper::create_const_column<TYPE_VARCHAR>("", 1);
    auto replace = BinaryColumn::create();

    const std::string strs[] = {"yyyy-mm-dd", "*starrocks."};
    const std::string replaces[] = {"CHINA", "CHINA"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        replace->append(replaces[i]);
    }

    columns.emplace_back(str);
    columns.emplace_back(ptn);
    columns.emplace_back(replace);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::replace_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    const auto result = StringFunctions::replace(context, columns).value();
    const auto v = ColumnHelper::as_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        ASSERT_EQ(strs[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(StringFunctions::replace_close(context,
                                               FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, moneyFormatDouble) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    double moneys[] = {1234.456, 1234.45, 1234.4, 1234.454};
    std::string results[] = {"1,234.46", "1,234.45", "1,234.40", "1,234.45"};

    Columns columns;
    auto money = DoubleColumn::create();

    for (double i : moneys) money->append(i);

    columns.emplace_back(money);
    ColumnPtr result = StringFunctions::money_format_double(ctx.get(), columns).value();
    auto v = ColumnHelper::as_raw_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(moneys) / sizeof(moneys[0]); ++i) ASSERT_EQ(results[i], v->get_data()[i].to_string());
}

PARALLEL_TEST(VecStringFunctionsTest, moneyFormatBigInt) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    int64_t moneys[] = {123456, -123456, 9223372036854775807};
    std::string results[] = {"123,456.00", "-123,456.00", "9,223,372,036,854,775,807.00"};

    Columns columns;
    auto money = Int64Column::create();

    for (long i : moneys) money->append(i);

    columns.emplace_back(money);
    ColumnPtr result = StringFunctions::money_format_bigint(ctx.get(), columns).value();
    auto v = ColumnHelper::as_raw_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(moneys) / sizeof(moneys[0]); ++i) ASSERT_EQ(results[i], v->get_data()[i].to_string());
}

PARALLEL_TEST(VecStringFunctionsTest, moneyFormatLargeInt) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    std::string str[] = {"170141183460469231731617303715884105727", "170141183460469231731687303715884105727",
                         "170141183460469231731687303715884105723"};
    __int128 moneys[sizeof(str) / sizeof(str[0])];
    for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
        std::stringstream ss;
        ss << str[i];
        ss >> moneys[i];
    }
    std::string results[] = {"170,141,183,460,469,231,731,617,303,715,884,105,727.00",
                             "170,141,183,460,469,231,731,687,303,715,884,105,727.00",
                             "170,141,183,460,469,231,731,687,303,715,884,105,723.00"};

    Columns columns;
    auto money = Int128Column::create();

    for (__int128 i : moneys) {
        money->append(i);
    }

    columns.emplace_back(money);
    ColumnPtr result = StringFunctions::money_format_largeint(ctx.get(), columns).value();
    auto v = ColumnHelper::as_raw_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(moneys) / sizeof(moneys[0]); ++i) ASSERT_EQ(results[i], v->get_data()[i].to_string());
}

PARALLEL_TEST(VecStringFunctionsTest, moneyFormatDecimalV2Value) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    std::string str[] = {"3333333333.2222222222", "-740740740.71604938271975308642"};
    DecimalV2Value moneys[sizeof(str) / sizeof(str[0])];
    for (int i = 0; i < sizeof(str) / sizeof(str[0]); ++i) {
        moneys[i] = DecimalV2Value(str[i]);
    }
    std::string results[] = {"3,333,333,333.22", "-740,740,740.72"};

    Columns columns;
    auto money = DecimalColumn::create();

    for (auto i : moneys) {
        money->append(i);
    }

    columns.emplace_back(money);
    ColumnPtr result = StringFunctions::money_format_decimalv2val(ctx.get(), columns).value();
    auto v = ColumnHelper::as_raw_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(moneys) / sizeof(moneys[0]); ++i) ASSERT_EQ(results[i], v->get_data()[i].to_string());
}

PARALLEL_TEST(VecStringFunctionsTest, parseUrlNullable) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto data = BinaryColumn::create();
    auto null = NullColumn::create();

    std::string strs[] = {"http://cccccc:password@hostname/dsfsf?vdv=value#xcvxv",
                          "http://werwrw:sdf@sdfsceesvdsdvs/ccvwfewf?cvx=value#sdfs",
                          "http://vdvsv:df23@hostname/path?cvxvv=value#dsfs"};

    for (auto& i : strs) {
        str->append(i);
    }

    data->append("PATH");
    data->append("HOST");
    data->append("PROTOCOL");
    null->append(0);
    null->append(0);
    null->append(1);

    columns.emplace_back(str);
    columns.emplace_back(NullableColumn::create(data, null));

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::parse_url_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    auto result = StringFunctions::parse_url(context, columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    ASSERT_EQ("/dsfsf", v->get_data()[0].to_string());
    ASSERT_EQ("sdfsceesvdsdvs", v->get_data()[1].to_string());
    ASSERT_TRUE(result->is_null(2));

    ASSERT_TRUE(StringFunctions::parse_url_close(context,
                                                 FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, parseUrlOnlyNull) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto part = ColumnHelper::create_const_null_column(1);

    std::string strs[] = {"http://cccccc:password@hostname/dsfsf?vdv=value#xcvxv",
                          "http://werwrw:sdf@hostname/path?cvx=value#sdfs",
                          "http://vdvsv:df23@hostname/path?cvxvv=value#dsfs"};

    for (auto& i : strs) {
        str->append(i);
    }

    columns.emplace_back(str);
    columns.emplace_back(part);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::parse_url_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    auto result = StringFunctions::parse_url(context, columns).value();

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        ASSERT_TRUE(result->is_null(i));
    }

    ASSERT_TRUE(StringFunctions::parse_url_close(context,
                                                 FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, parseUrlForConst) {
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto part = ColumnHelper::create_const_column<TYPE_VARCHAR>("AUTHORITY", 1);

        std::string strs[] = {"http://username:password@hostname/path?arg=value#anchor",
                              "http://starrockssss:apache/csdwwww?arg=value#anchor",
                              "http://wobushinidehao:kjkljq/wfefefe?arg=value#anchor"};

        std::string res[] = {"username:password@hostname", "starrockssss:apache", "wobushinidehao:kjkljq"};

        for (auto& i : strs) {
            str->append(i);
        }

        columns.emplace_back(str);
        columns.emplace_back(part);

        context->set_constant_columns(columns);

        ASSERT_TRUE(
                StringFunctions::parse_url_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

        auto result = StringFunctions::parse_url(context, columns).value();
        auto v = ColumnHelper::as_column<BinaryColumn>(result);

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], v->get_data()[i].to_string());
        }

        ASSERT_TRUE(StringFunctions::parse_url_close(
                            context, FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }

    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto part = ColumnHelper::create_const_column<TYPE_VARCHAR>("PATH", 1);

        std::string strs[] = {"http://useraadfname:password@hostname/path?arg=value#anchor",
                              "http://starrockssxxxss:apache/csdwwww?arg=value#anchor",
                              "http://wobushxinidehao:kjksljq/wfefefe?arg=value#anchor"};

        std::string res[] = {"/path", "/csdwwww", "/wfefefe"};

        for (auto& i : strs) {
            str->append(i);
        }

        columns.emplace_back(str);
        columns.emplace_back(part);

        context->set_constant_columns(columns);

        ASSERT_TRUE(
                StringFunctions::parse_url_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

        auto result = StringFunctions::parse_url(context, columns).value();
        auto v = ColumnHelper::as_column<BinaryColumn>(result);

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], v->get_data()[i].to_string());
        }

        ASSERT_TRUE(StringFunctions::parse_url_close(
                            context, FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }
}

PARALLEL_TEST(VecStringFunctionsTest, parseUrl) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto part = BinaryColumn::create();

    std::string strs[] = {"http://username:password@hostname/path?arg=value#anchor"};

    std::string parts[] = {"AUTHORITY", "FILE", "HOST", "PATH", "QUERY", "REF", "USERINFO", "PROTOCOL"};

    std::string res[] = {"username:password@hostname",
                         "/path?arg=value",
                         "hostname",
                         "/path",
                         "arg=value",
                         "anchor",
                         "username:password",
                         "http"};

    for (auto& i : parts) {
        str->append(strs[0]);
        part->append(i);
    }

    columns.emplace_back(str);
    columns.emplace_back(part);

    context->set_constant_columns(columns);

    ASSERT_TRUE(StringFunctions::parse_url_prepare(context, FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());

    auto result = StringFunctions::parse_url(context, columns).value();
    auto v = ColumnHelper::as_column<BinaryColumn>(result);

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], v->get_data()[i].to_string());
    }

    ASSERT_TRUE(StringFunctions::parse_url_close(context,
                                                 FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, hex_intTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto ints = Int64Column::create();

    int64_t values[] = {21, 16, 256, 514};
    std::string strs[] = {"15", "10", "100", "202"};

    for (long value : values) {
        ints->append(value);
    }

    columns.emplace_back(ints);

    ColumnPtr result = StringFunctions::hex_int(ctx.get(), columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        ASSERT_EQ(strs[j], v->get_data()[j].to_string());
    }
}

PARALLEL_TEST(VecStringFunctionsTest, hex_stringTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto ints = BinaryColumn::create();

    std::string values[] = {"21", "16", "256", "514"};
    std::string strs[] = {"3231", "3136", "323536", "353134"};

    for (auto& value : values) {
        ints->append(value);
    }

    columns.emplace_back(ints);

    ColumnPtr result = StringFunctions::hex_string(ctx.get(), columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        ASSERT_EQ(strs[j], v->get_data()[j].to_string());
    }
}

PARALLEL_TEST(VecStringFunctionsTest, unhexTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    auto ints = BinaryColumn::create();

    std::string strs[] = {"21", "16", "256", "514"};
    std::string values[] = {"3231", "3136", "323536", "353134"};

    for (auto& value : values) {
        ints->append(value);
    }

    columns.emplace_back(ints);

    ColumnPtr result = StringFunctions::unhex(ctx.get(), columns).value();
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        ASSERT_EQ(strs[j], v->get_data()[j].to_string());
    }
}

static void test_left_and_right_not_const(
        std::vector<std::tuple<std::string, int, std::string, std::string>> const& cases) {
    // left_not_const and right_not_const
    std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context());
    Columns columns;
    auto str_col = BinaryColumn::create();
    auto len_col = Int32Column::create();
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

PARALLEL_TEST(VecStringFunctionsTest, leftAndRightNotConstASCIITest) {
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
PARALLEL_TEST(VecStringFunctionsTest, leftAndRightNotConstUtf8Test) {
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
        columns.push_back(str_col);
        columns.push_back(ConstColumn::create(len_col, 1));
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

PARALLEL_TEST(VecStringFunctionsTest, leftAndRightConstASCIITest) {
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
    test_left_and_right_const(str_col, cases);
}

PARALLEL_TEST(VecStringFunctionsTest, leftAndRightConstUtf8Test) {
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
    test_left_and_right_const(str_col, cases);
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
    Columns columns{str_col, off_col, len_col};
    auto result = StringFunctions::substring(context.get(), columns).value();
    auto* binary_result = down_cast<BinaryColumn*>(result.get());
    const auto size = cases.size();
    ASSERT_TRUE(binary_result != nullptr);
    ASSERT_EQ(binary_result->size(), size);
    for (auto i = 0; i < size; ++i) {
        ASSERT_EQ(binary_result->get_slice(i).to_string(), std::get<3>(cases[i]));
    }
}

PARALLEL_TEST(VecStringFunctionsTest, substrNotConstASCIITest) {
    ColumnPtr str_col = BinaryColumn::create();
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

PARALLEL_TEST(VecStringFunctionsTest, substrNotConstUtf8Test) {
    ColumnPtr str_col = BinaryColumn::create();
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

PARALLEL_TEST(VecStringFunctionsTest, strcmpTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto lhs = BinaryColumn::create();
    auto rhs = BinaryColumn::create();

    lhs->append("");
    rhs->append("");

    lhs->append("");
    rhs->append("text1");

    lhs->append("text2");
    rhs->append("");

    lhs->append("text1");
    rhs->append("text1");

    lhs->append("text1");
    rhs->append("text2");

    lhs->append("text2");
    rhs->append("text1");

    columns.emplace_back(lhs);
    columns.emplace_back(rhs);

    ColumnPtr result = StringFunctions::strcmp(ctx.get(), columns).value();
    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    ASSERT_EQ(6, result->size());
    ASSERT_EQ(0, v->get_data()[0]);
    ASSERT_EQ(-1, v->get_data()[1]);
    ASSERT_EQ(1, v->get_data()[2]);
    ASSERT_EQ(0, v->get_data()[3]);
    ASSERT_EQ(-1, v->get_data()[4]);
    ASSERT_EQ(1, v->get_data()[5]);
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractAllPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();
    auto index = Int64Column::create();

    std::string strs[] = {"AbCdE", "AbCdrrCryE", "hitCdeciCsionCdlist", "hitCdecCisiCondlCist", "12342356"};
    std::string res[] = {"['b']", "['b']", "['hit','sion']", "['hit','isi']", "[]"};
    int indexs[] = {1, 1, 1, 1, 1};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        pattern->append("([[:lower:]]+)C([[:lower:]]+)");
        index->append(indexs[i]);
    }

    columns.push_back(str);
    columns.push_back(pattern);
    columns.push_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract_all(context, columns).value();

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        ASSERT_EQ(res[i], result->debug_item(i));
    }
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractAllNullablePattern1) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();
    auto index = Int64Column::create();

    std::string strs[] = {"AbCdE", "AbCdrrryE", "hitdeciCsiondlist", "hitdecCisiondlist"};
    int indexs[] = {1, 2, 1, 2};

    std::string res[] = {"['b']", "['drrry']", "['hitdeci']", "['isiondlist']"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        pattern->append("([[:lower:]]+)C([[:lower:]]+)");
        index->append(indexs[i]);
    }

    columns.push_back(str);
    columns.push_back(pattern);
    columns.push_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    auto result = StringFunctions::regexp_extract_all(context, columns).value();
    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        ASSERT_EQ(res[i], result->debug_item(i));
    }
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractAllNullablePattern2) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = BinaryColumn::create();
    auto null = NullColumn::create();
    auto index = Int64Column::create();

    std::string strs[] = {"AbCdE", "AbCdrrryE", "hitdeciCsiondlist", "hitdecCisioedlise"};
    int indexs[] = {1, 2, 1, 2};

    std::string res[] = {"NULL", "['drrry']", "NULL", "['td','sio','s']"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        pattern->append(i < 2 ? "([[:lower:]]+)C([[:lower:]]+)" : "(i)(.*?)(e)");
        null->append(i % 2 == 0);
        index->append(indexs[i]);
    }

    columns.push_back(str);
    columns.push_back(NullableColumn::create(pattern, null));
    columns.push_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract_all(context, columns).value();

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        ASSERT_EQ(res[i], result->debug_item(i));
    }
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractAllOnlyNullPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = ColumnHelper::create_const_null_column(1);
    auto index = Int64Column::create();

    int length = 4;

    for (int i = 0; i < length; ++i) {
        str->append("test" + std::to_string(i));
        index->append(1);
    }

    columns.push_back(str);
    columns.push_back(pattern);
    columns.push_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract_all(context, columns).value();
    for (int i = 0; i < length; ++i) {
        ASSERT_TRUE(result->is_null(i));
    }

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractAllConstPattern) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("([[:lower:]]+)C([[:lower:]]+)", 1);
    auto index = Int64Column::create();

    std::string strs[] = {"AbCdE", "AbCdrrryE", "hitdeciCsiondlist", "hitdecCisiondlist"};
    int indexs[] = {1, 2, 1, 2};

    std::string res[] = {"['b']", "['drrry']", "['hitdeci']", "['isiondlist']"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
        index->append(indexs[i]);
    }

    columns.push_back(str);
    columns.push_back(pattern);
    columns.push_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract_all(context, columns).value();

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());

    for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
        ASSERT_EQ(res[i], result->debug_item(i));
    }
}

PARALLEL_TEST(VecStringFunctionsTest, regexpExtractAllConst) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto context = ctx.get();

    Columns columns;

    auto str = BinaryColumn::create();
    auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("([[:lower:]]+)C([[:lower:]]+)", 5);
    auto index = ColumnHelper::create_const_column<TYPE_BIGINT>(2, 5);

    std::string strs[] = {"AbCdE", "AbCdrrCryE", "hitCdeciCsionCdlist", "hitCdecCisiCondlCist", "12342356"};
    std::string res[] = {"['d']", "['drr']", "['deci','dlist']", "['dec','ondl']", "[]"};

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        str->append(strs[i]);
    }

    columns.push_back(str);
    columns.push_back(pattern);
    columns.push_back(index);

    context->set_constant_columns(columns);

    ASSERT_TRUE(
            StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    auto result = StringFunctions::regexp_extract_all(context, columns).value();

    ASSERT_TRUE(
            StringFunctions::regexp_close(context, FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                    .ok());

    for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
        ASSERT_EQ(res[i], result->debug_item(i));
    }
}

PARALLEL_TEST(VecStringFunctionsTest, crc32Test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    str->append("starrocks");
    str->append("STARROCKS");
    columns.push_back(str);

    ASSERT_TRUE(StringFunctions::crc32(ctx.get(), columns).ok());
    ColumnPtr result = StringFunctions::crc32(ctx.get(), columns).value();
    auto v = ColumnHelper::cast_to<TYPE_BIGINT>(result);
    ASSERT_EQ(static_cast<uint32_t>(2312449062), v->get_data()[0]);
    ASSERT_EQ(static_cast<uint32_t>(3440849609), v->get_data()[1]);
}

PARALLEL_TEST(VecStringFunctionsTest, regexpSplitTest) {
    // const pattern, const max_split - default_max_split
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("[ABC]", 1);

        std::string strs[] = {"oneAtwoBthreeC", "1A2B3C", "AABBCC"};
        std::string res[] = {"['one','two','three','']", "['1','2','3','']", "['','','','','','','']"};

        for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
            str->append(strs[i]);
        }

        columns.push_back(str);
        columns.push_back(pattern);

        context->set_constant_columns(columns);

        ASSERT_TRUE(StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());
        auto result = StringFunctions::regexp_split(context, columns).value();

        ASSERT_TRUE(StringFunctions::regexp_close(context,
                                                  FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], result->debug_item(i));
        }
    }

    // const pattern, const max_split - customized_max_split
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto null = NullColumn::create();
        auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("[ABC]", 1);
        auto max_split = ColumnHelper::create_const_column<TYPE_INT>(2, 1);

        std::string strs[] = {"oneAtwoBthreeC", "1A2B3C", "AABBCC", "AABBCC"};
        std::string res[] = {"['one','twoBthreeC']", "['1','2B3C']", "['','ABBCC']", "NULL"};

        for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
            str->append(strs[i]);
            null->append(i == 3 ? 1 : 0);
        }

        columns.push_back(NullableColumn::create(str, null));
        columns.push_back(pattern);
        columns.push_back(max_split);

        context->set_constant_columns(columns);

        ASSERT_TRUE(StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());
        auto result = StringFunctions::regexp_split(context, columns).value();

        ASSERT_TRUE(StringFunctions::regexp_close(context,
                                                  FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], result->debug_item(i));
        }
    }

    // const pattern - default_max_split
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("[ABC]", 1);

        std::string strs[] = {"oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC",
                              "oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC"};

        std::string res[] = {"['one','two','three','']", "['one','two','three','']", "['one','two','three','']",
                             "['one','two','three','']", "['one','two','three','']", "['one','two','three','']"};

        for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
            str->append(strs[i]);
        }

        columns.push_back(str);
        columns.push_back(pattern);

        context->set_constant_columns(columns);

        ASSERT_TRUE(StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());
        auto result = StringFunctions::regexp_split(context, columns).value();

        ASSERT_TRUE(StringFunctions::regexp_close(context,
                                                  FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], result->debug_item(i));
        }
    }

    // const pattern - customized_max_split
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto null = NullColumn::create();
        auto pattern = ColumnHelper::create_const_column<TYPE_VARCHAR>("[ABC]", 1);
        auto max_split = Int32Column::create();

        std::string strs[] = {"oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC",
                              "oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC"};
        int max_splits[] = {-1, 0, 1, 2, 3, 4, 5};

        std::string res[] = {"['one','two','three','']",
                             "['one','two','three','']",
                             "['oneAtwoBthreeC']",
                             "['one','twoBthreeC']",
                             "['one','two','threeC']",
                             "['one','two','three','']",
                             "NULL"};

        for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
            str->append(strs[i]);
            null->append(i == 6 ? 1 : 0);
            max_split->append(max_splits[i]);
        }

        columns.push_back(NullableColumn::create(str, null));
        columns.push_back(pattern);
        columns.push_back(max_split);

        context->set_constant_columns(columns);

        ASSERT_TRUE(StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());
        auto result = StringFunctions::regexp_split(context, columns).value();

        ASSERT_TRUE(StringFunctions::regexp_close(context,
                                                  FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], result->debug_item(i));
        }
    }

    // const max_split - default_max_split
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto pattern = BinaryColumn::create();

        std::string strs[] = {"oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC"};
        std::string patterns[] = {"[nwe]", "[ne]", "[123]"};
        std::string res[] = {"['o','','At','oBthr','','C']", "['o','','AtwoBthr','','C']", "['oneAtwoBthreeC']"};

        for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
            str->append(strs[i]);
            pattern->append(patterns[i]);
        }

        columns.push_back(str);
        columns.push_back(pattern);

        context->set_constant_columns(columns);

        ASSERT_TRUE(StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());
        auto result = StringFunctions::regexp_split(context, columns).value();

        ASSERT_TRUE(StringFunctions::regexp_close(context,
                                                  FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], result->debug_item(i));
        }
    }

    // const max_split - customized_max_split
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto pattern = BinaryColumn::create();
        auto null = NullColumn::create();
        auto max_split = ColumnHelper::create_const_column<TYPE_INT>(4, 1);

        std::string strs[] = {"oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC"};
        std::string patterns[] = {"[nwe]", "[ne]", "[123]", "[123]"};
        std::string res[] = {"['o','','At','oBthreeC']", "['o','','AtwoBthr','eC']", "['oneAtwoBthreeC']", "NULL"};

        for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
            str->append(strs[i]);
            pattern->append(patterns[i]);
            null->append(i == 3 ? 1 : 0);
        }

        columns.push_back(str);
        columns.push_back(NullableColumn::create(pattern, null));
        columns.push_back(max_split);

        context->set_constant_columns(columns);

        ASSERT_TRUE(StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());
        auto result = StringFunctions::regexp_split(context, columns).value();

        ASSERT_TRUE(StringFunctions::regexp_close(context,
                                                  FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], result->debug_item(i));
        }
    }

    // none const - default_max_split
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto pattern = BinaryColumn::create();

        std::string strs[] = {"oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC"};
        std::string patterns[] = {"[nwe]", "[ne]", "[123]"};

        std::string res[] = {"['o','','At','oBthr','','C']", "['o','','AtwoBthr','','C']", "['oneAtwoBthreeC']"};

        for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
            str->append(strs[i]);
            pattern->append(patterns[i]);
        }

        columns.push_back(str);
        columns.push_back(pattern);

        context->set_constant_columns(columns);

        ASSERT_TRUE(StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());
        auto result = StringFunctions::regexp_split(context, columns).value();

        ASSERT_TRUE(StringFunctions::regexp_close(context,
                                                  FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], result->debug_item(i));
        }
    }

    // none const - customized_max_split
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto context = ctx.get();

        Columns columns;

        auto str = BinaryColumn::create();
        auto pattern = BinaryColumn::create();
        auto null = NullColumn::create();
        auto max_split = Int32Column::create();

        std::string strs[] = {"oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC", "oneAtwoBthreeC"};
        std::string patterns[] = {"[nwe]", "[ne]", "[123]", "[123]"};
        int max_splits[] = {1, 2, 3, 4};

        std::string res[] = {"['oneAtwoBthreeC']", "['o','eAtwoBthreeC']", "['oneAtwoBthreeC']", "NULL"};

        for (int i = 0; i < sizeof(strs) / sizeof(strs[0]); ++i) {
            str->append(strs[i]);
            pattern->append(patterns[i]);
            null->append(i == 3 ? 1 : 0);
            max_split->append(max_splits[i]);
        }

        columns.push_back(str);
        columns.push_back(NullableColumn::create(pattern, null));
        columns.push_back(max_split);

        context->set_constant_columns(columns);

        ASSERT_TRUE(StringFunctions::regexp_extract_prepare(context, FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());
        auto result = StringFunctions::regexp_split(context, columns).value();

        ASSERT_TRUE(StringFunctions::regexp_close(context,
                                                  FunctionContext::FunctionContext::FunctionStateScope::THREAD_LOCAL)
                            .ok());

        for (int i = 0; i < sizeof(res) / sizeof(res[0]); ++i) {
            ASSERT_EQ(res[i], result->debug_item(i));
        }
    }
}

} // namespace starrocks
