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

#include <gtest/gtest.h>

#include "exprs/string_functions.h"
#include "gen_cpp/Exprs_types.h"

namespace starrocks {

class StringFunctionLocateTest : public ::testing::Test {
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

TEST_F(StringFunctionLocateTest, instrTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto sub = BinaryColumn::create();

    for (int j = 0; j < 20; ++j) {
        str->append("abcd" + std::to_string(j));
        sub->append(std::to_string(j));
    }

    columns.emplace_back(std::move(str));
    columns.emplace_back(std::move(sub));

    ColumnPtr result = StringFunctions::instr(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < 20; ++j) {
        ASSERT_EQ(5, v->get_data()[j]);
    }
}

TEST_F(StringFunctionLocateTest, instrChineseTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto sub = BinaryColumn::create();

    for (int j = 0; j < 20; ++j) {
        str->append("中文字符" + std::to_string(j));
        sub->append(std::to_string(j));
    }

    columns.emplace_back(std::move(str));
    columns.emplace_back(std::move(sub));

    ColumnPtr result = StringFunctions::instr(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < 20; ++j) {
        ASSERT_EQ(5, v->get_data()[j]);
    }
}

TEST_F(StringFunctionLocateTest, locateNullTest) {
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

    columns.emplace_back(NullableColumn::create(std::move(sub), std::move(null)));
    columns.emplace_back(std::move(str));

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

TEST_F(StringFunctionLocateTest, locatePosTest) {
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

    columns.emplace_back(std::move(sub));
    columns.emplace_back(std::move(str));
    columns.emplace_back(std::move(pos));

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

TEST_F(StringFunctionLocateTest, locateStartBeyondLastUtf8CharTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto sub = BinaryColumn::create();
    auto str = BinaryColumn::create();
    auto pos = Int32Column::create();

    // a start position past the last character leaves nothing to scan, so the result is 0;
    // the walk has to step over the whole last character to reach that end
    str->append("你a");
    sub->append("a");
    pos->append(3);

    str->append("你好b");
    sub->append("b");
    pos->append(4);

    // a start position on the last character still finds it
    str->append("你好");
    sub->append("好");
    pos->append(2);

    // the 0xF0 lead byte claims four bytes while only two are left, so the walk ends at the end
    // of the value and the search is left with nothing to scan
    str->append(
            Slice("a\xF0"
                  "b",
                  3));
    sub->append("b");
    pos->append(3);

    columns.emplace_back(std::move(sub));
    columns.emplace_back(std::move(str));
    columns.emplace_back(std::move(pos));

    ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
    ASSERT_EQ(4, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);
    ASSERT_EQ(0, v->get_data()[0]);
    ASSERT_EQ(0, v->get_data()[1]);
    ASSERT_EQ(2, v->get_data()[2]);
    ASSERT_EQ(0, v->get_data()[3]);
}

TEST_F(StringFunctionLocateTest, locateHayStackEqualNeedleTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto sub = BinaryColumn::create();
    auto pos = Int32Column::create();

    for (int j = 0; j < 20; ++j) {
        str->append("abcd" + std::to_string(j));
        sub->append("abcd" + std::to_string(j));
        pos->append(1);
    }

    columns.emplace_back(std::move(sub));
    columns.emplace_back(std::move(str));
    columns.emplace_back(std::move(pos));

    ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < 20; ++j) {
        ASSERT_EQ(1, v->get_data()[j]);
    }
}

TEST_F(StringFunctionLocateTest, locateNegativePosTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto sub = BinaryColumn::create();
    auto pos = Int32Column::create();

    for (int j = 0; j < 20; ++j) {
        str->append("abcd" + std::to_string(j));
        sub->append(std::to_string(j));
        pos->append(-4);
    }

    columns.emplace_back(std::move(sub));
    columns.emplace_back(std::move(str));
    columns.emplace_back(std::move(pos));

    ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < 20; ++j) {
        ASSERT_EQ(0, v->get_data()[j]);
    }
}

TEST_F(StringFunctionLocateTest, locatePosLargerThanHaystackSizeTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto sub = BinaryColumn::create();
    auto pos = Int32Column::create();

    for (int j = 0; j < 20; ++j) {
        str->append(std::to_string(j) + "abcd" + std::to_string(j));
        sub->append(std::to_string(j));
        pos->append(10);
    }

    columns.emplace_back(std::move(sub));
    columns.emplace_back(std::move(str));
    columns.emplace_back(std::move(pos));

    ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
    ASSERT_EQ(20, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < 20; ++j) {
        ASSERT_EQ(0, v->get_data()[j]);
    }
}

TEST_F(StringFunctionLocateTest, locateNeedleAllNullTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto haystack = BinaryColumn::create();
    auto start_pos = Int32Column::create();

    std::string haystack_data = "治世不一道，便国不法古。";
    for (int i = 0; i < 100; ++i) {
        haystack->append(haystack_data);
        start_pos->append(3);
    }
    columns.emplace_back(ColumnHelper::create_const_null_column(1));
    columns.emplace_back(std::move(haystack));
    columns.emplace_back(std::move(start_pos));

    ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
    ASSERT_EQ(1, result->size());

    auto v = ColumnHelper::as_column<ConstColumn>(result);

    for (int j = 0; j < 1; ++j) {
        ASSERT_TRUE(v->is_null(j));
    }
}

TEST_F(StringFunctionLocateTest, locateNeedleEmptyTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto haystack = BinaryColumn::create();
    auto needle = BinaryColumn::create();
    auto start_pos = Int32Column::create();

    std::string haystack_data = "ababababababa";
    // repeat needle_const 5 times in haystack_data
    for (int i = 0; i < 100; ++i) {
        if (i < 50) {
            haystack->append("");
            needle->append("");
            start_pos->append(1);
        } else if (i < 100) {
            haystack->append(haystack_data);
            needle->append("");
            start_pos->append(2);
        }
    }

    columns.emplace_back(std::move(needle));
    columns.emplace_back(std::move(haystack));
    columns.emplace_back(std::move(start_pos));

    ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
    ASSERT_EQ(100, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int i = 0; i < 100; ++i) {
        if (i < 50) {
            ASSERT_EQ(1, v->get_data()[i]);
        } else {
            ASSERT_EQ(2, v->get_data()[i]);
        }
    }
}

TEST_F(StringFunctionLocateTest, locateInVolnitskyTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto haystack = BinaryColumn::create();
    auto needle = BinaryColumn::create();
    auto start_pos = Int32Column::create();

    std::string haystack_data = "治世不一道，便国不法古。";
    std::string needle_const = "便国不法古";
    for (int i = 0; i < 100; ++i) {
        haystack->append(haystack_data);
        start_pos->append(3);
    }
    needle->append(needle_const);
    columns.emplace_back(ConstColumn::create(std::move(needle), 1));
    columns.emplace_back(std::move(haystack));
    columns.emplace_back(std::move(start_pos));

    ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
    ASSERT_EQ(100, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < 100; ++j) {
        ASSERT_EQ(7, v->get_data()[j]);
    }
}

TEST_F(StringFunctionLocateTest, locateInVolnitskyTest2) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto haystack = BinaryColumn::create();
    auto needle = BinaryColumn::create();
    auto start_pos = Int32Column::create();

    std::string haystack_data =
            "宋有人耕田者，田中有株，兔走触株，折颈而死，"
            "因释其耒而守株，冀复得兔，兔不可复得，而身为宋国笑。"
            "今欲以先王之政，治当世之民，皆守株之类也。";
    std::string needle_const = "因释其耒而守株";
    for (int i = 0; i < 100; ++i) {
        haystack->append(haystack_data);
        start_pos->append(3);
    }
    needle->append(needle_const);
    columns.emplace_back(ConstColumn::create(std::move(needle), 1));
    columns.emplace_back(std::move(haystack));
    columns.emplace_back(std::move(start_pos));

    ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
    ASSERT_EQ(100, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < 100; ++j) {
        ASSERT_EQ(23, v->get_data()[j]);
    }
}

TEST_F(StringFunctionLocateTest, locateInVolnitskyTest3) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto haystack = BinaryColumn::create();
    auto needle = BinaryColumn::create();
    auto start_pos = Int32Column::create();

    std::string haystack_data = "abcdefghijklmnopqrstuvwxyz\0";
    // sse4.1 is on 16 byte.
    // needle_const is 15 byte width, but it can match haystack_data
    // because haystack_data is ends with '\0'
    // it has to handle the case
    std::string needle_const = "lmnopqrstuvwxyz";
    for (int i = 0; i < 100; ++i) {
        haystack->append(haystack_data);
        start_pos->append(3);
    }
    needle->append(needle_const);
    columns.emplace_back(ConstColumn::create(std::move(needle), 1));
    columns.emplace_back(std::move(haystack));
    columns.emplace_back(std::move(start_pos));

    ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
    ASSERT_EQ(100, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < 100; ++j) {
        ASSERT_EQ(12, v->get_data()[j]);
    }
}

TEST_F(StringFunctionLocateTest, locateInVolnitskyTest4) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto haystack = BinaryColumn::create();
    auto needle = BinaryColumn::create();
    auto start_pos = Int32Column::create();

    std::string haystack_data = "ababababababa";
    // repeat needle_const 5 times in haystack_data
    std::string needle_const = "bab";
    for (int i = 0; i < 100; ++i) {
        haystack->append(haystack_data);
        start_pos->append(3);
    }
    needle->append(needle_const);
    columns.emplace_back(ConstColumn::create(std::move(needle), 1));
    columns.emplace_back(std::move(haystack));
    columns.emplace_back(std::move(start_pos));

    ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
    ASSERT_EQ(100, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < 100; ++j) {
        ASSERT_EQ(4, v->get_data()[j]);
    }
}

TEST_F(StringFunctionLocateTest, locateVolnitskyTest5) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto haystack = BinaryColumn::create();
    auto null = NullColumn::create();
    auto needle = BinaryColumn::create();
    auto start_pos = Int32Column::create();

    std::string haystack_data = "ababababababa";
    // repeat needle_const 5 times in haystack_data
    std::string needle_const = "bab";
    for (int i = 0; i < 100; ++i) {
        haystack->append(haystack_data);
        null->append(i % 2);
        start_pos->append(3);
    }

    needle->append(needle_const);
    columns.emplace_back(ConstColumn::create(std::move(needle), 1));
    columns.emplace_back(NullableColumn::create(std::move(haystack), std::move(null)));
    columns.emplace_back(std::move(start_pos));

    ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
    ASSERT_EQ(100, result->size());
    ASSERT_TRUE(result->is_nullable());

    auto v = ColumnHelper::cast_to<TYPE_INT>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());

    for (int i = 0; i < 100; ++i) {
        if (i % 2) {
            ASSERT_TRUE(result->is_null(i));
        } else {
            ASSERT_EQ(4, v->get_data()[i]);
        }
    }
}

TEST_F(StringFunctionLocateTest, locateVolnitskyTest6) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto haystack = BinaryColumn::create();
    auto needle = BinaryColumn::create();
    auto start_pos = Int32Column::create();

    std::string haystack_data = "ababababababa";
    // repeat needle_const 5 times in haystack_data
    std::string needle_const = "";
    for (int i = 0; i < 100; ++i) {
        if (i < 20) {
            haystack->append("");
            start_pos->append(1);
        } else if (i < 40) {
            haystack->append("");
            start_pos->append(2);
        } else if (i < 60) {
            haystack->append(haystack_data);
            start_pos->append(10);
        } else if (i < 80) {
            haystack->append(haystack_data);
            start_pos->append(20);
        } else {
            haystack->append(haystack_data);
            start_pos->append(40);
        }
    }

    needle->append(needle_const);
    columns.emplace_back(ConstColumn::create(std::move(needle), 1));
    columns.emplace_back(std::move(haystack));
    columns.emplace_back(std::move(start_pos));

    ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
    ASSERT_EQ(100, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int i = 0; i < 100; ++i) {
        if (i < 20) {
            ASSERT_EQ(1, v->get_data()[i]);
        } else if (i < 40) {
            ASSERT_EQ(0, v->get_data()[i]);
        } else if (i < 60) {
            ASSERT_EQ(10, v->get_data()[i]);
        } else if (i < 80) {
            ASSERT_EQ(0, v->get_data()[i]);
        } else {
            ASSERT_EQ(0, v->get_data()[i]);
        }
    }
}

TEST_F(StringFunctionLocateTest, locateInFallbackVolnitskyTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto haystack = BinaryColumn::create();
    auto needle = BinaryColumn::create();

    std::string haystack_data = "治世不一道，便国不法古。";
    std::string needle_const = "国";
    for (int i = 0; i < 100; ++i) {
        haystack->append(haystack_data);
    }
    needle->append(needle_const);
    columns.emplace_back(ConstColumn::create(std::move(needle), 1));
    columns.emplace_back(std::move(haystack));

    ColumnPtr result = StringFunctions::locate(ctx.get(), columns).value();
    ASSERT_EQ(100, result->size());

    auto v = ColumnHelper::cast_to<TYPE_INT>(result);

    for (int j = 0; j < 100; ++j) {
        ASSERT_EQ(8, v->get_data()[j]);
    }
}

TEST_F(StringFunctionLocateTest, locatePosChineseTest) {
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

    columns.emplace_back(std::move(sub));
    columns.emplace_back(std::move(str));
    columns.emplace_back(std::move(pos));

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

// Verify that the result NullableColumn does not share its NullColumn with any input column.
// When haystack is nullable (needle is const), res_null must be a clone of haystack's null column.
// When start_pos is nullable (needle is const), res_null must be a clone of start_pos's null column.
TEST_F(StringFunctionLocateTest, locateNullColumnNotSharedTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    // Case 1: nullable haystack, const needle → result's null_column must differ from haystack's
    {
        auto haystack_data = BinaryColumn::create();
        auto null_col = NullColumn::create();
        auto needle = BinaryColumn::create();
        auto start_pos = Int32Column::create();

        for (int i = 0; i < 20; ++i) {
            haystack_data->append("abcd");
            null_col->append(i % 2);
            start_pos->append(1);
        }
        needle->append("bc");

        Columns columns;
        columns.emplace_back(ConstColumn::create(std::move(needle), 1));
        columns.emplace_back(NullableColumn::create(std::move(haystack_data), std::move(null_col)));
        columns.emplace_back(std::move(start_pos));

        const NullableColumn* input_nullable = ColumnHelper::as_raw_column<NullableColumn>(columns[1]);

        ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
        ASSERT_TRUE(result->is_nullable());

        const NullableColumn* result_nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        ASSERT_NE(result_nullable->null_column().get(), input_nullable->null_column().get())
                << "result must not share null_column with haystack input";
    }

    // Case 2: non-nullable haystack, nullable start_pos, const needle → result's null_column must differ from start_pos's
    {
        auto haystack_data = BinaryColumn::create();
        auto start_pos_data = Int32Column::create();
        auto start_pos_null = NullColumn::create();
        auto needle = BinaryColumn::create();

        for (int i = 0; i < 20; ++i) {
            haystack_data->append("abcd");
            start_pos_data->append(1);
            start_pos_null->append(i % 2);
        }
        needle->append("bc");

        Columns columns;
        columns.emplace_back(ConstColumn::create(std::move(needle), 1));
        columns.emplace_back(std::move(haystack_data));
        columns.emplace_back(NullableColumn::create(std::move(start_pos_data), std::move(start_pos_null)));

        const NullableColumn* input_start_pos_nullable = ColumnHelper::as_raw_column<NullableColumn>(columns[2]);

        ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
        ASSERT_TRUE(result->is_nullable());

        const NullableColumn* result_nullable = ColumnHelper::as_raw_column<NullableColumn>(result);
        ASSERT_NE(result_nullable->null_column().get(), input_start_pos_nullable->null_column().get())
                << "result must not share null_column with start_pos input";
    }
}

TEST_F(StringFunctionLocateTest, LocateConstStartPosNullableAndUtf8) {
    // 1. Constant start_pos <= 0 on Nullable haystack
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto str = BinaryColumn::create();
        auto null = NullColumn::create();
        for (int i = 0; i < 32; ++i) {
            str->append(i % 3 == 0 ? "https://www.google.com.au" : "https://starrocks.io");
            null->append(i % 4 == 0 ? 1 : 0);
        }

        Columns columns;
        columns.emplace_back(ColumnHelper::create_const_column<TYPE_VARCHAR>("google", 32));
        columns.emplace_back(NullableColumn::create(std::move(str), std::move(null)));
        columns.emplace_back(ColumnHelper::create_const_column<TYPE_INT>(0, 32));

        ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
        ASSERT_EQ(32, result->size());
        ASSERT_TRUE(result->is_nullable());
        auto data_col =
                ColumnHelper::cast_to<TYPE_INT>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());
        for (int i = 0; i < 32; ++i) {
            if (i % 4 == 0) {
                EXPECT_TRUE(result->is_null(i));
            } else {
                EXPECT_FALSE(result->is_null(i));
                EXPECT_EQ(0, data_col->get_data()[i]);
            }
        }
    }

    // 2. Constant start_pos > 1 on multi-byte UTF-8 haystack with needle appearing both before and after start_pos
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto str = BinaryColumn::create();
        // Char positions: 1:'中', 2:'文', 3:'g', 4:'o', 5:'o', 6:'g', 7:'l', 8:'e', 9:'测', 10:'试', 11:'g', ...
        str->append("中文google测试google尾");
        str->append("中文google测试无后缀");

        Columns columns;
        columns.emplace_back(ColumnHelper::create_const_column<TYPE_VARCHAR>("google", 2));
        columns.emplace_back(std::move(str));
        columns.emplace_back(ColumnHelper::create_const_column<TYPE_INT>(4, 2));

        ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
        auto data_col = ColumnHelper::cast_to<TYPE_INT>(result);
        ASSERT_EQ(2, data_col->size());
        EXPECT_EQ(11, data_col->get_data()[0]);
        EXPECT_EQ(0, data_col->get_data()[1]);
    }

    // 3. Constant start_pos wrapped in a NullableColumn (both non-null and null)
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto str = BinaryColumn::create();
        str->append("中文google测试google尾");
        str->append("中文google测试无后缀");

        auto pos_data = Int32Column::create();
        pos_data->append(4);
        auto pos_null = NullColumn::create(1, 0);
        auto nullable_const_pos =
                ConstColumn::create(NullableColumn::create(std::move(pos_data), std::move(pos_null)), 2);

        Columns columns;
        columns.emplace_back(ColumnHelper::create_const_column<TYPE_VARCHAR>("google", 2));
        columns.emplace_back(std::move(str));
        columns.emplace_back(std::move(nullable_const_pos));

        ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
        auto data_col = ColumnHelper::cast_to<TYPE_INT>(result);
        ASSERT_EQ(2, data_col->size());
        EXPECT_EQ(11, data_col->get_data()[0]);
        EXPECT_EQ(0, data_col->get_data()[1]);
    }

    // 4. Constant start_pos <= 0 on non-nullable haystack
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto str = BinaryColumn::create();
        str->append("https://www.google.com.au");
        str->append("https://starrocks.io");

        Columns columns;
        columns.emplace_back(ColumnHelper::create_const_column<TYPE_VARCHAR>("google", 2));
        columns.emplace_back(std::move(str));
        columns.emplace_back(ColumnHelper::create_const_column<TYPE_INT>(-1, 2));

        ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
        ASSERT_FALSE(result->is_nullable());
        auto data_col = ColumnHelper::cast_to<TYPE_INT>(result);
        ASSERT_EQ(2, data_col->size());
        EXPECT_EQ(0, data_col->get_data()[0]);
        EXPECT_EQ(0, data_col->get_data()[1]);
    }

    // 5. Nullable haystack and nullable non-constant start_pos with constant needle
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto str = BinaryColumn::create();
        auto str_null = NullColumn::create();
        auto pos = Int32Column::create();
        auto pos_null = NullColumn::create();

        str->append("xxgoogle");
        str_null->append(0);
        pos->append(1);
        pos_null->append(0);

        str->append("xxgoogle");
        str_null->append(1);
        pos->append(1);
        pos_null->append(0);

        str->append("xxgoogle");
        str_null->append(0);
        pos->append(1);
        pos_null->append(1);

        Columns columns;
        columns.emplace_back(ColumnHelper::create_const_column<TYPE_VARCHAR>("google", 3));
        columns.emplace_back(NullableColumn::create(std::move(str), std::move(str_null)));
        columns.emplace_back(NullableColumn::create(std::move(pos), std::move(pos_null)));

        ColumnPtr result = StringFunctions::locate_pos(ctx.get(), columns).value();
        ASSERT_TRUE(result->is_nullable());
        EXPECT_FALSE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        auto data_col =
                ColumnHelper::cast_to<TYPE_INT>(ColumnHelper::as_raw_column<NullableColumn>(result)->data_column());
        EXPECT_EQ(3, data_col->get_data()[0]);
    }

    // 6. Long homogeneous constant needle (100,000 'a' bytes) across a short chunk and a matching chunk
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        const std::string long_needle(100000, 'a');

        auto short_str = BinaryColumn::create();
        short_str->append("short_row_1");
        short_str->append("short_row_2");
        Columns short_cols;
        short_cols.emplace_back(ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(long_needle), 2));
        short_cols.emplace_back(std::move(short_str));
        ColumnPtr short_res = StringFunctions::locate(ctx.get(), short_cols).value();
        auto short_data = ColumnHelper::cast_to<TYPE_INT>(short_res);
        ASSERT_EQ(2, short_data->size());
        EXPECT_EQ(0, short_data->get_data()[0]);
        EXPECT_EQ(0, short_data->get_data()[1]);

        auto hit_str = BinaryColumn::create();
        hit_str->append(std::string(100050, 'b'));
        hit_str->append("中文" + long_needle + "尾");
        Columns hit_cols;
        hit_cols.emplace_back(ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(long_needle), 2));
        hit_cols.emplace_back(std::move(hit_str));
        ColumnPtr hit_res = StringFunctions::locate(ctx.get(), hit_cols).value();
        auto hit_data = ColumnHelper::cast_to<TYPE_INT>(hit_res);
        ASSERT_EQ(2, hit_data->size());
        EXPECT_EQ(0, hit_data->get_data()[0]);
        EXPECT_EQ(3, hit_data->get_data()[1]);
    }
}

} // namespace starrocks
