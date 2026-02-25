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

} // namespace starrocks
