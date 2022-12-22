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

class StringFunctionConcatTest : public ::testing::Test {
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

TEST_F(StringFunctionConcatTest, concatNormalTest) {
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

TEST_F(StringFunctionConcatTest, concatConstTest) {
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

    auto state = std::make_unique<ConcatState>();
    for (int i = 1; i < columns.size(); ++i) {
        state->tail.append(ColumnHelper::get_const_value<TYPE_VARCHAR>(columns[i]).to_string());
    }
    state->is_const = true;
    state->is_oversize = false;
    ctx->set_function_state(FunctionContext::FRAGMENT_LOCAL, state.get());
    ColumnPtr result = StringFunctions::concat(ctx.get(), columns).value();

    ASSERT_TRUE(result->is_binary());
    ASSERT_FALSE(result->is_nullable());

    auto v = ColumnHelper::as_column<BinaryColumn>(result);
    for (int k = 0; k < 20; ++k) {
        ASSERT_EQ("test" + std::to_string(k) + "_abcd_1234_道可道,非常道", v->get_data()[k].to_string());
    }
}

TEST_F(StringFunctionConcatTest, concatNullTest) {
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

TEST_F(StringFunctionConcatTest, concatWsTest) {
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

TEST_F(StringFunctionConcatTest, concatWs1Test) {
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
TEST_F(StringFunctionConcatTest, concatConstOversizeTest) {
    auto col0 = BinaryColumn::create();
    auto col1 = BinaryColumn::create();
    auto col2 = BinaryColumn::create();
    auto col3 = BinaryColumn::create();
    auto null_col = NullColumn::create();
    for (int i = 0; i < 10; ++i) {
        col0->append(Slice(std::string(OLAP_STRING_MAX_LENGTH - i, 'x')));
        col0->append(Slice(std::string(i + 1, 'y')));
    }
    col1->append(Slice(std::string(33, 'A')));
    col2->append(Slice(std::string(33, 'B')));
    col3->append(Slice(std::string(33, 'C')));
    const auto row_num = col0->size();
    for (auto i = 0; i < row_num; ++i) {
        if (i % 3 == 0) {
            null_col->append(1);
        } else {
            null_col->append(0);
        }
    }
    Columns columns;
    columns.emplace_back(NullableColumn::create(col0, null_col));
    columns.emplace_back(ConstColumn::create(col1, 1));
    columns.emplace_back(ConstColumn::create(col2, 1));
    columns.emplace_back(ConstColumn::create(col3, 1));
    std::shared_ptr<FunctionContext> context(FunctionContext::create_test_context());

    auto state = std::make_unique<ConcatState>();
    for (int i = 1; i < columns.size(); ++i) {
        state->tail.append(ColumnHelper::get_const_value<TYPE_VARCHAR>(columns[i]).to_string());
    }
    state->is_const = true;
    state->is_oversize = false;
    context->set_function_state(FunctionContext::FRAGMENT_LOCAL, state.get());

    auto result = StringFunctions::concat(context.get(), columns).value();

    ASSERT_TRUE(result->is_nullable());
    auto result_nullable = down_cast<NullableColumn*>(result.get());
    ASSERT_TRUE(result_nullable != nullptr);
    auto result_binary = down_cast<BinaryColumn*>(result_nullable->data_column().get());
    ASSERT_TRUE(result_binary != nullptr);
    ASSERT_EQ(result_binary->size(), col0->size());
    ASSERT_EQ(result_binary->size(), row_num);
    for (auto i = 0; i < row_num; ++i) {
        if (i % 3 == 0) {
            ASSERT_EQ(null_col->get_data()[i], 1);
            ASSERT_TRUE(columns[0]->is_null(i));
            ASSERT_TRUE(result->is_null(i));
            continue;
        }

        auto s0 = col0->get_slice(i);
        auto s = result_binary->get_slice(i);
        std::string tail =
                col1->get_slice(0).to_string() + col2->get_slice(0).to_string() + col3->get_slice(0).to_string();
        if (s0.size + 99 > OLAP_STRING_MAX_LENGTH) {
            ASSERT_TRUE(result->is_null(i));
        } else {
            ASSERT_FALSE(result->is_null(i));
            ASSERT_EQ(s0.size + 99, s.size);
            ASSERT_EQ(s0.to_string() + tail, s.to_string());
        }
    }
}

static inline void concat_not_const_test(const NullColumnPtr& null_col, Columns const& columns, size_t col_idx,
                                         const size_t limit) {
    std::shared_ptr<FunctionContext> context(FunctionContext::create_test_context());
    auto result = StringFunctions::concat(context.get(), columns).value();

    ASSERT_TRUE(result->is_nullable());
    auto result_nullable = down_cast<NullableColumn*>(result.get());
    ASSERT_TRUE(result_nullable != nullptr);
    auto result_binary = down_cast<BinaryColumn*>(result_nullable->data_column().get());
    ASSERT_TRUE(result_binary != nullptr);
    const auto row_num = columns[0]->size();
    ASSERT_EQ(result_binary->size(), columns[0]->size());
    ASSERT_EQ(result_binary->size(), row_num);
    auto col = ColumnHelper::get_binary_column(columns[col_idx].get());
    auto init_row = row_num > limit ? row_num - limit : size_t(0);
    for (auto i = init_row; i < row_num; ++i) {
        if (i % 3 == 0) {
            ASSERT_EQ(null_col->get_data()[i], 1);
            ASSERT_TRUE(result->is_null(i));
            continue;
        }
        auto s0 = col->get_slice(i);
        auto s = result_binary->get_slice(i);
        if (s0.size + 99 > OLAP_STRING_MAX_LENGTH) {
            ASSERT_TRUE(result->is_null(i));
        } else {
            ASSERT_FALSE(result->is_null(i));
            ASSERT_EQ(s0.size + 99, s.size);
            std::string expect;
            expect.reserve(s0.size + 99);
            for (auto k = 0; k < columns.size(); ++k) {
                if (k == col_idx) {
                    expect.append(s0.to_string());
                } else {
                    auto col_binary = ColumnHelper::get_binary_column(columns[k].get());
                    expect.append(col_binary->get_slice(i).to_string());
                }
            }
            ASSERT_EQ(expect, s.to_string());
        }
    }
}

static inline void concat_not_const_test(const NullColumnPtr& null_col, const ColumnPtr& col0, const ColumnPtr& col1,
                                         const ColumnPtr& col2, const ColumnPtr& col3, const size_t limit) {
    concat_not_const_test(null_col, {NullableColumn::create(col0, null_col), col1, col2, col3}, 0, limit);
    /*
    concat_not_const_test(null_col, {col1, NullableColumn::create(col0, null_col), col2, col3}, 1,
                          limit);
    concat_not_const_test(null_col, {col1, col2, NullableColumn::create(col0, null_col), col3}, 2,
                          limit);
    concat_not_const_test(null_col, {col1, col2, col3, NullableColumn::create(col0, null_col)}, 3,
                          limit);
    */
    concat_not_const_test(null_col, {col0, NullableColumn::create(col1, null_col), col2, col3}, 0, limit);
    /*
    concat_not_const_test(null_col, {col0, col1, NullableColumn::create(col2, null_col), col3}, 0,
                          limit);
    concat_not_const_test(null_col, {col0, col1, col2, NullableColumn::create(col3, null_col)}, 0,
                          limit);
    */
    concat_not_const_test(null_col, {NullableColumn::create(col1, null_col), col0, col2, col3}, 1, limit);
    /*
    concat_not_const_test(null_col, {col1, NullableColumn::create(col2, null_col), col0, col3}, 2,
                          limit);
    concat_not_const_test(null_col, {col1, col2, NullableColumn::create(col3, null_col), col0}, 3,
                          limit);
    */
}
TEST_F(StringFunctionConcatTest, concatNotConstSmallOversizeTest) {
    auto col0 = BinaryColumn::create();
    auto col1 = BinaryColumn::create();
    auto col2 = BinaryColumn::create();
    auto col3 = BinaryColumn::create();
    auto null_col = NullColumn::create();
    for (int i = 0; i < 5; ++i) {
        col0->append(Slice(std::string(OLAP_STRING_MAX_LENGTH - i, 'x')));
        col0->append(Slice(std::string(i + 1, 'y')));
    }
    const auto row_num = col0->size();
    for (int i = 0; i < row_num; ++i) {
        col1->append(Slice(std::string(33, 'A')));
        col2->append(Slice(std::string(33, 'B')));
        col3->append(Slice(std::string(33, 'C')));
    }

    for (auto i = 0; i < row_num; ++i) {
        if (i % 3 == 0) {
            null_col->append(1);
        } else {
            null_col->append(0);
        }
    }
    concat_not_const_test(null_col, col0, col1, col2, col3, 10);
}

TEST_F(StringFunctionConcatTest, concatNotConstBigOversizeTest) {
    auto col0 = BinaryColumn::create();
    auto col1 = BinaryColumn::create();
    auto col2 = BinaryColumn::create();
    auto col3 = BinaryColumn::create();
    auto null_col = NullColumn::create();

    for (int i = 0; i < 10; ++i) {
        col0->append(Slice(std::string(OLAP_STRING_MAX_LENGTH - i, 'x')));
        col0->append(Slice(std::string(i + 1, 'x')));
    }

    const auto row_num = col0->size();
    for (int i = 0; i < row_num; ++i) {
        col1->append(Slice(std::string(33, 'A')));
        col2->append(Slice(std::string(33, 'B')));
        col3->append(Slice(std::string(33, 'C')));
    }

    for (auto i = 0; i < row_num; ++i) {
        if (i % 3 == 0) {
            null_col->append(1);
        } else {
            null_col->append(0);
        }
    }
    concat_not_const_test(null_col, col0, col1, col2, col3, 0);
}

static inline void concat_ws_test(const NullColumnPtr& sep_null_col, const NullColumnPtr& null_col,
                                  const ColumnPtr& sep, Columns const& columns, size_t col_idx, const size_t limit) {
    std::shared_ptr<FunctionContext> context(FunctionContext::create_test_context());
    auto result = StringFunctions::concat_ws(context.get(), columns).value();
    auto union_null_col = FunctionHelper::union_null_column(sep_null_col, null_col);

    ASSERT_TRUE(result->is_nullable());
    auto result_nullable = down_cast<NullableColumn*>(result.get());
    ASSERT_TRUE(result_nullable != nullptr);
    auto result_binary = down_cast<BinaryColumn*>(result_nullable->data_column().get());
    ASSERT_TRUE(result_binary != nullptr);
    const auto row_num = columns[0]->size();
    ASSERT_EQ(result_binary->size(), columns[0]->size());
    ASSERT_EQ(result_binary->size(), row_num);
    auto init_row = row_num > limit ? row_num - limit : size_t(0);
    for (auto i = init_row; i < row_num; ++i) {
        if (i % 3 == 0) {
            ASSERT_EQ(union_null_col->get_data()[i], 1);
            ASSERT_TRUE(result->is_null(i));
            continue;
        }

        auto s = result_binary->get_slice(i);
        std::string expect;
        std::string sep_str = ColumnHelper::get_binary_column(sep.get())->get_slice(i).to_string();
        for (auto k = 1; k < columns.size(); ++k) {
            if (columns[k]->is_null(i)) {
                continue;
            }
            auto binary_column = ColumnHelper::get_binary_column(columns[k].get());
            expect.append(binary_column->get_slice(i).to_string());
            expect.append(sep_str);
        }
        if (expect.size() > 0) {
            expect.resize(expect.size() - sep_str.size());
        }

        if (expect.size() > OLAP_STRING_MAX_LENGTH) {
            ASSERT_TRUE(result->is_null(i));
        } else {
            ASSERT_FALSE(result->is_null(i));
            ASSERT_EQ(expect, s.to_string());
        }
    }
}
static inline void concat_ws_test(const NullColumnPtr& sep_null_col, const NullColumnPtr& null_col,
                                  const ColumnPtr& sep_col, const ColumnPtr& col0, const ColumnPtr& col1,
                                  const ColumnPtr& col2, const ColumnPtr& col3, const size_t limit) {
    auto nullable_sep_col = NullableColumn::create(sep_col, sep_null_col);
    concat_ws_test(sep_null_col, null_col, sep_col,
                   {nullable_sep_col, NullableColumn::create(col0, null_col), col1, col2, col3}, 1, limit);
    /*
    concat_ws_test(sep_null_col, null_col, sep_col,
                   {nullable_sep_col, col1, NullableColumn::create(col0, null_col), col2, col3}, 2,
                   limit);
    concat_ws_test(sep_null_col, null_col, sep_col,
                   {nullable_sep_col, col1, col2, NullableColumn::create(col0, null_col), col3}, 3,
                   limit);
    */
    concat_ws_test(sep_null_col, null_col, sep_col,
                   {nullable_sep_col, col1, col2, col3, NullableColumn::create(col0, null_col)}, 4, limit);

    concat_ws_test(sep_null_col, null_col, sep_col,
                   {nullable_sep_col, col0, NullableColumn::create(col1, null_col), col2, col3}, 1, limit);
    /*
    concat_ws_test(sep_null_col, null_col, sep_col,
                   {nullable_sep_col, col0, col1, NullableColumn::create(col2, null_col), col3}, 1,
                   limit);
    concat_ws_test(sep_null_col, null_col, sep_col,
                   {nullable_sep_col, col0, col1, col2, NullableColumn::create(col3, null_col)}, 1,
                   limit);
    */
    concat_ws_test(sep_null_col, null_col, sep_col,
                   {nullable_sep_col, NullableColumn::create(col1, null_col), col0, col2, col3}, 2, limit);
    /*
    concat_ws_test(sep_null_col, null_col, sep_col,
                   {nullable_sep_col, col1, NullableColumn::create(col2, null_col), col0, col3}, 3,
                   limit);
    concat_ws_test(sep_null_col, null_col, sep_col,
                   {nullable_sep_col, col1, col2, NullableColumn::create(col3, null_col), col0}, 4,
                   limit);
    */
}
void prepare_concat_ws_data(const NullColumnPtr& sep_null_col, const NullColumnPtr& null_col, const ColumnPtr& sep,
                            const ColumnPtr& col0, const ColumnPtr& col1, const ColumnPtr& col2,
                            const ColumnPtr& col3) {
    for (auto i = 0; i < 5; ++i) {
        sep->append_datum(Slice(""));
        sep->append_datum(Slice("x"));
    }
    for (auto i = 0; i < 5; ++i) {
        col0->append_datum(Slice(""));
        col1->append_datum(Slice(""));
        col2->append_datum(Slice(""));
        col3->append_datum(Slice(""));
        col0->append_datum(Slice("a"));
        col1->append_datum(Slice("b"));
        col2->append_datum(Slice("c"));
        col3->append_datum(Slice("d"));
    }

    for (auto i = 0; i < 5; ++i) {
        sep->append_datum(Slice(""));
        sep->append_datum(Slice(std::string(OLAP_STRING_MAX_LENGTH, 'x')));
        sep->append_datum(Slice(std::string(i + 1, 'x')));
        sep->append_datum(Slice(std::string(5 - i, 'x')));
    }

    for (auto i = 0; i < 10; ++i) {
        col0->append_datum(Slice(std::string(OLAP_STRING_MAX_LENGTH - i, 'x')));
        col0->append_datum(Slice(std::string(i + 1, 'x')));
    }

    for (auto i = 0; i < 20; ++i) {
        col1->append_datum(Slice(std::string(i % 3 + 1, 'A')));
        col2->append_datum(Slice(std::string((i + 10) % 3 + 1, 'B')));
        col3->append_datum(Slice(std::string((i + 20) % 3 + 1, 'C')));
    }

    for (auto i = 0; i < col1->size(); ++i) {
        if (i % 3 == 0) {
            sep_null_col->append(1);
        } else {
            sep_null_col->append(0);
        }
        if (i % 11 == 0) {
            null_col->append(1);
        } else {
            null_col->append(0);
        }
    }
}

TEST_F(StringFunctionConcatTest, concatWsSmallOversizeTest) {
    auto sep = BinaryColumn::create();
    auto col0 = BinaryColumn::create();
    auto col1 = BinaryColumn::create();
    auto col2 = BinaryColumn::create();
    auto col3 = BinaryColumn::create();
    auto sep_null_col = NullColumn::create();
    auto null_col = NullColumn::create();
    prepare_concat_ws_data(sep_null_col, null_col, sep, col0, col1, col2, col3);
    concat_ws_test(sep_null_col, null_col, sep, col0, col1, col2, col3, 300);
}

TEST_F(StringFunctionConcatTest, concatWsBigOversizeTest) {
    auto sep = BinaryColumn::create();
    auto col0 = BinaryColumn::create();
    auto col1 = BinaryColumn::create();
    auto col2 = BinaryColumn::create();
    auto col3 = BinaryColumn::create();
    auto sep_null_col = NullColumn::create();
    auto null_col = NullColumn::create();
    for (int i = 3786; i < 3796; ++i) {
        sep->append(Slice(std::string(OLAP_STRING_MAX_LENGTH - i, 'x')));
        col0->append(Slice(std::string(100, 'y')));
        col1->append(Slice(std::string(i + 1, 'z')));
        col2->append(Slice(std::string(4096 - i, 'w')));
        col3->append(Slice(std::string(3796 - i, 'v')));
    }
    prepare_concat_ws_data(sep_null_col, null_col, sep, col0, col1, col2, col3);
    concat_ws_test(sep_null_col, null_col, sep, col0, col1, col2, col3, 30);
}
} // namespace starrocks
