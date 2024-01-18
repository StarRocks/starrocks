// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <random>

#include "butil/time.h"
#include "exprs/vectorized/mock_vectorized_expr.h"
#include "exprs/vectorized/string_functions.h"
#include "testutil/assert.h"
#include "udf/udf.h"

namespace starrocks::vectorized {

class StringFunctionTrimTest : public ::testing::Test {
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
TEST_F(StringFunctionTrimTest, trimTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    for (int j = 0; j < 4096; ++j) {
        std::string spaces(j, ' ');
        str->append(spaces + "abcd" + std::to_string(j) + spaces);
    }

    columns.emplace_back(str);

    ctx->set_constant_columns(columns);
    ASSERT_OK(StringFunctions::trim_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL));
    ColumnPtr result = StringFunctions::trim(ctx.get(), columns).value();
    ASSERT_EQ(4096, result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int k = 0; k < 4096; ++k) {
        ASSERT_EQ("abcd" + std::to_string(k), v->get_data()[k].to_string());
    }
    ASSERT_OK(StringFunctions::trim_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL));
}

TEST_F(StringFunctionTrimTest, trimCharTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    // string column
    auto str_col = BinaryColumn::create();
    for (int j = 0; j < 4096; ++j) {
        std::string spaces(j, ' ');
        str_col->append(spaces + "abcd" + std::to_string(j) + spaces);
    }

    // remove character column
    auto remove_col = ColumnHelper::create_const_column<TYPE_VARCHAR>(" ab", 4096);

    std::vector<ColumnPtr> columns{str_col, remove_col};
    ctx->set_constant_columns(columns);
    ASSERT_OK(StringFunctions::trim_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL));
    ColumnPtr result = StringFunctions::trim(ctx.get(), columns).value();
    ASSERT_EQ(4096, result->size());
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int k = 0; k < 4096; ++k) {
        ASSERT_EQ("cd" + std::to_string(k), v->get_data()[k].to_string());
    }

    ASSERT_OK(StringFunctions::trim_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL));
    {
        // Trim utf-8 characters
        for (int i = 0; i < 10; i++) {
            auto remove_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("🙂🐶e", 1);
            auto str_col = BinaryColumn::create();
            std::string str = "abc🐱🐷";
            for (int j = 0; j < i; j++) {
                str += "e🙂🐶";
                str = "🙂🐶e" + str;
            }
            str_col->append(str);
            Columns columns{str_col, remove_col};
            ctx->set_constant_columns(columns);
            ASSERT_OK(StringFunctions::trim_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL));
            auto maybe_result = StringFunctions::trim(ctx.get(), columns).value();
            Slice result = *ColumnHelper::get_cpp_data<TYPE_VARCHAR>(maybe_result);
            ASSERT_EQ("abc🐱🐷", std::string(result));
            ASSERT_OK(StringFunctions::trim_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL));
        }
    }

    {
        // The 2rd parameter must be const
        auto remove_col = BinaryColumn::create();
        remove_col->append("ab");
        ctx->set_constant_columns({nullptr, remove_col});
        Status st = StringFunctions::trim_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
        EXPECT_TRUE(st.is_invalid_argument());
        EXPECT_EQ("Invalid argument: The second parameter of trim only accept literal value", st.to_string());
    }

    {
        // The 2rd parameter must not be empty
        auto remove_col = ColumnHelper::create_const_column<TYPE_VARCHAR>("", 4096);
        ctx->set_constant_columns({nullptr, remove_col});
        Status st = StringFunctions::trim_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
        EXPECT_TRUE(st.is_invalid_argument());
        EXPECT_EQ("Invalid argument: The second parameter should not be empty string", st.to_string());
    }
    {
        // The 2rd parameter must not be null
        auto remove_col = ColumnHelper::create_const_null_column(4096);
        ctx->set_constant_columns({nullptr, remove_col});
        Status st = StringFunctions::trim_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
        EXPECT_TRUE(st.is_invalid_argument());
        EXPECT_EQ("Invalid argument: The second parameter should not be null", st.to_string());
    }
}

TEST_F(StringFunctionTrimTest, trimOrphanEmptyStringTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    str->append(Slice((const char*)nullptr, 0));

    columns.emplace_back(str);

    ctx->set_constant_columns(columns);
    ASSERT_OK(StringFunctions::trim_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL));
    ColumnPtr result = StringFunctions::trim(ctx.get(), columns).value();
    ASSERT_EQ(1, result->size());
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
    ASSERT_EQ(v->get_slice(0).size, 0);

    result = StringFunctions::rtrim(ctx.get(), columns).value();
    ASSERT_EQ(1, result->size());
    v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
    ASSERT_EQ(v->get_slice(0).size, 0);

    result = StringFunctions::ltrim(ctx.get(), columns).value();
    ASSERT_EQ(1, result->size());
    v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
    ASSERT_EQ(v->get_slice(0).size, 0);
    ASSERT_OK(StringFunctions::trim_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL));
}

TEST_F(StringFunctionTrimTest, ltrimTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    for (int j = 0; j < 4096; ++j) {
        std::string spaces(j, ' ');
        str->append(spaces + "abcd" + std::to_string(j) + spaces);
    }

    columns.emplace_back(str);

    ctx->set_constant_columns(columns);
    ASSERT_OK(StringFunctions::trim_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL));
    ColumnPtr result = StringFunctions::ltrim(ctx.get(), columns).value();
    ASSERT_EQ(4096, result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int k = 0; k < 4096; ++k) {
        std::string spaces(k, ' ');
        ASSERT_EQ("abcd" + std::to_string(k) + spaces, v->get_data()[k].to_string());
    }
    ASSERT_OK(StringFunctions::trim_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL));
}

TEST_F(StringFunctionTrimTest, rtrimTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    for (int j = 0; j < 4096; ++j) {
        std::string spaces(j, ' ');
        str->append(spaces + "abcd" + std::to_string(j) + spaces);
    }

    columns.emplace_back(str);

    ctx->set_constant_columns(columns);
    ASSERT_OK(StringFunctions::trim_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL));
    ColumnPtr result = StringFunctions::rtrim(ctx.get(), columns).value();
    ASSERT_EQ(4096, result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int k = 0; k < 4096; ++k) {
        std::string spaces(k, ' ');
        ASSERT_EQ(spaces + "abcd" + std::to_string(k), v->get_data()[k].to_string());
    }
    ASSERT_OK(StringFunctions::trim_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL));
}

TEST_F(StringFunctionTrimTest, trimSpacesTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    auto nulls = NullColumn::create();
    for (int j = 0; j < 4096; ++j) {
        std::string spaces(j, ' ');
        str->append(spaces);
        if (j % 3 == 0) {
            nulls->append(1);
        } else {
            nulls->append(0);
        }
    }

    columns.emplace_back(NullableColumn::create(str, nulls));

    ctx->set_constant_columns(columns);
    ASSERT_OK(StringFunctions::trim_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL));
    ColumnPtr rtrim_result = StringFunctions::rtrim(ctx.get(), columns).value();
    ColumnPtr ltrim_result = StringFunctions::ltrim(ctx.get(), columns).value();
    ColumnPtr trim_result = StringFunctions::trim(ctx.get(), columns).value();
    ASSERT_EQ(4096, rtrim_result->size());
    ASSERT_EQ(4096, ltrim_result->size());
    ASSERT_EQ(4096, trim_result->size());

    for (int k = 0; k < 4096; ++k) {
        if (k % 3 == 0) {
            ASSERT_TRUE(rtrim_result->is_null(k));
            ASSERT_TRUE(ltrim_result->is_null(k));
            ASSERT_TRUE(trim_result->is_null(k));
        } else {
            ASSERT_FALSE(rtrim_result->is_null(k));
            ASSERT_FALSE(ltrim_result->is_null(k));
            ASSERT_FALSE(trim_result->is_null(k));
            ASSERT_FALSE(rtrim_result->get(k).is_null());
            ASSERT_FALSE(ltrim_result->get(k).is_null());
            ASSERT_FALSE(trim_result->get(k).is_null());
            ASSERT_EQ(std::string(), ltrim_result->get(k).get_slice().to_string());
            ASSERT_EQ(std::string(), rtrim_result->get(k).get_slice().to_string());
            ASSERT_EQ(std::string(), trim_result->get(k).get_slice().to_string());
        }
    }
    ASSERT_OK(StringFunctions::trim_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL));
}
} // namespace starrocks::vectorized
