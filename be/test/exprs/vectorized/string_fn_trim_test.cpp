// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <random>

#include "butil/time.h"
#include "exprs/vectorized/mock_vectorized_expr.h"
#include "exprs/vectorized/string_functions.h"

namespace starrocks {
namespace vectorized {

class StringFunctionTrimTest : public ::testing::Test {
public:
    void SetUp() {
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

    ColumnPtr result = StringFunctions::trim(ctx.get(), columns);
    ASSERT_EQ(4096, result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int k = 0; k < 4096; ++k) {
        ASSERT_EQ("abcd" + std::to_string(k), v->get_data()[k].to_string());
    }
}

TEST_F(StringFunctionTrimTest, trimOrphanEmptyStringTest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    str->append(Slice((const char*)nullptr, 0));

    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::trim(ctx.get(), columns);
    ASSERT_EQ(1, result->size());
    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
    ASSERT_EQ(v->get_slice(0).size, 0);

    result = StringFunctions::rtrim(ctx.get(), columns);
    ASSERT_EQ(1, result->size());
    v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
    ASSERT_EQ(v->get_slice(0).size, 0);

    result = StringFunctions::ltrim(ctx.get(), columns);
    ASSERT_EQ(1, result->size());
    v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
    ASSERT_EQ(v->get_slice(0).size, 0);
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

    ColumnPtr result = StringFunctions::ltrim(ctx.get(), columns);
    ASSERT_EQ(4096, result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int k = 0; k < 4096; ++k) {
        std::string spaces(k, ' ');
        ASSERT_EQ("abcd" + std::to_string(k) + spaces, v->get_data()[k].to_string());
    }
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

    ColumnPtr result = StringFunctions::rtrim(ctx.get(), columns);
    ASSERT_EQ(4096, result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int k = 0; k < 4096; ++k) {
        std::string spaces(k, ' ');
        ASSERT_EQ(spaces + "abcd" + std::to_string(k), v->get_data()[k].to_string());
    }
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

    ColumnPtr rtrim_result = StringFunctions::rtrim(ctx.get(), columns);
    ColumnPtr ltrim_result = StringFunctions::ltrim(ctx.get(), columns);
    ColumnPtr trim_result = StringFunctions::trim(ctx.get(), columns);
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
}
} // namespace vectorized
} // namespace starrocks
