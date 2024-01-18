// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <random>

#include "butil/time.h"
#include "exprs/vectorized/mock_vectorized_expr.h"
#include "exprs/vectorized/string_functions.h"

namespace starrocks::vectorized {

class StringFunctionReverseTest : public ::testing::Test {
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
TEST_F(StringFunctionReverseTest, reverseASCIITest) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    std::string s = "abcd_efg_higk_lmn" + std::string(100, 'x');
    for (int j = 0; j < 100; ++j) {
        str->append(s.substr(0, j));
    }

    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::reverse(ctx.get(), columns).value();
    ASSERT_EQ(100, result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int k = 0; k < 100; ++k) {
        auto tmp_s = str->get_slice(k).to_string();
        std::string expect(tmp_s.rbegin(), tmp_s.rend());
        ASSERT_EQ(expect, v->get_data()[k].to_string());
    }
}

TEST_F(StringFunctionReverseTest, reverseUtf8Test) {
    std::vector<std::tuple<std::string, std::string>> cases = {
            {"道", "道"},
            {"", ""},
            {"a", "a"},
            {"abcd博学笃志efg切问静思", "思静问切gfe志笃学博dcba"},
            {"三十年终生牛马，六十年诸佛龙象", "象龙佛诸年十六，马牛生终年十三"}};

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto str = BinaryColumn::create();
    for (auto& c : cases) {
        str->append(std::get<0>(c));
    }

    columns.emplace_back(str);

    ColumnPtr result = StringFunctions::reverse(ctx.get(), columns).value();
    ASSERT_EQ(str->size(), result->size());

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (auto i = 0; i < str->size(); ++i) {
        auto expect = std::get<1>(cases[i]);
        ASSERT_EQ(expect, v->get_data()[i].to_string());
    }
}

} // namespace starrocks::vectorized
