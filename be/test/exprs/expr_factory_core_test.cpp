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

#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_factory.h"
#include "runtime/descriptors.h"

namespace starrocks {

namespace {

TExprNode make_int_literal_node(int64_t value) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::INT_LITERAL);
    node.__set_num_children(0);
    node.__set_is_nullable(false);
    node.__set_type(gen_type_desc(TPrimitiveType::INT));

    TIntLiteral int_literal;
    int_literal.__set_value(value);
    node.__set_int_literal(int_literal);
    return node;
}

TExprNode make_cast_node(TPrimitiveType::type from_type, TPrimitiveType::type to_type) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::CAST_EXPR);
    node.__set_opcode(TExprOpcode::CAST);
    node.__set_num_children(1);
    node.__set_is_nullable(true);
    node.__set_type(gen_type_desc(to_type));
    node.__set_child_type(from_type);
    return node;
}

TExprNode make_function_call_node(std::string function_name, int num_children, TPrimitiveType::type return_type) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    node.__set_num_children(num_children);
    node.__set_is_nullable(true);
    node.__set_type(gen_type_desc(return_type));
    node.__set_child_type(TPrimitiveType::INT);

    TFunctionName fn_name;
    fn_name.__set_db_name("db");
    fn_name.__set_function_name(std::move(function_name));

    TFunction fn;
    fn.__set_name(fn_name);
    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
    fn.__set_arg_types({});
    fn.__set_has_var_args(false);
    fn.__set_fid(0);
    node.__set_fn(fn);

    return node;
}

} // namespace

TEST(ExprFactoryCoreTest, create_expr_tree_succeeds_for_literal_tree) {
    ObjectPool pool;
    TExpr texpr;
    texpr.nodes.emplace_back(make_int_literal_node(7));

    Expr* root = nullptr;
    Status status = ExprFactory::create_expr_tree(&pool, texpr, &root, nullptr);
    ASSERT_TRUE(status.ok()) << status.message();
    ASSERT_NE(nullptr, root);
    EXPECT_TRUE(root->is_literal());
}

TEST(ExprFactoryCoreTest, create_expr_tree_succeeds_for_core_cast) {
    ObjectPool pool;
    TExpr texpr;
    texpr.nodes.emplace_back(make_cast_node(TPrimitiveType::INT, TPrimitiveType::BIGINT));
    texpr.nodes.emplace_back(make_int_literal_node(1));

    Expr* root = nullptr;
    Status status = ExprFactory::create_expr_tree(&pool, texpr, &root, nullptr);
    ASSERT_TRUE(status.ok()) << status.message();
    ASSERT_NE(nullptr, root);
    EXPECT_TRUE(root->is_cast_expr());
    EXPECT_EQ(1, root->get_num_children());
}

TEST(ExprFactoryCoreTest, create_expr_tree_succeeds_for_core_condition_function) {
    ObjectPool pool;
    TExpr texpr;
    texpr.nodes.emplace_back(make_function_call_node("ifnull", 2, TPrimitiveType::INT));
    texpr.nodes.emplace_back(make_int_literal_node(1));
    texpr.nodes.emplace_back(make_int_literal_node(2));

    Expr* root = nullptr;
    Status status = ExprFactory::create_expr_tree(&pool, texpr, &root, nullptr);
    ASSERT_TRUE(status.ok()) << status.message();
    ASSERT_NE(nullptr, root);
    EXPECT_EQ(2, root->get_num_children());
}

TEST(ExprFactoryCoreTest, non_core_function_returns_unsupported_without_extension) {
    ObjectPool pool;
    TExpr texpr;
    texpr.nodes.emplace_back(make_function_call_node("pi", 0, TPrimitiveType::DOUBLE));

    Expr* root = nullptr;
    Status status = ExprFactory::create_expr_tree(&pool, texpr, &root, nullptr);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(nullptr, root);
}

TEST(ExprFactoryCoreTest, create_expr_tree_with_jit_flag_is_safe_without_hook) {
    ObjectPool pool;
    TExpr texpr;
    texpr.nodes.emplace_back(make_int_literal_node(8));

    Expr* root = nullptr;
    Status status = ExprFactory::create_expr_tree(&pool, texpr, &root, nullptr, true);
    ASSERT_TRUE(status.ok()) << status.message();
    ASSERT_NE(nullptr, root);
}

} // namespace starrocks
