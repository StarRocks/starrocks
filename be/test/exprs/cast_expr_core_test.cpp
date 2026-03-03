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
#include "exprs/cast_expr.h"

namespace starrocks {

namespace {

TExprNode make_cast_node(const TypeDescriptor& from, const TypeDescriptor& to) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::CAST_EXPR);
    node.__set_opcode(TExprOpcode::CAST);
    node.__set_num_children(1);
    node.__set_is_nullable(true);
    node.__set_type(to.to_thrift());
    node.__set_child_type(to_thrift(from.type));
    node.__set_child_type_desc(from.to_thrift());
    return node;
}

TypeDescriptor make_array_type(LogicalType elem_type) {
    TypeDescriptor array_type(TYPE_ARRAY);
    array_type.children.emplace_back(elem_type);
    return array_type;
}

} // namespace

TEST(CastExprCoreTest, from_thrift_succeeds_for_primitive_cast) {
    ObjectPool pool;
    TExprNode node = make_cast_node(TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_BIGINT));
    Expr* expr = VectorizedCastExprFactory::from_thrift(&pool, node);
    ASSERT_NE(nullptr, expr);
    pool.add(expr);
    EXPECT_TRUE(expr->is_cast_expr());
}

TEST(CastExprCoreTest, from_thrift_returns_null_for_unsupported_cast) {
    ObjectPool pool;
    TypeDescriptor from = make_array_type(TYPE_INT);
    TypeDescriptor to(TYPE_VARCHAR);
    to.len = 10;
    TExprNode node = make_cast_node(from, to);
    Expr* expr = VectorizedCastExprFactory::from_thrift(&pool, node);
    EXPECT_EQ(nullptr, expr);
}

} // namespace starrocks
