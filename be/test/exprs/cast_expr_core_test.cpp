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

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/map_column.h"
#include "common/object_pool.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"

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

const auto int_map_type = TypeDescriptor::create_map_type(TYPE_INT_DESC, TYPE_INT_DESC);
const auto bigint_key_map_type = TypeDescriptor::create_map_type(TYPE_BIGINT_DESC, TYPE_INT_DESC);

StatusOr<ColumnPtr> cast_map(ColumnPtr input) {
    ObjectPool pool;
    auto* input_expr = pool.add(new ColumnRef(int_map_type, 0));
    auto* expr = VectorizedCastExprFactory::from_type(int_map_type, bigint_key_map_type, input_expr, &pool);
    if (expr == nullptr) {
        return Status::InternalError("Failed to create MAP cast expression");
    }
    Chunk chunk;
    chunk.append_column(std::move(input), 0);
    return expr->evaluate_checked(nullptr, &chunk);
}

} // namespace

TEST(CastExprCoreTest, from_thrift_succeeds_for_primitive_cast) {
    ObjectPool pool;
    TExprNode node = make_cast_node(TYPE_INT_DESC, TYPE_BIGINT_DESC);
    Expr* expr = VectorizedCastExprFactory::from_thrift(&pool, node);
    ASSERT_NE(nullptr, expr);
    pool.add(expr);
    EXPECT_TRUE(expr->is_cast_expr());
}

TEST(CastExprCoreTest, from_thrift_returns_null_for_unsupported_cast) {
    ObjectPool pool;
    TypeDescriptor from = make_array_type(TYPE_INT);
    TypeDescriptor to = TypeDescriptor::create_varchar_type(10);
    TExprNode node = make_cast_node(from, to);
    Expr* expr = VectorizedCastExprFactory::from_thrift(&pool, node);
    EXPECT_EQ(nullptr, expr);
}

TEST(CastExprCoreTest, map_cast_preserves_constant) {
    auto input = ColumnHelper::create_column(int_map_type, false);
    input->append_datum(DatumMap{{int32_t{1}, int32_t{7}}});
    auto result = cast_map(ConstColumn::create(std::move(input), 4));
    ASSERT_TRUE(result.ok()) << result.status();
    ASSERT_EQ(4, result.value()->size());
    ASSERT_TRUE(result.value()->is_constant());

    const auto* map = down_cast<const MapColumn*>(ColumnHelper::get_data_column(result.value().get()));
    EXPECT_EQ(1, map->size());
    EXPECT_EQ("{1:7}", map->debug_item(0));
    EXPECT_NE(nullptr, dynamic_cast<const Int64Column*>(ColumnHelper::get_data_column(map->keys_column().get())));
}

TEST(CastExprCoreTest, map_cast_preserves_nullable_rows) {
    auto input = ColumnHelper::create_column(int_map_type, true);
    input->append_datum(DatumMap{{int32_t{1}, int32_t{7}}});
    input->append_nulls(1);
    input->append_datum(DatumMap{});
    input->append_datum(DatumMap{{int32_t{2}, int32_t{9}}});
    auto result = cast_map(std::move(input));
    ASSERT_TRUE(result.ok()) << result.status();
    const auto& output = result.value();
    ASSERT_EQ(4, output->size());
    EXPECT_FALSE(output->is_constant());
    ASSERT_TRUE(output->is_nullable());
    EXPECT_EQ("{1:7}", output->debug_item(0));
    EXPECT_TRUE(output->is_null(1));
    EXPECT_EQ("{}", output->debug_item(2));
    EXPECT_EQ("{2:9}", output->debug_item(3));
}

TEST(CastExprCoreTest, map_cast_preserves_constant_null) {
    auto result = cast_map(ColumnHelper::create_const_null_column(4));
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(4, result.value()->size());
    EXPECT_TRUE(result.value()->only_null());
}

} // namespace starrocks
