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

#include "butil/time.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "exprs/arithmetic_expr.h"
#include "exprs/cast_expr.h"
#include "exprs/function_call_expr.h"
#include "exprs/is_null_predicate.h"
#include "exprs/lambda_function.h"
#include "exprs/literal.h"
#include "exprs/map_apply_expr.h"
#include "exprs/map_expr.h"
#include "exprs/mock_vectorized_expr.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks {

ColumnPtr const_int_column(int32_t value, size_t size = 1) {
    auto data = Int32Column::create();
    data->append(value);
    return ConstColumn::create(std::move(data), size);
}

ColumnPtr const_varchar_column(const std::string& value, size_t size = 1) {
    auto data = BinaryColumn::create();
    data->append_string(value);
    return ConstColumn::create(std::move(data), size);
}

std::unique_ptr<MapApplyExpr> create_map_apply_expr(const TypeDescriptor& type) {
    return std::unique_ptr<MapApplyExpr>(new MapApplyExpr(type));
}

class MapApplyExprTest : public ::testing::Test {
protected:
    void SetUp() override {
        // init the int_type.
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::INT);
        node.__set_scalar_type(scalar_type);
        int_type.types.push_back(node);
        // init expr_node
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);

        create_lambda_expr();
        cur_chunk.append_column(const_int_column(1, 5), 1);
    }
    void TearDown() override { _objpool.clear(); }

    FakeConstExpr* new_fake_const_expr(ColumnPtr value, const TypeDescriptor& type) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::INT_LITERAL);
        node.__set_num_children(0);
        node.__set_type(type.to_thrift());
        FakeConstExpr* e = _objpool.add(new FakeConstExpr(node));
        e->_column = std::move(value);
        return e;
    }

    void create_lambda_expr() {
        // create lambda functions
        TExprNode tlambda_func;
        tlambda_func.opcode = TExprOpcode::ADD;
        tlambda_func.child_type = TPrimitiveType::INT;
        tlambda_func.node_type = TExprNodeType::LAMBDA_FUNCTION_EXPR;
        tlambda_func.num_children = 2;
        tlambda_func.__isset.opcode = true;
        tlambda_func.__isset.child_type = true;
        tlambda_func.type = gen_type_desc(TPrimitiveType::INT);
        LambdaFunction* lambda_func = _objpool.add(new LambdaFunction(tlambda_func));

        /// (k,v) -> map_expr(k,v)
        TExprNode int_slot_ref;
        int_slot_ref.node_type = TExprNodeType::SLOT_REF;
        int_slot_ref.type = int_type;
        int_slot_ref.num_children = 0;
        int_slot_ref.__isset.slot_ref = true;
        int_slot_ref.slot_ref.slot_id = 100;
        int_slot_ref.slot_ref.tuple_id = 0;
        int_slot_ref.__set_is_nullable(true);
        ColumnRef* col1 = _objpool.add(new ColumnRef(int_slot_ref));
        int_slot_ref.slot_ref.slot_id = 101;
        ColumnRef* col2 = _objpool.add(new ColumnRef(int_slot_ref));

        TExprNode tmap_expr;
        tmap_expr.opcode = TExprOpcode::ADD;
        tmap_expr.child_type = TPrimitiveType::INT;
        tmap_expr.node_type = TExprNodeType::MAP_EXPR;
        tmap_expr.num_children = 2;
        tmap_expr.__isset.opcode = true;
        tmap_expr.__isset.child_type = true;
        tmap_expr.type = gen_type_desc(TPrimitiveType::INT);
        auto* map_expr = _objpool.add(MapExprFactory::from_thrift(tmap_expr));
        map_expr->add_child(col1);
        map_expr->add_child(col2);

        // lambda : map_expr, key, value
        lambda_func->add_child(map_expr);
        lambda_func->add_child(col1);
        lambda_func->add_child(col2);
        _lambda_func.push_back(lambda_func);

        /// (k,v) -> map_expr(k is null, v + a)
        lambda_func = _objpool.add(new LambdaFunction(tlambda_func));
        int_slot_ref.slot_ref.slot_id = 103;
        ColumnRef* key3 = _objpool.add(new ColumnRef(int_slot_ref));
        int_slot_ref.slot_ref.slot_id = 104;
        ColumnRef* val3 = _objpool.add(new ColumnRef(int_slot_ref));
        // k is null
        expr_node.fn.name.function_name = "is_null_pred";
        auto* is_null = _objpool.add(VectorizedIsNullPredicateFactory::from_thrift(expr_node));
        is_null->add_child(key3);

        // v + a
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.type = gen_type_desc(TPrimitiveType::INT);
        auto* add_expr = _objpool.add(VectorizedArithmeticExprFactory::from_thrift(expr_node));
        int_slot_ref.slot_ref.slot_id = 1;
        ColumnRef* col7 = _objpool.add(new ColumnRef(int_slot_ref));
        add_expr->add_child(val3);
        add_expr->add_child(col7);
        // map_expr
        map_expr = _objpool.add(MapExprFactory::from_thrift(tmap_expr));
        map_expr->add_child(is_null);
        map_expr->add_child(add_expr);

        // lambda : map_expr, key, value
        lambda_func->add_child(map_expr);
        lambda_func->add_child(key3);
        lambda_func->add_child(val3);
        _lambda_func.push_back(lambda_func);
    }
    std::vector<Expr*> _lambda_func;
    TExprNode expr_node;
    Chunk cur_chunk = Chunk();

private:
    TTypeDesc int_type;
    RuntimeState _runtime_state;
    ObjectPool _objpool;
};

// NOLINTNEXTLINE
TEST_F(MapApplyExprTest, test_map_int_int) {
    TypeDescriptor type_map_int_int;
    type_map_int_int.type = LogicalType::TYPE_MAP;
    type_map_int_int.children.emplace_back(TypeDescriptor(LogicalType::TYPE_INT));
    type_map_int_int.children.emplace_back(TypeDescriptor(LogicalType::TYPE_INT));

    TypeDescriptor type_int(LogicalType::TYPE_INT);

    auto column = ColumnHelper::create_column(type_map_int_int, true);

    DatumMap map1;
    map1[(int32_t)1] = (int32_t)44;
    map1[(int32_t)2] = (int32_t)55;
    map1[(int32_t)4] = (int32_t)66;
    column->append_datum(map1);

    DatumMap map2;
    map2[(int32_t)2] = (int32_t)77;
    map2[(int32_t)3] = (int32_t)88;
    column->append_datum(map2);

    DatumMap map3;
    map3[(int32_t)3] = Datum();
    column->append_datum(map3);

    // {} empty
    column->append_datum(DatumMap());
    // NULL
    column->append_datum(Datum{});

    // Inputs:
    //   c0
    // --------
    //   [1->44, 2->55, 4->66]
    //   [2->77, 3->88]
    //   [3 -> NULL]
    //   []
    //   NULL
    //
    // Query:
    //   select map_apply((k,v)->(k,v),map)
    //
    // Outputs: the same

    {
        std::unique_ptr<MapApplyExpr> map_apply_expr = create_map_apply_expr(type_map_int_int);

        map_apply_expr->add_child(_lambda_func[0]);
        map_apply_expr->add_child(new_fake_const_expr(column, type_map_int_int));

        ExprContext exprContext(map_apply_expr.get());
        std::vector<ExprContext*> expr_ctxs = {&exprContext};
        ASSERT_OK(Expr::prepare(expr_ctxs, &_runtime_state));
        ASSERT_OK(Expr::open(expr_ctxs, &_runtime_state));
        ColumnPtr result = map_apply_expr->evaluate(&exprContext, &cur_chunk);

        EXPECT_TRUE(result->is_nullable());
        EXPECT_TRUE(result->debug_string() == column->debug_string());

        Expr::close(expr_ctxs, &_runtime_state);
    }

    // Inputs:
    //   c0
    // --------
    //   [1->44, 2->55, 4->66]
    //   [2->77, 3->88]
    //   [3 -> NULL]
    //   []
    //   NULL
    //
    // Query:
    //   select array_map((k,v)->(k is null, v + a), map)
    //
    // Outputs: "[{0:67}, {0:89}, {0:NULL}, {}, NULL]"

    {
        std::unique_ptr<MapApplyExpr> map_apply_expr = create_map_apply_expr(type_map_int_int);

        map_apply_expr->add_child(_lambda_func[1]);
        map_apply_expr->add_child(new_fake_const_expr(column, type_map_int_int));

        ExprContext exprContext(map_apply_expr.get());
        std::vector<ExprContext*> expr_ctxs = {&exprContext};
        ASSERT_OK(Expr::prepare(expr_ctxs, &_runtime_state));
        ASSERT_OK(Expr::open(expr_ctxs, &_runtime_state));
        ColumnPtr result = map_apply_expr->evaluate(&exprContext, &cur_chunk);

        EXPECT_TRUE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(), "[{0:67}, {0:89}, {0:NULL}, {}, NULL]");

        Expr::close(expr_ctxs, &_runtime_state);
    }
}

// NOLINTNEXTLINE
TEST_F(MapApplyExprTest, test_map_varchar_int) {
    TypeDescriptor type_map_varchar_int;
    type_map_varchar_int.type = LogicalType::TYPE_MAP;
    type_map_varchar_int.children.resize(2);
    type_map_varchar_int.children[0].type = LogicalType::TYPE_VARCHAR;
    type_map_varchar_int.children[0].len = 10;
    type_map_varchar_int.children[1].type = LogicalType::TYPE_INT;

    TypeDescriptor type_varchar(LogicalType::TYPE_VARCHAR);
    type_varchar.len = 10;

    TypeDescriptor type_int(LogicalType::TYPE_INT);

    auto column = ColumnHelper::create_column(type_map_varchar_int, false);

    DatumMap map;
    map[(Slice) "a"] = (int32_t)11;
    map[(Slice) "b"] = (int32_t)22;
    map[(Slice) "c"] = (int32_t)33;
    column->append_datum(map);

    DatumMap map1;
    map1[(Slice) "a"] = (int32_t)44;
    map1[(Slice) "b"] = (int32_t)55;
    map1[(Slice) "d"] = (int32_t)66;
    column->append_datum(map1);

    DatumMap map2;
    map2[(Slice) "b"] = (int32_t)77;
    map2[(Slice) "c"] = (int32_t)88;
    column->append_datum(map2);

    DatumMap map3;
    map3[(Slice) "b"] = (int32_t)99;
    column->append_datum(map3);

    column->append_datum(DatumMap());

    // Inputs:
    //   c0
    // --------
    //   [a->11, b->22, c->33]
    //   [a->44, b->55, d->66]
    //   [b->77, c->88]
    //   [b->99]
    //   []
    //
    // Query:
    //   select map_apply((k,v)->(k,v),map)
    //
    // Outputs: the same

    {
        std::unique_ptr<MapApplyExpr> map_apply_expr = create_map_apply_expr(type_map_varchar_int);

        map_apply_expr->add_child(_lambda_func[0]);
        map_apply_expr->add_child(new_fake_const_expr(column, type_map_varchar_int));

        ExprContext exprContext(map_apply_expr.get());
        std::vector<ExprContext*> expr_ctxs = {&exprContext};
        ASSERT_OK(Expr::prepare(expr_ctxs, &_runtime_state));
        ASSERT_OK(Expr::open(expr_ctxs, &_runtime_state));
        ColumnPtr result = map_apply_expr->evaluate(&exprContext, &cur_chunk);

        EXPECT_FALSE(result->is_nullable());
        EXPECT_TRUE(result->debug_string() == column->debug_string());

        Expr::close(expr_ctxs, &_runtime_state);
    }

    // Inputs:
    //   c0
    // --------
    //   [a->11, b->22, c->33]
    //   [a->44, b->55, d->66]
    //   [b->77, c->88]
    //   [b->99]
    //   []
    //
    // Query:
    //   select c0[3]
    //
    // Outputs: {0:34}, {0:67}, {0:89}, {0:100}, {}
    {
        std::unique_ptr<MapApplyExpr> map_apply_expr = create_map_apply_expr(type_map_varchar_int);

        map_apply_expr->add_child(_lambda_func[1]);
        map_apply_expr->add_child(new_fake_const_expr(column, type_map_varchar_int));

        ExprContext exprContext(map_apply_expr.get());
        std::vector<ExprContext*> expr_ctxs = {&exprContext};
        ASSERT_OK(Expr::prepare(expr_ctxs, &_runtime_state));
        ASSERT_OK(Expr::open(expr_ctxs, &_runtime_state));
        ColumnPtr result = map_apply_expr->evaluate(&exprContext, &cur_chunk);

        EXPECT_FALSE(result->is_nullable());
        EXPECT_STREQ(result->debug_string().c_str(), "{0:34}, {0:67}, {0:89}, {0:100}, {}");

        Expr::close(expr_ctxs, &_runtime_state);
    }
}

} // namespace starrocks
