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

#include <cmath>

#include "butil/time.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "exprs/arithmetic_expr.h"
#include "exprs/array_expr.h"
#include "exprs/array_map_expr.h"
#include "exprs/cast_expr.h"
#include "exprs/function_call_expr.h"
#include "exprs/is_null_predicate.h"
#include "exprs/lambda_function.h"
#include "exprs/literal.h"
#include "exprs/mock_vectorized_expr.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks {

ColumnPtr build_int_column(const std::vector<int>& values) {
    auto data = Int32Column::create();
    data->append_numbers(values.data(), values.size() * sizeof(int32_t));
    return data;
}

class VectorizedLambdaFunctionExprTest : public ::testing::Test {
public:
    void SetUp() override { create_array_expr(); }

    static TExprNode create_expr_node();

    static std::vector<Expr*> create_lambda_expr(ObjectPool* pool);

    void create_array_expr() {
        TypeDescriptor type_arr_int;
        type_arr_int.type = LogicalType::TYPE_ARRAY;
        type_arr_int.children.emplace_back();
        type_arr_int.children.back().type = LogicalType::TYPE_INT;

        // [1,4]
        // [null,null]
        // [null,12]
        auto array = ColumnHelper::create_column(type_arr_int, true);
        array->append_datum(DatumArray{Datum((int32_t)1), Datum((int32_t)4)}); // [1,4]
        array->append_datum(DatumArray{Datum(), Datum()});                     // [NULL, NULL]
        array->append_datum(DatumArray{Datum(), Datum((int32_t)12)});          // [NULL, 12]
        auto* array_values = new_fake_const_expr(array, type_arr_int);
        _array_expr.push_back(array_values);

        // null
        array = ColumnHelper::create_column(type_arr_int, true);
        array->append_datum(Datum{}); // null
        auto* const_null = new_fake_const_expr(array, type_arr_int);
        _array_expr.push_back(const_null);

        // [null]
        array = ColumnHelper::create_column(type_arr_int, true);
        array->append_datum(DatumArray{Datum()});
        auto* null_array = new_fake_const_expr(array, type_arr_int);
        _array_expr.push_back(null_array);

        // []
        array = ColumnHelper::create_column(type_arr_int, true);
        array->append_datum(DatumArray{}); // []
        auto empty_array = new_fake_const_expr(array, type_arr_int);
        _array_expr.push_back(empty_array);

        // [null]
        // []
        // NULL
        array = ColumnHelper::create_column(type_arr_int, true);
        array->append_datum(DatumArray{Datum()}); // [null]
        array->append_datum(DatumArray{});        // []
        array->append_datum(Datum{});             // NULL
        auto* array_special = new_fake_const_expr(array, type_arr_int);
        _array_expr.push_back(array_special);

        // const([1,4]...)
        array = ColumnHelper::create_column(type_arr_int, false);
        array->append_datum(DatumArray{Datum((int32_t)1), Datum((int32_t)4)}); // [1,4]
        auto const_col = ConstColumn::create(array, 3);
        auto* const_array = new_fake_const_expr(const_col, type_arr_int);
        _array_expr.push_back(const_array);

        // const(null...)
        array = ColumnHelper::create_column(type_arr_int, true);
        array->append_datum(Datum{}); // null...
        const_col = ConstColumn::create(array, 3);
        const_array = new_fake_const_expr(const_col, type_arr_int);
        _array_expr.push_back(const_array);

        // const([null]...)
        array = ColumnHelper::create_column(type_arr_int, false);
        array->append_datum(DatumArray{Datum()}); // [null]...
        const_col = ConstColumn::create(array, 3);
        const_array = new_fake_const_expr(const_col, type_arr_int);
        _array_expr.push_back(const_array);

        // const([]...)
        array = ColumnHelper::create_column(type_arr_int, false);
        array->append_datum(DatumArray{}); // []...
        const_col = ConstColumn::create(array, 3);
        const_array = new_fake_const_expr(const_col, type_arr_int);
        _array_expr.push_back(const_array);
    }

    FakeConstExpr* new_fake_const_expr(ColumnPtr value, const TypeDescriptor& type) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::INT_LITERAL);
        node.__set_num_children(0);
        node.__set_type(type.to_thrift());
        FakeConstExpr* e = _objpool.add(new FakeConstExpr(node));
        e->_column = std::move(value);
        return e;
    }

    static TExprNode create_int_literal_node(int64_t value_literal) {
        TExprNode lit_node;
        lit_node.__set_node_type(TExprNodeType::INT_LITERAL);
        lit_node.__set_num_children(0);
        lit_node.__set_type(gen_type_desc(TPrimitiveType::INT));
        TIntLiteral lit_value;
        lit_value.__set_value(value_literal);
        lit_node.__set_int_literal(lit_value);
        return lit_node;
    }

    std::vector<Expr*> _array_expr;
    std::vector<Chunk*> _chunks;

protected:
    RuntimeState _runtime_state;
    ObjectPool _objpool;
};

TExprNode VectorizedLambdaFunctionExprTest::create_expr_node() {
    TExprNode expr_node;
    expr_node.opcode = TExprOpcode::ADD;
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.node_type = TExprNodeType::BINARY_PRED;
    expr_node.num_children = 2;
    expr_node.__isset.opcode = true;
    expr_node.__isset.child_type = true;
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    return expr_node;
}

std::vector<Expr*> VectorizedLambdaFunctionExprTest::create_lambda_expr(ObjectPool* pool) {
    std::vector<Expr*> lambda_funcs;

    // create lambda functions
    TExprNode tlambda_func;
    tlambda_func.opcode = TExprOpcode::ADD;
    tlambda_func.child_type = TPrimitiveType::INT;
    tlambda_func.node_type = TExprNodeType::LAMBDA_FUNCTION_EXPR;
    tlambda_func.num_children = 2;
    tlambda_func.__isset.opcode = true;
    tlambda_func.__isset.child_type = true;
    tlambda_func.type = gen_type_desc(TPrimitiveType::INT);
    LambdaFunction* lambda_func = pool->add(new LambdaFunction(tlambda_func));

    // x -> x
    TExprNode slot_ref;
    slot_ref.node_type = TExprNodeType::SLOT_REF;
    slot_ref.type = gen_type_desc(TPrimitiveType::INT);
    slot_ref.num_children = 0;
    slot_ref.__isset.slot_ref = true;
    slot_ref.slot_ref.slot_id = 100000;
    slot_ref.slot_ref.tuple_id = 0;
    slot_ref.__set_is_nullable(true);

    ColumnRef* col1 = pool->add(new ColumnRef(slot_ref));
    ColumnRef* col2 = pool->add(new ColumnRef(slot_ref));
    lambda_func->add_child(col1);
    lambda_func->add_child(col2);
    lambda_funcs.push_back(lambda_func);

    // x -> x is null
    lambda_func = pool->add(new LambdaFunction(tlambda_func));
    ColumnRef* col3 = pool->add(new ColumnRef(slot_ref));
    ColumnRef* col4 = pool->add(new ColumnRef(slot_ref));
    TExprNode node = create_expr_node();
    node.fn.name.function_name = "is_null_pred";
    auto* is_null = pool->add(VectorizedIsNullPredicateFactory::from_thrift(node));
    is_null->add_child(col4);
    lambda_func->add_child(is_null);
    lambda_func->add_child(col3);
    lambda_funcs.push_back(lambda_func);

    // x -> x + a (captured columns)
    lambda_func = pool->add(new LambdaFunction(tlambda_func));
    ColumnRef* col5 = pool->add(new ColumnRef(slot_ref));
    node = create_expr_node();
    node.opcode = TExprOpcode::ADD;
    node.type = gen_type_desc(TPrimitiveType::INT);
    auto* add_expr = pool->add(VectorizedArithmeticExprFactory::from_thrift(node));
    ColumnRef* col6 = pool->add(new ColumnRef(slot_ref));
    slot_ref.slot_ref.slot_id = 1;
    ColumnRef* col7 = pool->add(new ColumnRef(slot_ref));
    add_expr->_children.push_back(col6);
    add_expr->_children.push_back(col7);
    lambda_func->add_child(add_expr);
    lambda_func->add_child(col5);
    lambda_funcs.push_back(lambda_func);

    // x -> -110
    lambda_func = pool->add(new LambdaFunction(tlambda_func));
    auto tint_literal = create_int_literal_node(-110);
    auto int_literal = pool->add(new VectorizedLiteral(tint_literal));
    slot_ref.slot_ref.slot_id = 100000;
    ColumnRef* col8 = pool->add(new ColumnRef(slot_ref));
    lambda_func->add_child(int_literal);
    lambda_func->add_child(col8);
    lambda_funcs.push_back(lambda_func);
    return lambda_funcs;
}

// just consider one level, not nested
// array_map(lambdaFunction(x<type>, lambdaExpr),array<type>)
TEST_F(VectorizedLambdaFunctionExprTest, array_map_lambda_test_normal_array) {
    auto cur_chunk = std::make_shared<Chunk>();
    std::vector<int> vec_a = {1, 1, 1};
    cur_chunk->append_column(build_int_column(vec_a), 1);
    for (int i = 0; i < 1; ++i) {
        auto lambda_funcs = create_lambda_expr(&_objpool);
        for (int j = 0; j < lambda_funcs.size(); ++j) {
            ArrayMapExpr array_map_expr(array_type(TYPE_INT));
            array_map_expr.clear_children();
            array_map_expr.add_child(lambda_funcs[j]);
            array_map_expr.add_child(_array_expr[i]);
            ExprContext exprContext(&array_map_expr);
            std::vector<ExprContext*> expr_ctxs = {&exprContext};
            ASSERT_OK(Expr::prepare(expr_ctxs, &_runtime_state));
            ASSERT_OK(Expr::open(expr_ctxs, &_runtime_state));
            auto lambda = dynamic_cast<LambdaFunction*>(lambda_funcs[j]);

            // check LambdaFunction::prepare()
            std::vector<SlotId> ids, arguments;
            lambda->get_slot_ids(&ids);
            lambda->get_lambda_arguments_ids(&arguments);

            ASSERT_TRUE(arguments.size() == 1 && arguments[0] == 100000); // the x's slot_id = 100000
            if (j == 2) {
                ASSERT_TRUE(ids.size() == 1 && ids[0] == 1); // the slot_id of the captured column is 1
            } else {
                ASSERT_TRUE(ids.empty());
            }

            ColumnPtr result = array_map_expr.evaluate(&exprContext, cur_chunk.get());

            if (i == 0 && j == 0) { // array_map(x -> x, array<int>)
                                    // [1,4]
                                    // [null,null]
                                    // [null,12]
                ASSERT_FALSE(result->is_constant());
                ASSERT_FALSE(result->is_numeric());

                EXPECT_EQ(3, result->size());
                EXPECT_EQ(1, result->get(0).get_array()[0].get_int32());
                EXPECT_EQ(4, result->get(0).get_array()[1].get_int32());
                ASSERT_TRUE(result->get(1).get_array()[0].is_null());
                ASSERT_TRUE(result->get(1).get_array()[1].is_null());
                ASSERT_TRUE(result->get(2).get_array()[0].is_null());
                EXPECT_EQ(12, result->get(2).get_array()[1].get_int32());
            } else if (i == 0 && j == 1) { // array_map(x -> x is null, array<int>)
                EXPECT_EQ(3, result->size());
                EXPECT_EQ(0, result->get(0).get_array()[0].get_int8());
                EXPECT_EQ(0, result->get(0).get_array()[1].get_int8());
                EXPECT_EQ(1, result->get(1).get_array()[0].get_int8());
                EXPECT_EQ(1, result->get(1).get_array()[1].get_int8());
                EXPECT_EQ(1, result->get(2).get_array()[0].get_int8());
                EXPECT_EQ(0, result->get(2).get_array()[1].get_int8());
            } else if (i == 0 && j == 2) { // // array_map(x -> x+a, array<int>)
                EXPECT_EQ(3, result->size());
                EXPECT_EQ(2, result->get(0).get_array()[0].get_int32());
                EXPECT_EQ(5, result->get(0).get_array()[1].get_int32());
                ASSERT_TRUE(result->get(1).get_array()[0].is_null());
                ASSERT_TRUE(result->get(1).get_array()[1].is_null());
                ASSERT_TRUE(result->get(2).get_array()[0].is_null());
                EXPECT_EQ(13, result->get(2).get_array()[1].get_int32());
            } else if (i == 0 && j == 3) {
                EXPECT_EQ(3, result->size());
                EXPECT_EQ(-110, result->get(0).get_array()[0].get_int32());
                EXPECT_EQ(-110, result->get(0).get_array()[1].get_int32());
                EXPECT_EQ(-110, result->get(1).get_array()[0].get_int32());
                EXPECT_EQ(-110, result->get(1).get_array()[1].get_int32());
                EXPECT_EQ(-110, result->get(2).get_array()[0].get_int32());
                EXPECT_EQ(-110, result->get(2).get_array()[1].get_int32());
            }

            Expr::close(expr_ctxs, &_runtime_state);
        }
    }
}

TEST_F(VectorizedLambdaFunctionExprTest, array_map_lambda_test_special_array) {
    auto cur_chunk = std::make_shared<Chunk>();
    std::vector<int> vec_a = {1, 1, 1};
    cur_chunk->append_column(build_int_column(vec_a), 1);
    for (int i = 1; i < 5; ++i) {
        auto lambda_funcs = create_lambda_expr(&_objpool);
        for (int j = 0; j < lambda_funcs.size(); ++j) {
            ArrayMapExpr array_map_expr(array_type(TYPE_INT));
            array_map_expr.clear_children();
            array_map_expr.add_child(lambda_funcs[j]);
            array_map_expr.add_child(_array_expr[i]);
            ExprContext exprContext(&array_map_expr);
            std::vector<ExprContext*> expr_ctxs = {&exprContext};
            ASSERT_OK(Expr::prepare(expr_ctxs, &_runtime_state));
            ASSERT_OK(Expr::open(expr_ctxs, &_runtime_state));
            auto lambda = dynamic_cast<LambdaFunction*>(lambda_funcs[j]);

            // check LambdaFunction::prepare()
            std::vector<SlotId> ids, arguments;
            lambda->get_slot_ids(&ids);
            lambda->get_lambda_arguments_ids(&arguments);

            ASSERT_TRUE(arguments.size() == 1 && arguments[0] == 100000); // the x's slot_id = 100000
            if (j == 2) {
                ASSERT_TRUE(ids.size() == 1 && ids[0] == 1); // the slot_id of the captured column is 1
            } else {
                ASSERT_TRUE(ids.empty());
            }

            ColumnPtr result = array_map_expr.evaluate(&exprContext, cur_chunk.get());

            if (i == 1) { // array_map(x->xxx,null)
                EXPECT_EQ(1, result->size());
                ASSERT_TRUE(result->is_null(0));
            } else if (i == 2 && (j == 0 || j == 2)) { // array_map( x->x || x->x+a, [null])
                EXPECT_EQ(1, result->size());
                ASSERT_TRUE(result->get(0).get_array()[0].is_null());
            } else if (i == 2 && j == 1) { // array_map(x -> x is null,[null])
                EXPECT_EQ(1, result->size());
                EXPECT_EQ(1, result->get(0).get_array()[0].get_int8());
            } else if (i == 2 && j == 3) { // array_map(x -> -110,[null])
                EXPECT_EQ(1, result->size());
                EXPECT_EQ(-110, result->get(0).get_array()[0].get_int32());
            } else if (i == 3) { // array_map(x->xxx,[])
                EXPECT_EQ(1, result->size());
                ASSERT_TRUE(result->get(0).get_array().empty());
            } else if (i == 4 && (j == 0 || j == 2)) { // array_map(x->x || x->x+a, array<special>)
                                                       // [null]
                                                       // []
                                                       // NULL
                EXPECT_EQ(3, result->size());
                ASSERT_TRUE(result->get(0).get_array()[0].is_null());
                ASSERT_TRUE(result->get(1).get_array().empty());
                ASSERT_TRUE(result->is_null(2));
            } else if (i == 4 && j == 1) { // array_map(x->x is null, array<special>)
                EXPECT_EQ(3, result->size());
                EXPECT_EQ(1, result->get(0).get_array()[0].get_int8());
                ASSERT_TRUE(result->get(1).get_array().empty());
                ASSERT_TRUE(result->is_null(2));
            } else if (i == 4 && j == 3) { // array_map(x-> -110, array<special>)
                EXPECT_EQ(3, result->size());
                EXPECT_EQ(-110, result->get(0).get_array()[0].get_int32());
                ASSERT_TRUE(result->get(1).get_array().empty());
                ASSERT_TRUE(result->is_null(2));
            }

            Expr::close(expr_ctxs, &_runtime_state);
        }
    }
}

TEST_F(VectorizedLambdaFunctionExprTest, array_map_lambda_test_const_array) {
    auto cur_chunk = std::make_shared<Chunk>();
    std::vector<int> vec_a = {1, 1, 1};
    cur_chunk->append_column(build_int_column(vec_a), 1);
    for (int i = 5; i < _array_expr.size(); ++i) {
        auto lambda_funcs = create_lambda_expr(&_objpool);
        for (int j = 0; j < lambda_funcs.size(); ++j) {
            ArrayMapExpr array_map_expr(array_type(j == 1 ? TYPE_BOOLEAN : TYPE_INT));
            array_map_expr.clear_children();
            array_map_expr.add_child(lambda_funcs[j]);
            array_map_expr.add_child(_array_expr[i]);
            ExprContext exprContext(&array_map_expr);
            std::vector<ExprContext*> expr_ctxs = {&exprContext};
            ASSERT_OK(Expr::prepare(expr_ctxs, &_runtime_state));
            ASSERT_OK(Expr::open(expr_ctxs, &_runtime_state));
            auto lambda = dynamic_cast<LambdaFunction*>(lambda_funcs[j]);

            // check LambdaFunction::prepare()
            std::vector<SlotId> ids, arguments;
            lambda->get_slot_ids(&ids);
            lambda->get_lambda_arguments_ids(&arguments);

            ASSERT_TRUE(arguments.size() == 1 && arguments[0] == 100000); // the x's slot_id = 100000
            if (j == 2) {
                ASSERT_TRUE(ids.size() == 1 && ids[0] == 1); // the slot_id of the captured column is 1
            } else {
                ASSERT_TRUE(ids.empty());
            }

            ColumnPtr result = array_map_expr.evaluate(&exprContext, cur_chunk.get());
            if (i == 5 && j == 0) { // array_map( x->x, array<const[1,4]...>)
                EXPECT_EQ(3, result->size());
                EXPECT_EQ(1, result->get(0).get_array()[0].get_int32());
                EXPECT_EQ(4, result->get(0).get_array()[1].get_int32());
                EXPECT_EQ(1, result->get(1).get_array()[0].get_int32());
                EXPECT_EQ(4, result->get(1).get_array()[1].get_int32());
                EXPECT_EQ(1, result->get(2).get_array()[0].get_int32());
                EXPECT_EQ(4, result->get(2).get_array()[1].get_int32());
            } else if (i == 5 && j == 1) { // array_map(x->x is null, array<const[1,4]...>)
                EXPECT_EQ(3, result->size());
                EXPECT_EQ(0, result->get(0).get_array()[0].get_int8());
                EXPECT_EQ(0, result->get(0).get_array()[1].get_int8());
                EXPECT_EQ(0, result->get(1).get_array()[0].get_int8());
                EXPECT_EQ(0, result->get(1).get_array()[1].get_int8());
                EXPECT_EQ(0, result->get(2).get_array()[0].get_int8());
                EXPECT_EQ(0, result->get(2).get_array()[1].get_int8());
            } else if (i == 5 && j == 2) { // // array_map( x->x + a, array<const[1,4]...>)
                EXPECT_EQ(3, result->size());
                EXPECT_EQ(2, result->get(0).get_array()[0].get_int32());
                EXPECT_EQ(5, result->get(0).get_array()[1].get_int32());
                EXPECT_EQ(2, result->get(1).get_array()[0].get_int32());
                EXPECT_EQ(5, result->get(1).get_array()[1].get_int32());
                EXPECT_EQ(2, result->get(2).get_array()[0].get_int32());
                EXPECT_EQ(5, result->get(2).get_array()[1].get_int32());
            } else if (i == 5 && j == 3) { // // array_map( x-> -110, array<const[1,4]...>)
                EXPECT_EQ(3, result->size());
                EXPECT_EQ(-110, result->get(0).get_array()[0].get_int32());
                EXPECT_EQ(-110, result->get(0).get_array()[1].get_int32());
                EXPECT_EQ(-110, result->get(1).get_array()[0].get_int32());
                EXPECT_EQ(-110, result->get(1).get_array()[1].get_int32());
                EXPECT_EQ(-110, result->get(2).get_array()[0].get_int32());
                EXPECT_EQ(-110, result->get(2).get_array()[1].get_int32());
            } else if (i == 6) { // array_map(x -> x || x->x is null || x -> x+a, array<const(null...)>)
                EXPECT_EQ(3, result->size());
                ASSERT_TRUE(result->is_null(0));
                ASSERT_TRUE(result->is_null(1));
                ASSERT_TRUE(result->is_null(2));
            } else if (i == 7 && (j == 0 || j == 2)) { // array_map(x -> x || x-> x+a,array<const([null]...)>)
                EXPECT_EQ(3, result->size());
                ASSERT_TRUE(result->get(0).get_array()[0].is_null());
                ASSERT_TRUE(result->get(1).get_array()[0].is_null());
                ASSERT_TRUE(result->get(2).get_array()[0].is_null());

            } else if (i == 7 && j == 1) { // array_map(x -> x is null, array<const([null]...)>)
                EXPECT_EQ(3, result->size());
                EXPECT_EQ(1, result->get(0).get_array()[0].get_int8());
                EXPECT_EQ(1, result->get(1).get_array()[0].get_int8());
                EXPECT_EQ(1, result->get(2).get_array()[0].get_int8());
            } else if (i == 7 && j == 3) { // array_map(x -> -110, array<const([null]...)>)
                EXPECT_EQ(3, result->size());
                EXPECT_EQ(-110, result->get(0).get_array()[0].get_int32());
                EXPECT_EQ(-110, result->get(1).get_array()[0].get_int32());
                EXPECT_EQ(-110, result->get(2).get_array()[0].get_int32());
            } else if (i == 8) { // array_map(x -> x || x -> x is null || x -> x+a || x -> -110, array<const([]...)>)
                EXPECT_EQ(3, result->size());
                ASSERT_TRUE(result->get(0).get_array().empty());
                ASSERT_TRUE(result->get(1).get_array().empty());
                ASSERT_TRUE(result->get(2).get_array().empty());
            }

            if (j == 1) { // array<int> -> array<bool>
                if (result->is_nullable()) {
                    auto col = std::dynamic_pointer_cast<NullableColumn>(result);
                    auto array_col = std::dynamic_pointer_cast<ArrayColumn>(col->data_column());
                    EXPECT_EQ(2, array_col->elements_column()->type_size()); // nullable bool
                } else {
                    auto array_col = std::dynamic_pointer_cast<ArrayColumn>(result);
                    EXPECT_EQ(2, array_col->elements_column()->type_size()); // nullable bool
                }
            }
            Expr::close(expr_ctxs, &_runtime_state);
        }
    }
}

} // namespace starrocks
