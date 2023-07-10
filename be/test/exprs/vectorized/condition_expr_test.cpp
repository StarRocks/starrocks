// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/condition_expr.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <random>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/fixed_length_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/vectorized/mock_vectorized_expr.h"
#include "gen_cpp/Exprs_types.h"
#include "gutil/casts.h"
#include "runtime/primitive_type.h"

namespace starrocks::vectorized {

class VectorizedConditionExprTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BIGINT);
    }

public:
    TExprNode expr_node;
};

TEST_F(VectorizedConditionExprTest, ifNullLNotNull) {
    auto expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_null_expr(expr_node));

    MockVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10);
    MockVectorizedExpr<TYPE_BIGINT> col2(expr_node, 10, 20);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);
    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ASSERT_TRUE(ptr->is_numeric());

        auto v = ColumnHelper::cast_to_raw<TYPE_BIGINT>(ptr);
        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_EQ(10, v->get_data()[j]);
        }
    }
}

TEST_F(VectorizedConditionExprTest, ifNullLAllNull) {
    auto expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_null_expr(expr_node));

    MockNullVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10);
    MockVectorizedExpr<TYPE_BIGINT> col2(expr_node, 10, 20);

    col1.all_null = true;
    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);
    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ASSERT_TRUE(ptr->is_numeric());

        auto v = ColumnHelper::cast_to_raw<TYPE_BIGINT>(ptr);
        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_EQ(20, v->get_data()[j]);
        }
    }
}

TEST_F(VectorizedConditionExprTest, ifNull) {
    auto expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_null_expr(expr_node));

    MockNullVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10);
    MockVectorizedExpr<TYPE_BIGINT> col2(expr_node, 10, 20);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);
    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ASSERT_TRUE(ptr->is_numeric());

        auto v = ColumnHelper::cast_to_raw<TYPE_BIGINT>(ptr);
        for (int j = 0; j < ptr->size(); ++j) {
            if (j % 2 == 0) {
                ASSERT_EQ(10, v->get_data()[j]);
            } else {
                ASSERT_EQ(20, v->get_data()[j]);
            }
        }
    }
}

TEST_F(VectorizedConditionExprTest, ifNullNull) {
    auto expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_null_expr(expr_node));

    MockNullVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10);
    MockNullVectorizedExpr<TYPE_BIGINT> col2(expr_node, 10, 20);

    expr->_children.push_back(&col1);
    expr->_children.push_back(&col2);
    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ASSERT_TRUE(ptr->is_nullable());
        ASSERT_FALSE(ptr->is_numeric());

        auto v =
                ColumnHelper::cast_to_raw<TYPE_BIGINT>(ColumnHelper::as_raw_column<NullableColumn>(ptr)->data_column());
        for (int j = 0; j < ptr->size(); ++j) {
            if (j % 2 == 0) {
                ASSERT_EQ(10, v->get_data()[j]);
            } else {
                ASSERT_TRUE(ptr->is_null(j));
                ASSERT_EQ(20, v->get_data()[j]);
            }
        }
    }
}

template <PrimitiveType Type>
class RandomValueExpr final : public Expr {
public:
    RandomValueExpr(const TExprNode& t, size_t size, std::default_random_engine& re) : Expr(t), _re(re) { _init(size); }
    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override { return col; }

    typename RunTimeColumnType<Type>::Container get_data() { return col->get_data(); }

    Expr* clone(ObjectPool* pool) const override { return nullptr; }

private:
    void _init(int size) {
        std::uniform_int_distribution<int64_t> u(0, 4096);
        if constexpr (pt_is_decimal<Type>) {
            col = RunTimeColumnType<Type>::create(this->type().precision, this->type().scale);
        } else {
            col = RunTimeColumnType<Type>::create();
        }

        auto& data = col->get_data();
        data.resize(size);
        for (int i = 0; i < size; ++i) {
            if constexpr (Type == TYPE_BOOLEAN) {
                data[i] = static_cast<bool>(u(_re) % 2);
            } else {
                data[i] = u(_re);
            }
        }
    }
    std::shared_ptr<RunTimeColumnType<Type>> col;
    std::default_random_engine& _re;
};

template <PrimitiveType Type>
class MakeNullableExpr final : public Expr {
public:
    MakeNullableExpr(const TExprNode& t, size_t size, Expr* inner) : Expr(t), _inner(inner) { init(); }
    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override { return _col; }
    Expr* clone(ObjectPool* pool) const override { return nullptr; }

    ColumnPtr get_col_ptr() { return _col; }

private:
    void init() {
        auto res = _inner->evaluate(nullptr, nullptr);
        int sz = res->size();
        _col = NullableColumn::create(std::move(res), NullColumn::create(sz));
    }
    Expr* _inner;
    ColumnPtr _col;
};

TEST_F(VectorizedConditionExprTest, ifExpr) {
    std::default_random_engine e;

    int chunk_size = 4096 - 1;

    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    RandomValueExpr<TYPE_BOOLEAN> select_col(expr_node, chunk_size, e);
    // Test INT32
    expr_node.type = gen_type_desc(TPrimitiveType::INT);
    auto expr0 = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_expr(expr_node));
    RandomValueExpr<TYPE_INT> col1(expr_node, chunk_size, e);
    RandomValueExpr<TYPE_INT> col2(expr_node, chunk_size, e);

    expr0->_children.push_back(&select_col);
    expr0->_children.push_back(&col1);
    expr0->_children.push_back(&col2);

    ColumnPtr ptr = expr0->evaluate(nullptr, nullptr);
    auto* res_col0 = down_cast<Int32Column*>(ptr.get());
    for (int i = 0; i < res_col0->size(); ++i) {
        auto result = select_col.get_data()[i] ? col1.get_data()[i] : col2.get_data()[i];
        ASSERT_EQ(result, res_col0->get_data()[i]);
    }

    // Test FLOAT
    expr_node.type = gen_type_desc(TPrimitiveType::FLOAT);
    auto expr1 = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_expr(expr_node));
    RandomValueExpr<TYPE_FLOAT> col3(expr_node, chunk_size, e);
    RandomValueExpr<TYPE_FLOAT> col4(expr_node, chunk_size, e);
    expr1->_children.push_back(&select_col);
    expr1->_children.push_back(&col3);
    expr1->_children.push_back(&col4);

    ptr = expr1->evaluate(nullptr, nullptr);
    auto* res_col1 = down_cast<FloatColumn*>(ptr.get());
    for (int i = 0; i < res_col1->size(); ++i) {
        auto result = select_col.get_data()[i] ? col3.get_data()[i] : col4.get_data()[i];
        ASSERT_FLOAT_EQ(result, res_col1->get_data()[i]);
    }

    // Test INT8
    expr_node.type = gen_type_desc(TPrimitiveType::TINYINT);
    auto expr2 = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_expr(expr_node));
    RandomValueExpr<TYPE_TINYINT> col5(expr_node, chunk_size, e);
    RandomValueExpr<TYPE_TINYINT> col6(expr_node, chunk_size, e);
    expr2->_children.push_back(&select_col);
    expr2->_children.push_back(&col5);
    expr2->_children.push_back(&col6);

    ptr = expr2->evaluate(nullptr, nullptr);
    auto* res_col2 = down_cast<Int8Column*>(ptr.get());
    for (int i = 0; i < res_col2->size(); ++i) {
        auto result = select_col.get_data()[i] ? col5.get_data()[i] : col6.get_data()[i];
        ASSERT_EQ(result, res_col2->get_data()[i]);
    }

    // Test INT8 var const
    auto expr3 = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_expr(expr_node));
    RandomValueExpr<TYPE_TINYINT> col7(expr_node, chunk_size, e);
    MockConstVectorizedExpr<TYPE_TINYINT> col8(expr_node, 123);
    auto copyed_data = select_col.get_data();
    expr3->_children.push_back(&select_col);
    expr3->_children.push_back(&col7);
    expr3->_children.push_back(&col8);

    ptr = expr3->evaluate(nullptr, nullptr);
    auto* res_col3 = down_cast<Int8Column*>(ptr.get());
    for (int i = 0; i < res_col3->size(); ++i) {
        auto result = copyed_data[i] ? col7.get_data()[i] : 123;
        ASSERT_EQ(result, res_col3->get_data()[i]);
    }
    // Test INT8 const var
    auto expr4 = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_expr(expr_node));
    MockConstVectorizedExpr<TYPE_TINYINT> col9(expr_node, 123);
    RandomValueExpr<TYPE_TINYINT> col10(expr_node, chunk_size, e);
    copyed_data = select_col.get_data();
    expr4->_children.push_back(&select_col);
    expr4->_children.push_back(&col9);
    expr4->_children.push_back(&col10);

    ptr = expr4->evaluate(nullptr, nullptr);
    auto* res_col4 = down_cast<Int8Column*>(ptr.get());
    for (int i = 0; i < res_col4->size(); ++i) {
        auto result = copyed_data[i] ? 123 : col10.get_data()[i];
        ASSERT_EQ(result, res_col4->get_data()[i]);
    }
    // Test INT8 const const

    // Test Nullable(INT8) var const
    {
        expr_node.type = gen_type_desc(TPrimitiveType::TINYINT);
        auto if_expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_expr(expr_node));
        RandomValueExpr<TYPE_TINYINT> v_col(expr_node, chunk_size, e);
        MakeNullableExpr<TYPE_TINYINT> col_x(expr_node, chunk_size, &v_col);
        MockConstVectorizedExpr<TYPE_TINYINT> col_y(expr_node, 123);

        if_expr->_children.push_back(&select_col);
        if_expr->_children.push_back(&col_x);
        if_expr->_children.push_back(&col_y);

        ptr = if_expr->evaluate(nullptr, nullptr);

        ColumnViewer<TYPE_TINYINT> viewer(ptr);
        for (int i = 0; i < ptr->size(); ++i) {
            auto result = select_col.get_data()[i] ? v_col.get_data()[i] : 123;
            if (!viewer.is_null(i)) {
                ASSERT_EQ(viewer.value(i), result);
            } else {
                ASSERT_TRUE(select_col.get_data()[i]);
            }
        }
    }

    // Test Nullable(Selector) const const
    {
        expr_node.type = gen_type_desc(TPrimitiveType::TINYINT);
        auto if_expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_expr(expr_node));
        MakeNullableExpr<TYPE_TINYINT> nullable_selector(expr_node, chunk_size, &select_col);
        MockConstVectorizedExpr<TYPE_TINYINT> col_x(expr_node, 123);
        MockConstVectorizedExpr<TYPE_TINYINT> col_y(expr_node, 4);

        if_expr->_children.push_back(&nullable_selector);
        if_expr->_children.push_back(&col_x);
        if_expr->_children.push_back(&col_y);

        ptr = if_expr->evaluate(nullptr, nullptr);

        ColumnViewer<TYPE_BOOLEAN> sel_viewer(nullable_selector.get_col_ptr());
        auto* res_x = down_cast<Int8Column*>(ptr.get());

        for (int i = 0; i < ptr->size(); ++i) {
            auto result = 0;
            if (sel_viewer.is_null(i) || !sel_viewer.value(i)) {
                result = 4;
            } else {
                result = 123;
            }
            ASSERT_EQ(result, res_x->get_data()[i]);
        }
    }
}

} // namespace starrocks::vectorized
