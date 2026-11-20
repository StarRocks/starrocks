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

#include "exprs/condition_expr.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <optional>
#include <random>
#include <string>
#include <vector>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/fixed_length_column.h"
#include "column/runtime_type_traits.h"
#include "column/simd_selector.h"
#include "column/variant_column.h"
#include "column/variant_encoder.h"
#include "column/vectorized_fwd.h"
#include "exprs/expr_factory.h"
#include "exprs/mock_vectorized_expr.h"
#include "gen_cpp/Exprs_types.h"
#include "gutil/casts.h"
#include "types/logical_type.h"

namespace starrocks {

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
        tttype_desc.push_back(expr_node.type);

        TTypeDesc ttype_desc;
        ttype_desc.__isset.types = true;
        ttype_desc.types.emplace_back();
        ttype_desc.types.back().__set_type(TTypeNodeType::ARRAY);
        ttype_desc.types.emplace_back();
        ttype_desc.types.back().__set_type(TTypeNodeType::SCALAR);
        ttype_desc.types.back().__set_scalar_type(TScalarType());
        ttype_desc.types.back().scalar_type.__set_type(TPrimitiveType::INT);
        ttype_desc.types.back().scalar_type.__set_len(10);
        tttype_desc.push_back(ttype_desc);

        tttype_desc1.push_back(ttype_desc);
        tttype_desc1.push_back(gen_type_desc(TPrimitiveType::TINYINT));
    }

private:
    std::vector<TTypeDesc> tttype_desc;
    std::vector<TTypeDesc> tttype_desc1;
    TExprNode expr_node;
};

// VARIANT goes down the same row-wise Column::append() path as ARRAY/MAP/STRUCT, because a shredded
// VariantColumn keeps its rows in _typed_columns/_metadata_column/_remain_value_column rather than in
// ObjectColumn::_pool, which is the only place ColumnViewer<TYPE_VARIANT> would look. Every column built
// here is shredded, so a regression back onto the viewer/builder path cannot pass these tests.
static MutableColumnPtr create_variant_column(const std::vector<std::string>& json_values) {
    auto column = VariantColumn::create();
    for (const auto& json : json_values) {
        auto encoded = VariantEncoder::encode_json_text_to_variant(json);
        CHECK(encoded.ok()) << encoded.status().to_string();
        column->append(encoded.value());
    }
    CHECK(column->is_shredded_variant());
    return column;
}

static MutableColumnPtr create_nullable_variant_column(const std::vector<std::optional<std::string>>& values) {
    std::vector<std::string> data;
    auto nulls = NullColumn::create();
    for (const auto& value : values) {
        // Null rows still need a well-formed payload underneath, so the shredded layout stays intact.
        data.push_back(value.value_or("0"));
        nulls->append(value.has_value() ? 0 : 1);
    }
    return NullableColumn::create(create_variant_column(data), std::move(nulls));
}

static void assert_variant_result(const ColumnPtr& result, const std::vector<std::optional<std::string>>& expected) {
    ASSERT_EQ(expected.size(), result->size());

    const Column* data_column = result.get();
    const bool is_const = data_column->is_constant();
    if (is_const) {
        data_column = down_cast<const ConstColumn*>(data_column)->data_column().get();
    }
    if (data_column->is_nullable()) {
        data_column = down_cast<const NullableColumn*>(data_column)->data_column().get();
    }
    const auto* variant_column = down_cast<const VariantColumn*>(data_column);

    for (size_t i = 0; i < expected.size(); ++i) {
        SCOPED_TRACE(i);
        if (!expected[i].has_value()) {
            EXPECT_TRUE(result->is_null(i));
            continue;
        }
        ASSERT_FALSE(result->is_null(i));
        VariantRowValue row_buffer;
        const VariantRowValue* row = variant_column->get_row_value(is_const ? 0 : i, &row_buffer);
        ASSERT_NE(nullptr, row);
        EXPECT_EQ(expected[i].value(), row->to_string());
    }
}

TEST_F(VectorizedConditionExprTest, ifNullLArray) {
    expr_node.type = tttype_desc[1];
    auto expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_null_expr(expr_node));

    auto array0 = ColumnHelper::create_column(TYPE_ARRAY_INT_DESC, true);
    array0->append_datum(DatumArray{Datum((int32_t)1), Datum((int32_t)4)}); // [1,4]
    array0->append_datum(DatumArray{Datum(), Datum()});                     // [NULL, NULL]
    array0->append_datum(Datum{});                                          // NULL
    auto array_expr0 = MockExpr(TYPE_ARRAY_INT_DESC, array0);

    auto array1 = ColumnHelper::create_column(TYPE_ARRAY_INT_DESC, false);
    array1->append_datum(DatumArray{Datum((int32_t)11), Datum((int32_t)41)}); // [11,41]
    array1->append_datum(DatumArray{Datum(), Datum()});                       // [NULL, NULL]
    array1->append_datum(DatumArray{Datum(), Datum((int32_t)1)});             // [NULL, 1]
    auto array_expr1 = MockExpr(TYPE_ARRAY_INT_DESC, array1);

    expr->_children.push_back(&array_expr0);
    expr->_children.push_back(&array_expr1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        if (ptr->is_nullable()) {
            ptr = down_cast<const NullableColumn*>(ptr.get())->data_column();
        }
        ASSERT_TRUE(ptr->is_array());
        ASSERT_TRUE(array0->equals(0, *ptr, 0));
        ASSERT_TRUE(array0->equals(1, *ptr, 1));
        ASSERT_TRUE(array1->equals(2, *ptr, 2));
    }
}

TEST_F(VectorizedConditionExprTest, ifNullLNotNull) {
    for (const auto& desc : tttype_desc) {
        expr_node.type = desc;
        auto expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_null_expr(expr_node));
        expr->set_type(TYPE_BIGINT_DESC);
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
}

TEST_F(VectorizedConditionExprTest, ifNullLAllNull) {
    for (const auto& desc : tttype_desc) {
        expr_node.type = desc;
        auto expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_null_expr(expr_node));
        expr->set_type(TYPE_BIGINT_DESC);

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
}

TEST_F(VectorizedConditionExprTest, ifNull) {
    for (const auto& desc : tttype_desc) {
        expr_node.type = desc;
        auto expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_null_expr(expr_node));
        expr->set_type(TYPE_BIGINT_DESC);

        MockNullVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10);
        MockVectorizedExpr<TYPE_BIGINT> col2(expr_node, 10, 20);

        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);
        {
            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            if (ptr->is_nullable()) {
                ptr = down_cast<const NullableColumn*>(ptr.get())->data_column();
            }
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
}

TEST_F(VectorizedConditionExprTest, ifNullRightConst) {
    for (const auto& desc : tttype_desc) {
        expr_node.type = desc;
        auto expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_null_expr(expr_node));
        expr->set_type(TYPE_BIGINT_DESC);

        MockNullVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10);
        MockConstVectorizedExpr<TYPE_BIGINT> col2(expr_node, 20); // const

        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);
        {
            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            if (ptr->is_nullable()) {
                ptr = down_cast<const NullableColumn*>(ptr.get())->data_column();
            }
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
}

TEST_F(VectorizedConditionExprTest, ifNullNull) {
    for (const auto& desc : tttype_desc) {
        expr_node.type = desc;
        auto expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_null_expr(expr_node));
        expr->set_type(TYPE_BIGINT_DESC);

        MockNullVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10);
        MockNullVectorizedExpr<TYPE_BIGINT> col2(expr_node, 10, 20);

        expr->_children.push_back(&col1);
        expr->_children.push_back(&col2);
        {
            ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
            ASSERT_TRUE(ptr->is_nullable());
            ASSERT_FALSE(ptr->is_numeric());

            auto v = ColumnHelper::cast_to_raw<TYPE_BIGINT>(
                    ColumnHelper::as_raw_column<NullableColumn>(ptr)->data_column());
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
}

template <LogicalType Type>
class RandomValueExpr final : public Expr {
public:
    RandomValueExpr(const TExprNode& t, size_t size, std::default_random_engine& re) : Expr(t), _re(re) { _init(size); }
    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override { return col; }

    typename RunTimeColumnType<Type>::ImmContainer get_data() {
        return ColumnHelper::as_raw_column<RunTimeColumnType<Type>>(col.get())->immutable_data();
    }

    Expr* clone(ObjectPool* pool) const override { return nullptr; }

private:
    void _init(int size) {
        std::uniform_int_distribution<int64_t> u(0, 4096);
        MutableColumnPtr mutable_col;
        if constexpr (lt_is_decimal<Type>) {
            mutable_col = RunTimeColumnType<Type>::create(this->type().precision, this->type().scale);
        } else {
            mutable_col = RunTimeColumnType<Type>::create();
        }

        auto* typed_col = ColumnHelper::as_raw_column<RunTimeColumnType<Type>>(mutable_col.get());
        auto& data = typed_col->get_data();
        data.resize(size);
        for (int i = 0; i < size; ++i) {
            if constexpr (Type == TYPE_BOOLEAN) {
                data[i] = static_cast<bool>(u(_re) % 2);
            } else {
                data[i] = u(_re);
            }
        }
        col = ColumnHelper::cast_to<Type>(std::move(mutable_col));
    }
    typename RunTimeColumnType<Type>::Ptr col;
    std::default_random_engine& _re;
};

template <LogicalType Type>
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
    const auto* res_col0 = down_cast<const Int32Column*>(ptr.get());
    for (int i = 0; i < res_col0->size(); ++i) {
        auto result = select_col.get_data()[i] ? col1.get_data()[i] : col2.get_data()[i];
        ASSERT_EQ(result, res_col0->get_data()[i]);
    }

    // Test fake Array works
    {
        expr_node.type = tttype_desc[1];
        auto expr0 = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_expr(expr_node));
        expr0->set_type(TYPE_INT_DESC);
        RandomValueExpr<TYPE_INT> col1(expr_node, chunk_size, e);
        RandomValueExpr<TYPE_INT> col2(expr_node, chunk_size, e);

        expr0->_children.push_back(&select_col);
        expr0->_children.push_back(&col1);
        expr0->_children.push_back(&col2);

        ColumnPtr ptr = expr0->evaluate(nullptr, nullptr);
        if (ptr->is_nullable()) {
            ptr = down_cast<const NullableColumn*>(ptr.get())->data_column();
        }
        const auto* res_col0 = down_cast<const Int32Column*>(ptr.get());
        for (int i = 0; i < res_col0->size(); ++i) {
            auto result = select_col.get_data()[i] ? col1.get_data()[i] : col2.get_data()[i];
            ASSERT_EQ(result, res_col0->get_data()[i]);
        }
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
    const auto* res_col1 = down_cast<const FloatColumn*>(ptr.get());
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
    const auto* res_col2 = down_cast<const Int8Column*>(ptr.get());
    for (int i = 0; i < res_col2->size(); ++i) {
        auto result = select_col.get_data()[i] ? col5.get_data()[i] : col6.get_data()[i];
        ASSERT_EQ(result, res_col2->get_data()[i]);
    }

    for (const auto& desc : tttype_desc1) {
        expr_node.type = desc;
        // Test INT8 var const
        auto expr3 = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_expr(expr_node));
        expr3->set_type(TYPE_TINYINT_DESC);
        RandomValueExpr<TYPE_TINYINT> col7(expr_node, chunk_size, e);
        MockConstVectorizedExpr<TYPE_TINYINT> col8(expr_node, 123);
        auto copyed_data = select_col.get_data();
        expr3->_children.push_back(&select_col);
        expr3->_children.push_back(&col7);
        expr3->_children.push_back(&col8);

        ptr = expr3->evaluate(nullptr, nullptr);
        const auto* res_col3 = down_cast<const Int8Column*>(ColumnHelper::get_data_column(ptr.get()));
        for (int i = 0; i < res_col3->size(); ++i) {
            auto result = copyed_data[i] ? col7.get_data()[i] : 123;
            ASSERT_EQ(result, res_col3->get_data()[i]);
        }

        {
            auto expr3 = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_expr(expr_node));
            expr3->set_type(TYPE_TINYINT_DESC);
            MockNullVectorizedExpr<TYPE_TINYINT> col7(expr_node, chunk_size, 1, true);
            MockConstVectorizedExpr<TYPE_TINYINT> col8(expr_node, 123);
            auto copyed_data = select_col.get_data();
            expr3->_children.push_back(&select_col);
            expr3->_children.push_back(&col7);
            expr3->_children.push_back(&col8);

            ptr = expr3->evaluate(nullptr, nullptr);
            const auto* res_col3 = down_cast<const Int8Column*>(ColumnHelper::get_data_column(ptr.get()));
            for (int i = 0; i < res_col3->size(); ++i) {
                if (copyed_data[i]) {
                    ASSERT_TRUE(ptr->is_null(i));
                } else {
                    ASSERT_EQ(123, res_col3->get_data()[i]);
                }
            }
        }

        // Test INT8 const var
        auto expr4 = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_expr(expr_node));
        expr4->set_type(TYPE_TINYINT_DESC);
        MockConstVectorizedExpr<TYPE_TINYINT> col9(expr_node, 123);
        RandomValueExpr<TYPE_TINYINT> col10(expr_node, chunk_size, e);
        copyed_data = select_col.get_data();
        expr4->_children.push_back(&select_col);
        expr4->_children.push_back(&col9);
        expr4->_children.push_back(&col10);

        ptr = expr4->evaluate(nullptr, nullptr);
        const auto* res_col4 = down_cast<const Int8Column*>(ColumnHelper::get_data_column(ptr.get()));
        for (int i = 0; i < res_col4->size(); ++i) {
            auto result = copyed_data[i] ? 123 : col10.get_data()[i];
            ASSERT_EQ(result, res_col4->get_data()[i]);
        }

        // Test INT8 const const

        // Test Nullable(INT8) var const
        {
            auto if_expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_expr(expr_node));
            if_expr->set_type(TYPE_TINYINT_DESC);

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
            auto if_expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_expr(expr_node));
            if_expr->set_type(TYPE_TINYINT_DESC);

            MakeNullableExpr<TYPE_TINYINT> nullable_selector(expr_node, chunk_size, &select_col);
            MockConstVectorizedExpr<TYPE_TINYINT> col_x(expr_node, 123);
            MockConstVectorizedExpr<TYPE_TINYINT> col_y(expr_node, 4);

            if_expr->_children.push_back(&nullable_selector);
            if_expr->_children.push_back(&col_x);
            if_expr->_children.push_back(&col_y);

            ptr = if_expr->evaluate(nullptr, nullptr);

            ColumnViewer<TYPE_BOOLEAN> sel_viewer(nullable_selector.get_col_ptr());
            const auto* res_x = down_cast<const Int8Column*>(ColumnHelper::get_data_column(ptr.get()));

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
}

TEST_F(VectorizedConditionExprTest, ifNullVariant) {
    expr_node.type = gen_type_desc(TPrimitiveType::VARIANT);
    auto expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_null_expr(expr_node));
    ASSERT_NE(nullptr, expr);

    MockExpr lhs(TypeDescriptor(TYPE_VARIANT), create_nullable_variant_column({"1", std::nullopt, "3", std::nullopt}));
    MockExpr rhs(TypeDescriptor(TYPE_VARIANT), create_variant_column({"10", "20", "30", "40"}));

    expr->_children.push_back(&lhs);
    expr->_children.push_back(&rhs);

    ColumnPtr result = expr->evaluate(nullptr, nullptr);
    assert_variant_result(result, {"1", "20", "3", "40"});
}

TEST_F(VectorizedConditionExprTest, nullIfVariant) {
    expr_node.type = gen_type_desc(TPrimitiveType::VARIANT);
    auto expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_null_if_expr(expr_node));
    ASSERT_NE(nullptr, expr);

    MockExpr lhs(TypeDescriptor(TYPE_VARIANT), create_variant_column({"1", "2", "3", "4"}));
    MockExpr rhs(TypeDescriptor(TYPE_VARIANT), create_variant_column({"1", "20", "3", "40"}));

    expr->_children.push_back(&lhs);
    expr->_children.push_back(&rhs);

    ColumnPtr result = expr->evaluate(nullptr, nullptr);
    assert_variant_result(result, {std::nullopt, "2", std::nullopt, "4"});
}

TEST_F(VectorizedConditionExprTest, ifVariant) {
    expr_node.type = gen_type_desc(TPrimitiveType::VARIANT);
    auto expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_expr(expr_node));
    ASSERT_NE(nullptr, expr);

    auto selector = BooleanColumn::create();
    selector->append(1);
    selector->append(0);
    selector->append(1);
    selector->append(0);
    MockExpr cond(TYPE_BOOLEAN_DESC, selector);
    MockExpr lhs(TypeDescriptor(TYPE_VARIANT), create_variant_column({"1", "2", "3", "4"}));
    MockExpr rhs(TypeDescriptor(TYPE_VARIANT), create_variant_column({"10", "20", "30", "40"}));

    expr->_children.push_back(&cond);
    expr->_children.push_back(&lhs);
    expr->_children.push_back(&rhs);

    ColumnPtr result = expr->evaluate(nullptr, nullptr);
    assert_variant_result(result, {"1", "20", "3", "40"});
}

TEST_F(VectorizedConditionExprTest, ifVariantWithNulls) {
    expr_node.type = gen_type_desc(TPrimitiveType::VARIANT);
    auto expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_expr(expr_node));
    ASSERT_NE(nullptr, expr);

    auto selector = BooleanColumn::create();
    selector->append(1);
    selector->append(0);
    selector->append(1);
    selector->append(0);
    MockExpr cond(TYPE_BOOLEAN_DESC, selector);
    MockExpr lhs(TypeDescriptor(TYPE_VARIANT), create_nullable_variant_column({std::nullopt, "2", "3", std::nullopt}));
    MockExpr rhs(TypeDescriptor(TYPE_VARIANT), create_nullable_variant_column({"10", std::nullopt, "30", "40"}));

    expr->_children.push_back(&cond);
    expr->_children.push_back(&lhs);
    expr->_children.push_back(&rhs);

    ColumnPtr result = expr->evaluate(nullptr, nullptr);
    assert_variant_result(result, {std::nullopt, std::nullopt, "3", "40"});
}

TEST_F(VectorizedConditionExprTest, coalesceVariant) {
    expr_node.type = gen_type_desc(TPrimitiveType::VARIANT);
    auto expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_coalesce_expr(expr_node));
    ASSERT_NE(nullptr, expr);

    MockExpr c0(TypeDescriptor(TYPE_VARIANT),
                create_nullable_variant_column({"1", std::nullopt, std::nullopt, std::nullopt}));
    MockExpr c1(TypeDescriptor(TYPE_VARIANT), create_nullable_variant_column({"11", "12", std::nullopt, std::nullopt}));
    MockExpr c2(TypeDescriptor(TYPE_VARIANT), create_nullable_variant_column({"21", "22", "23", std::nullopt}));

    expr->_children.push_back(&c0);
    expr->_children.push_back(&c1);
    expr->_children.push_back(&c2);

    ColumnPtr result = expr->evaluate(nullptr, nullptr);
    assert_variant_result(result, {"1", "12", "23", std::nullopt});
}

template <LogicalType Type>
static RunTimeCppType<Type> make_test_value_for_if(int seed) {
    using CppType = RunTimeCppType<Type>;
    if constexpr (Type == TYPE_BOOLEAN) {
        return static_cast<uint8_t>((seed & 1) != 0);
    } else if constexpr (Type == TYPE_DATE) {
        return DateValue{static_cast<int32_t>(DateValue::MIN_DATE_VALUE._julian + (seed % 100000))};
    } else if constexpr (Type == TYPE_DATETIME) {
        return TimestampValue{
                static_cast<int64_t>(TimestampValue::MIN_TIMESTAMP_VALUE._timestamp + int64_t(seed) * 1000)};
    } else if constexpr (Type == TYPE_FLOAT) {
        return static_cast<float>(seed) * 1.25f - 17.5f;
    } else if constexpr (Type == TYPE_DOUBLE) {
        return static_cast<double>(seed) * 2.5 - 35.125;
    } else {
        return static_cast<CppType>(seed);
    }
}

template <LogicalType Type>
static void verify_simd_selector_type() {
    using Container = typename SIMD_selector<Type>::Container;
    using CppType = typename SIMD_selector<Type>::CppType;

    // 1. Verify size == 0 does not dereference null const pointers in low-level SIMD helpers.
    {
        uint8_t dummy_sel = 0;
        uint8_t* sel_ptr = &dummy_sel;
        CppType dummy_dst{};
        CppType* dst_ptr = &dummy_dst;
        const CppType* null_a = nullptr;
        const CppType* null_b = nullptr;
#ifdef __AVX2__
        if constexpr (sizeof(CppType) == 1) {
            avx2_select_if<CppType, true, true>(sel_ptr, dst_ptr, null_a, null_b, 0);
        } else if constexpr (could_use_common_select_if<CppType>()) {
            avx2_select_if_common_implement<CppType, true, true>(sel_ptr, dst_ptr, null_a, null_b, 0);
        }
#elif defined(__ARM_NEON) && defined(__aarch64__)
        if constexpr (neon_could_use_common_select_if<CppType>()) {
            neon_select_if_common_implement<CppType, true, true>(sel_ptr, dst_ptr, null_a, null_b, 0);
        }
#endif
        EXPECT_EQ(&dummy_dst, dst_ptr);
    }

    const uint8_t truthy_bytes[3] = {0x02, 0x80, 0xFF};
    const uint8_t mixed_truthy[5] = {0x01, 0x02, 0x7F, 0x80, 0xFF};

    for (int n : {0, 1, 8, 15, 16, 17, 31, 32, 33, 48, 63, 64}) {
        // Pattern 0: all-zero
        // Pattern 1: all-nonzero with non-canonical truthy bytes (0x02, 0x80, 0xFF)
        // Pattern 2: sub-chunk homogeneous within a mixed batch (alternating all-0 and all-truthy sub-chunks)
        // Pattern 3: single active lane per sub-chunk (exercises every bit in data_mask including lane 15 0x8000)
        // Pattern 4: alternating mixed non-canonical truthy bytes
        for (int pat : {0, 1, 2, 3, 4}) {
            BooleanColumn::Container selector(n, 0);
            constexpr int sub_lanes = (sizeof(CppType) <= 8) ? (16 / sizeof(CppType)) : 1;
            for (int r = 0; r < n; ++r) {
                if (pat == 0) {
                    selector[r] = 0x00;
                } else if (pat == 1) {
                    selector[r] = truthy_bytes[r % 3];
                } else if (pat == 2) {
                    int sub_idx = (sub_lanes > 0) ? (r / sub_lanes) : r;
                    selector[r] = (sub_idx & 1) ? truthy_bytes[r % 3] : 0x00;
                } else if (pat == 3) {
                    int lane_in_16 = r % 16;
                    int target_lane = ((r / 16) * 7 + 15) % 16;
                    selector[r] = (lane_in_16 == target_lane) ? truthy_bytes[r % 3] : 0x00;
                } else {
                    selector[r] = ((r % 3) != 0) ? mixed_truthy[r % 5] : 0x00;
                }
            }

            Container a_vec(n);
            Container b_vec(n);
            for (int r = 0; r < n; ++r) {
                a_vec[r] = make_test_value_for_if<Type>(1000 + r * 3);
                b_vec[r] = make_test_value_for_if<Type>(5000 + r * 5);
            }
            const CppType a_const = make_test_value_for_if<Type>(777);
            const CppType b_const = make_test_value_for_if<Type>(888);

            // Verify exact batch multiples are processed 100% by the SIMD loop without falling back to the scalar tail.
#ifdef __AVX2__
            if (n > 0 && (n % 32 == 0)) {
                Container simd_dst(n);
                uint8_t* sel_ptr = selector.data();
                CppType* dst_ptr = simd_dst.data();
                const CppType* a_ptr = a_vec.data();
                const CppType* b_ptr = b_vec.data();
                if constexpr (sizeof(CppType) == 1) {
                    avx2_select_if(sel_ptr, dst_ptr, a_ptr, b_ptr, n);
                } else if constexpr (could_use_common_select_if<CppType>()) {
                    avx2_select_if_common_implement(sel_ptr, dst_ptr, a_ptr, b_ptr, n);
                }
                ASSERT_EQ(simd_dst.data() + n, dst_ptr)
                        << "AVX2 loop did not consume full batch multiple n=" << n << " type=" << Type;
            }
#elif defined(__ARM_NEON) && defined(__aarch64__)
            if (n > 0 && (n % 16 == 0)) {
                Container simd_dst(n);
                uint8_t* sel_ptr = selector.data();
                CppType* dst_ptr = simd_dst.data();
                const CppType* a_ptr = a_vec.data();
                const CppType* b_ptr = b_vec.data();
                if constexpr (neon_could_use_common_select_if<CppType>()) {
                    neon_select_if_common_implement(sel_ptr, dst_ptr, a_ptr, b_ptr, n);
                }
                ASSERT_EQ(simd_dst.data() + n, dst_ptr)
                        << "NEON loop did not consume full batch multiple n=" << n << " type=" << Type;
            }
#endif

            // Mode 0: var, var
            {
                Container dst(n, make_test_value_for_if<Type>(0));
                SIMD_selector<Type>::select_if(selector.data(), dst, a_vec, b_vec);
                for (int r = 0; r < n; ++r) {
                    CppType expected = (selector[r] != 0) ? a_vec[r] : b_vec[r];
                    ASSERT_EQ(expected, dst[r])
                            << "var/var type=" << Type << " n=" << n << " pat=" << pat << " r=" << r;
                }
            }
            // Mode 1: const, var
            {
                Container dst(n, make_test_value_for_if<Type>(0));
                SIMD_selector<Type>::select_if(selector.data(), dst, a_const, b_vec);
                for (int r = 0; r < n; ++r) {
                    CppType expected = (selector[r] != 0) ? a_const : b_vec[r];
                    ASSERT_EQ(expected, dst[r])
                            << "const/var type=" << Type << " n=" << n << " pat=" << pat << " r=" << r;
                }
            }
            // Mode 2: var, const
            {
                Container dst(n, make_test_value_for_if<Type>(0));
                SIMD_selector<Type>::select_if(selector.data(), dst, a_vec, b_const);
                for (int r = 0; r < n; ++r) {
                    CppType expected = (selector[r] != 0) ? a_vec[r] : b_const;
                    ASSERT_EQ(expected, dst[r])
                            << "var/const type=" << Type << " n=" << n << " pat=" << pat << " r=" << r;
                }
            }
            // Mode 3: const, const
            {
                Container dst(n, make_test_value_for_if<Type>(0));
                SIMD_selector<Type>::select_if(selector.data(), dst, a_const, b_const);
                for (int r = 0; r < n; ++r) {
                    CppType expected = (selector[r] != 0) ? a_const : b_const;
                    ASSERT_EQ(expected, dst[r])
                            << "const/const type=" << Type << " n=" << n << " pat=" << pat << " r=" << r;
                }
            }
        }
    }
}

TEST_F(VectorizedConditionExprTest, simdSelectorExactBatchMultiplesAndNonCanonicalBytes) {
    static_assert(can_use_simd_selector<TYPE_BOOLEAN>);
    static_assert(can_use_simd_selector<TYPE_INT>);
    static_assert(can_use_simd_selector<TYPE_DOUBLE>);
#if defined(__AVX2__) || (defined(__ARM_NEON) && defined(__aarch64__))
    static_assert(can_use_simd_selector<TYPE_DATE>);
    static_assert(can_use_simd_selector<TYPE_DATETIME>);
#endif

    // Directly test consolidated SIMDUtils primitives (load_selector_mask, expand_sub_mask, set_data).
#ifdef __AVX2__
    {
        alignas(32) uint8_t raw_sel[32] = {};
        raw_sel[0] = 0x02;
        raw_sel[15] = 0x80;
        raw_sel[16] = 0xFF;
        raw_sel[31] = 0x01;
        uint32_t mask = 0;
        __m256i mask_vec = SIMDUtils::load_selector_mask(raw_sel, &mask);
        EXPECT_EQ((1u << 0) | (1u << 15) | (1u << 16) | (1u << 31), mask);
        EXPECT_EQ(mask, static_cast<uint32_t>(_mm256_movemask_epi8(mask_vec)));

        // 2-byte expand_sub_mask: lane 0 and lane 15 (0x8001)
        __m256i exp16 = SIMDUtils::expand_sub_mask<2>(0x8001u);
        alignas(32) uint16_t lanes16[16] = {};
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(lanes16), exp16);
        EXPECT_EQ(0xFFFFu, lanes16[0]);
        EXPECT_EQ(0x0000u, lanes16[1]);
        EXPECT_EQ(0x0000u, lanes16[14]);
        EXPECT_EQ(0xFFFFu, lanes16[15]);

        // 4-byte expand_sub_mask: lane 0 and lane 7 (0x81)
        __m256i exp32 = SIMDUtils::expand_sub_mask<4>(0x81u);
        alignas(32) uint32_t lanes32[8] = {};
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(lanes32), exp32);
        EXPECT_EQ(0xFFFFFFFFu, lanes32[0]);
        EXPECT_EQ(0x00000000u, lanes32[1]);
        EXPECT_EQ(0xFFFFFFFFu, lanes32[7]);

        // 8-byte expand_sub_mask: lane 0 and lane 3 (0x09)
        __m256i exp64 = SIMDUtils::expand_sub_mask<8>(0x09u);
        alignas(32) uint64_t lanes64[4] = {};
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(lanes64), exp64);
        EXPECT_EQ(0xFFFFFFFFFFFFFFFFull, lanes64[0]);
        EXPECT_EQ(0x0000000000000000ull, lanes64[1]);
        EXPECT_EQ(0xFFFFFFFFFFFFFFFFull, lanes64[3]);
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    {
        uint8_t raw_sel[16] = {};
        raw_sel[0] = 0x02;
        raw_sel[7] = 0x80;
        raw_sel[15] = 0xFF;
        uint64_t nibble_mask = 0;
        uint8x16_t mask_vec = SIMDUtils::load_selector_mask(raw_sel, &nibble_mask);
        EXPECT_EQ((0xFull << 0) | (0xFull << 28) | (0xFull << 60), nibble_mask);
        uint8x16_t exp16 = SIMDUtils::expand_sub_mask<2>(mask_vec, 0);
        uint16_t lanes16[8] = {};
        vst1q_u16(lanes16, vreinterpretq_u16_u8(exp16));
        EXPECT_EQ(0xFFFFu, lanes16[0]);
        EXPECT_EQ(0x0000u, lanes16[1]);
        EXPECT_EQ(0xFFFFu, lanes16[7]);
    }
#endif

    verify_simd_selector_type<TYPE_BOOLEAN>();
    verify_simd_selector_type<TYPE_TINYINT>();
    verify_simd_selector_type<TYPE_SMALLINT>();
    verify_simd_selector_type<TYPE_INT>();
    verify_simd_selector_type<TYPE_BIGINT>();
    verify_simd_selector_type<TYPE_FLOAT>();
    verify_simd_selector_type<TYPE_DOUBLE>();
    verify_simd_selector_type<TYPE_DATE>();
    verify_simd_selector_type<TYPE_DATETIME>();
}

template <LogicalType Type>
static void verify_if_expr_date_or_datetime() {
    using CppType = RunTimeCppType<Type>;
    using ColumnType = RunTimeColumnType<Type>;

    TExprNode node;
    node.opcode = TExprOpcode::ADD;
    node.child_type = TPrimitiveType::INT;
    node.node_type = TExprNodeType::FUNCTION_CALL;
    node.num_children = 3;
    node.__isset.opcode = true;
    node.__isset.child_type = true;
    node.type = gen_type_desc(to_thrift(Type));

    const uint8_t truthy_bytes[3] = {0x02, 0x80, 0xFF};

    for (int n : {16, 32, 64, 65}) {
        // Mode 0: var/var non-null
        // Mode 1: const/var non-null
        // Mode 2: var/const non-null
        // Mode 3: const/const non-null
        // Mode 4: nullable lhs with unselected null at row 0 (verifies SIMD branch returns NullableColumn)
        // Mode 5: nullable cond (has_null() == false) with non-canonical truthy bytes (0x02, 0x80, 0xFF)
        // Mode 6: nullable cond (has_null() == true) with non-canonical truthy bytes (0x02, 0x80, 0xFF)
        for (int mode : {0, 1, 2, 3, 4, 5, 6}) {
            auto raw_cond_col = BooleanColumn::create();
            auto& cond_data = raw_cond_col->get_data();
            cond_data.resize(n);
            for (int r = 0; r < n; ++r) {
                if (mode == 4 && r == 0) {
                    cond_data[r] = 0x00;
                } else {
                    cond_data[r] = ((r % 3) != 0) ? truthy_bytes[r % 3] : 0x00;
                }
            }
            const auto expected_cond_data = cond_data;

            ColumnPtr cond_col = raw_cond_col;
            if (mode == 5 || mode == 6) {
                auto cond_nulls = NullColumn::create(n, 0);
                if (mode == 6) {
                    for (int r = 0; r < n; r += 5) {
                        cond_nulls->get_data()[r] = 1;
                    }
                }
                auto nullable_cond = NullableColumn::create(std::move(raw_cond_col), std::move(cond_nulls));
                nullable_cond->update_has_null();
                cond_col = std::move(nullable_cond);
            }

            const CppType lhs_const_val = make_test_value_for_if<Type>(777);
            const CppType rhs_const_val = make_test_value_for_if<Type>(888);

            ColumnPtr lhs_col;
            if (mode == 1 || mode == 3) {
                auto c = ColumnType::create();
                c->append(lhs_const_val);
                lhs_col = ConstColumn::create(std::move(c), n);
            } else {
                auto v = ColumnType::create();
                for (int r = 0; r < n; ++r) {
                    v->append(make_test_value_for_if<Type>(100 + r));
                }
                if (mode == 4) {
                    auto nulls = NullColumn::create(n, 0);
                    nulls->get_data()[0] = 1; // Null at row 0 (where cond_data[0] == 0)
                    lhs_col = NullableColumn::create(std::move(v), std::move(nulls));
                } else {
                    lhs_col = std::move(v);
                }
            }

            ColumnPtr rhs_col;
            if (mode == 2 || mode == 3) {
                auto c = ColumnType::create();
                c->append(rhs_const_val);
                rhs_col = ConstColumn::create(std::move(c), n);
            } else {
                auto v = ColumnType::create();
                for (int r = 0; r < n; ++r) {
                    v->append(make_test_value_for_if<Type>(500 + r));
                }
                rhs_col = std::move(v);
            }

            const TypeDescriptor type_desc(Type);
            auto expr = std::unique_ptr<Expr>(VectorizedConditionExprFactory::create_if_expr(node));
            expr->set_type(type_desc);
            MockExpr cond_expr(TYPE_BOOLEAN_DESC, cond_col);
            MockExpr lhs_expr(type_desc, lhs_col);
            MockExpr rhs_expr(type_desc, rhs_col);
            expr->_children.push_back(&cond_expr);
            expr->_children.push_back(&lhs_expr);
            expr->_children.push_back(&rhs_expr);

            ColumnPtr res = expr->evaluate(nullptr, nullptr);
            ASSERT_EQ(n, res->size());

#if defined(__AVX2__) || (defined(__ARM_NEON) && defined(__aarch64__))
            if (mode == 4) {
                // The SIMD nullable branch always wraps select_data and select_null in a NullableColumn,
                // whereas _evaluate_general<true> only builds a NullableColumn when a selected row is null.
                EXPECT_TRUE(res->is_nullable()) << "Expected SIMD nullable branch for type=" << Type;
            }
#endif

            ColumnViewer<Type> viewer(res);
            for (int r = 0; r < n; ++r) {
                ASSERT_FALSE(viewer.is_null(r)) << "mode=" << mode << " r=" << r;
                const bool cond_is_null = (mode == 6) && ((r % 5) == 0);
                const bool take_lhs = !cond_is_null && (expected_cond_data[r] != 0);
                CppType expected;
                if (take_lhs) {
                    expected = (mode == 1 || mode == 3) ? lhs_const_val : make_test_value_for_if<Type>(100 + r);
                } else {
                    expected = (mode == 2 || mode == 3) ? rhs_const_val : make_test_value_for_if<Type>(500 + r);
                }
                ASSERT_EQ(expected, viewer.value(r)) << "type=" << Type << " n=" << n << " mode=" << mode << " r=" << r;
            }
        }
    }
}

TEST_F(VectorizedConditionExprTest, ifExprDateAndDateTimeSimd) {
    verify_if_expr_date_or_datetime<TYPE_DATE>();
    verify_if_expr_date_or_datetime<TYPE_DATETIME>();
}

} // namespace starrocks
