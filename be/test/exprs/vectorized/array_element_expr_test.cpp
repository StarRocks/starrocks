// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exprs/vectorized/array_element_expr.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "column/array_column.h"
#include "column/column_helper.h"

namespace starrocks::vectorized {

namespace {

class FakeConstExpr : public starrocks::Expr {
public:
    explicit FakeConstExpr(const TExprNode& dummy) : Expr(dummy) {}

    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override { return _column; }

    Expr* clone(ObjectPool*) const override { return nullptr; }

    ColumnPtr _column;
};

ColumnPtr const_int_column(int32_t value, size_t size = 1) {
    auto data = Int32Column::create();
    data->append(value);
    return ConstColumn::create(std::move(data), size);
}

std::unique_ptr<Expr> create_array_element_expr(const TTypeDesc& type) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::ARRAY_ELEMENT_EXPR);
    node.__set_is_nullable(true);
    node.__set_type(type);
    node.__set_num_children(0);

    auto* expr = ArrayElementExprFactory::from_thrift(node);
    return std::unique_ptr<Expr>(expr);
}

std::unique_ptr<Expr> create_array_element_expr(const TypeDescriptor& type) {
    return create_array_element_expr(type.to_thrift());
}

} // anonymous namespace

class ArrayElementExprTest : public ::testing::Test {
protected:
    void SetUp() override {}
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

private:
    ObjectPool _objpool;
};

// NOLINTNEXTLINE
TEST_F(ArrayElementExprTest, test_one_dim_array) {
    TypeDescriptor type_array_int;
    type_array_int.type = PrimitiveType::TYPE_ARRAY;
    type_array_int.children.emplace_back(TypeDescriptor(PrimitiveType::TYPE_INT));

    TypeDescriptor type_int(PrimitiveType::TYPE_INT);

    auto array = ColumnHelper::create_column(type_array_int, true);
    array->append_datum(DatumArray{Datum((int32_t)1)});                    // [1]
    array->append_datum(DatumArray{});                                     // []
    array->append_datum(DatumArray{Datum((int32_t)3), Datum((int32_t)4)}); // [3,4]
    array->append_datum(Datum{});
    array->append_datum(DatumArray{Datum(), Datum((int32_t)5)}); // [NULL, 5]

    // Inputs:
    //   c0
    // --------
    //   [1]
    //   []
    //   [3, 4]
    //   NULL
    //   [NULL, 5]
    //
    // Query:
    //   select c0[1]
    //
    // Outputs:
    //   1
    //   NULL
    //   3
    //   NULL
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_array_element_expr(type_int);

        expr->add_child(new_fake_const_expr(array, type_array_int));
        expr->add_child(new_fake_const_expr(const_int_column(1), type_int));

        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(5, result->size());
        EXPECT_FALSE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_FALSE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
        EXPECT_TRUE(result->is_null(4));

        EXPECT_EQ(1, result->get(0).get_int32());
        EXPECT_TRUE(result->get(1).is_null());
        EXPECT_EQ(3, result->get(2).get_int32());
        EXPECT_TRUE(result->get(3).is_null());
        EXPECT_TRUE(result->get(4).is_null());
    }

    // Inputs:
    //   c0
    // --------
    //   [1]
    //   []
    //   [3, 4]
    //   NULL
    //   [NULL, 5]
    //
    // Query:
    //   select c0[0]
    //
    // Outputs:
    //   NULL
    //   NULL
    //   NULL
    //   NULL
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_array_element_expr(type_int);

        expr->add_child(new_fake_const_expr(array, type_array_int));
        expr->add_child(new_fake_const_expr(const_int_column(0), type_int));

        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(5, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
        EXPECT_TRUE(result->is_null(4));
    }

    // Inputs:
    //   c0
    // --------
    //   [1]
    //   []
    //   [3, 4]
    //   NULL
    //   [NULL, 5]
    //
    // Query:
    //   select c0[-1]
    //
    // Outputs:
    //   NULL
    //   NULL
    //   NULL
    //   NULL
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_array_element_expr(type_int);

        expr->add_child(new_fake_const_expr(array, type_array_int));
        expr->add_child(new_fake_const_expr(const_int_column(-1), type_int));

        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(5, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
        EXPECT_TRUE(result->is_null(4));
    }

    // Inputs:
    //   c0
    // --------
    //   [1]
    //   []
    //   [3, 4]
    //   NULL
    //   [NULL, 5]
    //
    // Query:
    //   select c0[NULL]
    //
    // Outputs:
    //   NULL
    //   NULL
    //   NULL
    //   NULL
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_array_element_expr(type_int);

        auto subscript = NullableColumn::create(Int32Column::create(), NullColumn::create());
        (void)subscript->append_nulls(5);

        expr->add_child(new_fake_const_expr(array, type_array_int));
        expr->add_child(new_fake_const_expr(subscript, type_int));

        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(5, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
        EXPECT_TRUE(result->is_null(4));
    }

    // Inputs:
    //   c0
    // -------
    //   []
    //
    // Query:
    //   select c0[1]
    //
    // Outputs:
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_array_element_expr(type_int);

        auto array1 = ArrayColumn::create(Int32Column::create(), UInt32Column::create());
        array1->append_datum(Datum(DatumArray()));

        expr->add_child(new_fake_const_expr(array1, type_array_int));
        expr->add_child(new_fake_const_expr(const_int_column(1), type_int));

        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }

    // Inputs:
    //   c0
    // -------
    //   []
    //
    // Query:
    //   select c0[100]
    //
    // Outputs:
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_array_element_expr(type_int);

        auto array1 = ArrayColumn::create(Int32Column::create(), UInt32Column::create());
        array1->append_datum(Datum(DatumArray()));

        expr->add_child(new_fake_const_expr(array1, type_array_int));
        expr->add_child(new_fake_const_expr(const_int_column(100), type_int));

        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }

    // Inputs:
    //   c0
    // -------
    //   []
    //
    // Query:
    //   select c0[0]
    //
    // Outputs:
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_array_element_expr(type_int);

        auto array1 = ArrayColumn::create(Int32Column::create(), UInt32Column::create());
        array1->append_datum(Datum(DatumArray()));

        expr->add_child(new_fake_const_expr(array1, type_array_int));
        expr->add_child(new_fake_const_expr(const_int_column(0), type_int));

        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(1, result->size());
        EXPECT_TRUE(result->is_null(0));
    }
}

// NOLINTNEXTLINE
TEST_F(ArrayElementExprTest, test_two_dim_array) {
    TypeDescriptor type_desc;
    type_desc.type = PrimitiveType::TYPE_ARRAY;
    type_desc.children.emplace_back();
    type_desc.children.back().type = PrimitiveType::TYPE_ARRAY;
    type_desc.children.back().children.emplace_back();
    type_desc.children.back().children.back().type = PrimitiveType::TYPE_INT;

    TypeDescriptor type_int(PrimitiveType::TYPE_INT);

    //
    //  array:
    // ------------
    //  []
    //  [[1,2],[3]]
    //  [NULL, [4,5,6]]
    //  NULL
    auto array = ColumnHelper::create_column(type_desc, true);
    array->append_datum(DatumArray{});
    array->append_datum(DatumArray{DatumArray{(int32_t)1, (int32_t)2}, DatumArray{(int32_t)3}});
    array->append_datum(DatumArray{Datum{}, DatumArray{(int32_t)4, (int32_t)5, (int32_t)6}});
    array->append_datum(Datum{});

    // select array[NULL]
    //
    // Expect:
    //   NULL
    //   NULL
    //   NULL
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_array_element_expr(type_desc.children[0]);

        auto subscript = NullableColumn::create(Int32Column::create(), NullColumn::create());
        (void)subscript->append_nulls(4);

        expr->add_child(new_fake_const_expr(array, type_desc));
        expr->add_child(new_fake_const_expr(subscript, type_int));

        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    // select array[-1]
    //
    // Expect:
    //   NULL
    //   NULL
    //   NULL
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_array_element_expr(type_desc.children[0]);

        expr->add_child(new_fake_const_expr(array, type_desc));
        expr->add_child(new_fake_const_expr(const_int_column(-1), type_int));

        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    // select array[0]
    //
    // Expect:
    //   NULL
    //   NULL
    //   NULL
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_array_element_expr(type_desc.children[0]);

        expr->add_child(new_fake_const_expr(array, type_desc));
        expr->add_child(new_fake_const_expr(const_int_column(0), type_int));

        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    // select array[1]
    //
    // Expect:
    //   NULL
    //   [1,2]
    //   NULL
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_array_element_expr(type_desc.children[0]);

        expr->add_child(new_fake_const_expr(array, type_desc));
        expr->add_child(new_fake_const_expr(const_int_column(1), type_int));

        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_FALSE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));

        // [1,2]
        EXPECT_EQ(2, result->get(1).get_array().size());
        EXPECT_EQ(1, result->get(1).get_array()[0].get_int32());
        EXPECT_EQ(2, result->get(1).get_array()[1].get_int32());
    }

    // select array[2]
    //
    // Expect:
    //   NULL
    //   [3]
    //   [4,5,6]
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_array_element_expr(type_desc.children[0]);

        expr->add_child(new_fake_const_expr(array, type_desc));
        expr->add_child(new_fake_const_expr(const_int_column(2), type_int));

        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_FALSE(result->is_null(1));
        EXPECT_FALSE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));

        // [3]
        EXPECT_EQ(1, result->get(1).get_array().size());
        EXPECT_EQ(3, result->get(1).get_array()[0].get_int32());

        // [4,5,6]
        EXPECT_EQ(3, result->get(2).get_array().size());
        EXPECT_EQ(4, result->get(2).get_array()[0].get_int32());
        EXPECT_EQ(5, result->get(2).get_array()[1].get_int32());
        EXPECT_EQ(6, result->get(2).get_array()[2].get_int32());
    }

    // select array[3]
    //
    // Expect:
    //   NULL
    //   NULL
    //   NULL
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_array_element_expr(type_desc.children[0]);

        expr->add_child(new_fake_const_expr(array, type_desc));
        expr->add_child(new_fake_const_expr(const_int_column(3), type_int));

        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }

    // select array[100000]
    //
    // Expect:
    //   NULL
    //   NULL
    //   NULL
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_array_element_expr(type_desc.children[0]);

        expr->add_child(new_fake_const_expr(array, type_desc));
        expr->add_child(new_fake_const_expr(const_int_column(100000), type_int));

        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }
    // select array[-100000]
    //
    // Expect:
    //   NULL
    //   NULL
    //   NULL
    //   NULL
    {
        std::unique_ptr<Expr> expr = create_array_element_expr(type_desc.children[0]);

        expr->add_child(new_fake_const_expr(array, type_desc));
        expr->add_child(new_fake_const_expr(const_int_column(-100000), type_int));

        auto result = expr->evaluate(nullptr, nullptr);
        EXPECT_TRUE(result->is_nullable());
        EXPECT_EQ(4, result->size());
        EXPECT_TRUE(result->is_null(0));
        EXPECT_TRUE(result->is_null(1));
        EXPECT_TRUE(result->is_null(2));
        EXPECT_TRUE(result->is_null(3));
    }
}

} // namespace starrocks::vectorized
