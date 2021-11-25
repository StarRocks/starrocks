// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exprs/vectorized/case_expr.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <utility>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/fixed_length_column.h"
#include "common/object_pool.h"
#include "exprs/vectorized/mock_vectorized_expr.h"
#include "runtime/mem_pool.h"
#include "runtime/primitive_type.h"

namespace starrocks {
namespace vectorized {

template <PrimitiveType child_type>
struct VectorizedCaseExprTestBuilder {
    VectorizedCaseExprTestBuilder(bool has_else, bool has_case) {
        _has_else = has_else;
        _has_case = has_case;
        _expr.reset(VectorizedCaseExprFactory::from_thrift(case_when_node()));
    }

    template <template <PrimitiveType Type> typename T, typename... Args>
    VectorizedCaseExprTestBuilder& add_then(Args&&... args) {
        _then_expr_sz++;
        _expr->add_child(_obj_pool.add(new T<child_type>(mock_node(child_type), std::forward<Args>(args)...)));
        return *this;
    }

    template <template <PrimitiveType Type> typename T, typename... Args>
    VectorizedCaseExprTestBuilder& add_when(Args&&... args) {
        _when_expr_sz++;
        _expr->add_child(_obj_pool.add(new T<TYPE_BOOLEAN>(mock_node(TYPE_BOOLEAN), std::forward<Args>(args)...)));
        return *this;
    }

    Expr* build() {
        if (!_has_else) {
            DCHECK_EQ(_when_expr_sz, _then_expr_sz);
        } else {
            DCHECK_EQ(_when_expr_sz + 1, _then_expr_sz);
        }
        return _expr.release();
    }

private:
    TExprNode case_when_node() {
        TExprNode expr_node;
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = to_thrift(child_type);
        expr_node.node_type = TExprNodeType::CASE_EXPR;
        expr_node.num_children = 0;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(to_thrift(child_type));
        return expr_node;
    }

    TExprNode mock_node(PrimitiveType type) {
        TExprNode expr_node;
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = to_thrift(type);
        expr_node.node_type = TExprNodeType::FUNCTION_CALL;
        expr_node.num_children = 0;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(to_thrift(type));
        return expr_node;
    }

    bool _has_else;
    bool _has_case;

    int _when_expr_sz = 0;
    int _then_expr_sz = 0;

    std::unique_ptr<Expr> _expr;
    ObjectPool _obj_pool;
    MemPool _mem_pool;
};

class VectorizedCaseExprTest : public ::testing::Test {
public:
    void SetUp() {
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

TEST_F(VectorizedCaseExprTest, whenSliceCase) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::DATETIME);
    expr_node.case_expr.has_case_expr = true;
    expr_node.case_expr.has_else_expr = false;

    std::unique_ptr<Expr> expr(VectorizedCaseExprFactory::from_thrift(expr_node));

    std::string v1("test1");
    std::string v2("test2");
    std::string v3("test3");

    Slice s1(v1);
    Slice s2(v2);
    Slice s3(v3);

    MockVectorizedExpr<TYPE_VARCHAR> case1(expr_node, 10, s1);
    MockVectorizedExpr<TYPE_VARCHAR> when2(expr_node, 10, s2);
    MockVectorizedExpr<TYPE_DATETIME> then2(expr_node, 10, TimestampValue::create(2000, 12, 2, 12, 12, 30));
    MockVectorizedExpr<TYPE_VARCHAR> when3(expr_node, 10, s1);
    MockVectorizedExpr<TYPE_DATETIME> then3(expr_node, 10, TimestampValue::create(2002, 12, 2, 12, 12, 30));

    expr->_children.push_back(&case1);
    expr->_children.push_back(&when2);
    expr->_children.push_back(&then2);
    expr->_children.push_back(&when3);
    expr->_children.push_back(&then3);

    {
        Chunk chunk;
        ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
        ASSERT_TRUE(ptr->is_timestamp());

        auto v = ColumnHelper::cast_to_raw<TYPE_DATETIME>(ptr);
        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_EQ(TimestampValue::create(2002, 12, 2, 12, 12, 30), v->get_data()[j]);
        }
    }
}

TEST_F(VectorizedCaseExprTest, whenDecimalCase) {
    expr_node.child_type = TPrimitiveType::DECIMAL128;
    auto type_desc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 15, 4);
    expr_node.type = type_desc.to_thrift();
    expr_node.case_expr.has_case_expr = true;
    expr_node.case_expr.has_else_expr = false;

    std::unique_ptr<Expr> expr(VectorizedCaseExprFactory::from_thrift(expr_node));

    std::string case_v1("1234567890.1234");
    std::string case_v2("1234567890.1230");
    std::string then_v1("123456.9999");
    std::string then_v2("123456.8888");

    int128_t case_decimal1;
    int128_t case_decimal2;
    int64_t then_decimal1;
    int64_t then_decimal2;
    DecimalV3Cast::from_string<int128_t>(&case_decimal1, 33, 11, case_v1.c_str(), case_v1.size());
    DecimalV3Cast::from_string<int128_t>(&case_decimal2, 33, 11, case_v2.c_str(), case_v2.size());
    DecimalV3Cast::from_string<int64_t>(&then_decimal1, 15, 4, then_v1.c_str(), then_v1.size());
    DecimalV3Cast::from_string<int64_t>(&then_decimal2, 15, 4, then_v2.c_str(), then_v2.size());

    MockVectorizedExpr<TYPE_DECIMAL128> case_expr(expr_node, 10, case_decimal1);
    MockVectorizedExpr<TYPE_DECIMAL128> when2_expr(expr_node, 10, case_decimal2);
    MockVectorizedExpr<TYPE_DECIMAL64> then2_expr(expr_node, 10, then_decimal2);
    MockVectorizedExpr<TYPE_DECIMAL128> when1_expr(expr_node, 10, case_decimal1);
    MockVectorizedExpr<TYPE_DECIMAL64> then1_expr(expr_node, 10, then_decimal1);

    expr->_children.push_back(&case_expr);
    expr->_children.push_back(&when2_expr);
    expr->_children.push_back(&then2_expr);
    expr->_children.push_back(&when1_expr);
    expr->_children.push_back(&then1_expr);
    {
        Chunk chunk;
        ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
        ASSERT_TRUE(ptr->is_decimal());

        auto v = ColumnHelper::cast_to_raw<TYPE_DECIMAL64>(ptr);
        for (int i = 0; i < ptr->size(); ++i) {
            ASSERT_EQ(v->get_data()[i], then_decimal1);
        }
    }
}

TEST_F(VectorizedCaseExprTest, whenIntCaseAllNull) {
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.type = gen_type_desc(TPrimitiveType::INT);
    expr_node.case_expr.has_case_expr = true;
    expr_node.case_expr.has_else_expr = false;

    std::unique_ptr<Expr> expr(VectorizedCaseExprFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_INT> case1(expr_node, 10, 1);
    MockNullVectorizedExpr<TYPE_INT> when2(expr_node, 10, 2);
    MockVectorizedExpr<TYPE_INT> then2(expr_node, 10, 10);
    MockNullVectorizedExpr<TYPE_INT> when3(expr_node, 10, 1);
    MockVectorizedExpr<TYPE_INT> then3(expr_node, 10, 20);

    when2.only_null = true;
    when3.only_null = true;

    expr->_children.push_back(&case1);
    expr->_children.push_back(&when2);
    expr->_children.push_back(&then2);
    expr->_children.push_back(&when3);
    expr->_children.push_back(&then3);

    {
        Chunk chunk;
        ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
        ASSERT_TRUE(ptr->is_nullable());
        ASSERT_TRUE(ptr->only_null());
    }
}

TEST_F(VectorizedCaseExprTest, whenTimestampCaseElse) {
    expr_node.child_type = TPrimitiveType::DATETIME;
    expr_node.type = gen_type_desc(TPrimitiveType::INT);
    expr_node.case_expr.has_case_expr = true;
    expr_node.case_expr.has_else_expr = true;

    std::unique_ptr<Expr> expr(VectorizedCaseExprFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_DATETIME> case1(expr_node, 10, TimestampValue::create(2000, 12, 2, 12, 12, 30));
    MockVectorizedExpr<TYPE_DATETIME> when2(expr_node, 10, TimestampValue::create(2001, 12, 2, 12, 12, 30));
    MockVectorizedExpr<TYPE_INT> then2(expr_node, 10, 1);
    MockVectorizedExpr<TYPE_DATETIME> when3(expr_node, 10, TimestampValue::create(2002, 12, 2, 12, 12, 30));
    MockVectorizedExpr<TYPE_INT> then3(expr_node, 10, 2);
    MockVectorizedExpr<TYPE_INT> else1(expr_node, 10, 3);

    expr->_children.push_back(&case1);
    expr->_children.push_back(&when2);
    expr->_children.push_back(&then2);
    expr->_children.push_back(&when3);
    expr->_children.push_back(&then3);
    expr->_children.push_back(&else1);

    {
        Chunk chunk;
        ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
        ASSERT_TRUE(ptr->is_numeric());

        auto v = ColumnHelper::cast_to_raw<TYPE_INT>(ptr);
        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_EQ(3, v->get_data()[j]);
        }
    }
}

TEST_F(VectorizedCaseExprTest, whenNullIntCaseElse) {
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
    expr_node.case_expr.has_case_expr = true;
    expr_node.case_expr.has_else_expr = true;

    std::unique_ptr<Expr> expr(VectorizedCaseExprFactory::from_thrift(expr_node));

    std::string v1("test1");
    std::string v2("test2");
    std::string v3("test3");

    Slice s1(v1);
    Slice s2(v2);
    Slice s3(v3);

    MockNullVectorizedExpr<TYPE_INT> case1(expr_node, 10, 1);
    MockVectorizedExpr<TYPE_INT> when2(expr_node, 10, 2);
    MockVectorizedExpr<TYPE_VARCHAR> then2(expr_node, 10, s1);
    MockVectorizedExpr<TYPE_INT> when3(expr_node, 10, 1);
    MockVectorizedExpr<TYPE_VARCHAR> then3(expr_node, 10, s2);
    MockVectorizedExpr<TYPE_VARCHAR> else1(expr_node, 10, s3);

    expr->_children.push_back(&case1);
    expr->_children.push_back(&when2);
    expr->_children.push_back(&then2);
    expr->_children.push_back(&when3);
    expr->_children.push_back(&then3);
    expr->_children.push_back(&else1);

    {
        Chunk chunk;
        ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
        ASSERT_TRUE(ptr->is_binary());

        auto v = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(ptr);
        for (int j = 0; j < ptr->size(); ++j) {
            if (j % 2) {
                ASSERT_FALSE(ptr->is_null(j));
                ASSERT_EQ(s3, v->get_data()[j]);
            } else {
                ASSERT_FALSE(ptr->is_null(j));
                ASSERT_EQ(s2, v->get_data()[j]);
            }
        }
    }
}

TEST_F(VectorizedCaseExprTest, whenIntCaseNullElse) {
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.type = gen_type_desc(TPrimitiveType::INT);
    expr_node.case_expr.has_case_expr = true;
    expr_node.case_expr.has_else_expr = true;

    std::unique_ptr<Expr> expr(VectorizedCaseExprFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_INT> case1(expr_node, 10, 1);
    MockNullVectorizedExpr<TYPE_INT> when2(expr_node, 10, 2);
    MockVectorizedExpr<TYPE_INT> then2(expr_node, 10, 10);
    MockNullVectorizedExpr<TYPE_INT> when3(expr_node, 10, 1);
    MockVectorizedExpr<TYPE_INT> then3(expr_node, 10, 20);
    MockVectorizedExpr<TYPE_INT> else1(expr_node, 10, 30);

    when2.only_null = true;

    expr->_children.push_back(&case1);
    expr->_children.push_back(&when2);
    expr->_children.push_back(&then2);
    expr->_children.push_back(&when3);
    expr->_children.push_back(&then3);
    expr->_children.push_back(&else1);

    {
        Chunk chunk;
        ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
        ASSERT_TRUE(ptr->is_numeric());

        auto v = ColumnHelper::cast_to_raw<TYPE_INT>(ptr);
        for (int j = 0; j < ptr->size(); ++j) {
            if (j % 2) {
                ASSERT_EQ(30, v->get_data()[j]);
            } else {
                ASSERT_EQ(20, v->get_data()[j]);
            }
        }
    }
}

TEST_F(VectorizedCaseExprTest, WhenNoCaseIntNullElse) {
    {
        // No CASE No ELSE
        VectorizedCaseExprTestBuilder<TYPE_INT> builder(false, false);
        builder.add_when<MockVectorizedExpr>(10, uint8_t(true));
        builder.add_then<MockVectorizedExpr>(10, 10);
        std::unique_ptr<Expr> expr(builder.build());

        Chunk chunk;
        ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
        ASSERT_TRUE(ptr->is_numeric() && !ptr->is_nullable());
    }
    {
        // No CASE Has ELSE
        VectorizedCaseExprTestBuilder<TYPE_INT> builder(true, false);
        builder.add_when<MockVectorizedExpr>(10, uint8_t(false));
        builder.add_then<MockVectorizedExpr>(10, 10);
        builder.add_then<MockNullVectorizedExpr>(10, 40);

        std::unique_ptr<Expr> expr(builder.build());
        Chunk chunk;
        ColumnPtr ptr = expr->evaluate(nullptr, &chunk);

        ColumnViewer<TYPE_INT> viewer(ptr);

        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_TRUE(!viewer.is_null(j));
            ASSERT_EQ(40, viewer.value(j));
        }
    }
    {
        // No CASE Has always NULL ELSE
        VectorizedCaseExprTestBuilder<TYPE_INT> builder(true, false);
        builder.add_when<MockVectorizedExpr>(10, uint8_t(false));
        builder.add_then<MockVectorizedExpr>(10, 10);
        builder.add_then<MockNullVectorizedExpr>(10, 0, true); // only_null

        std::unique_ptr<Expr> expr(builder.build());
        Chunk chunk;
        ColumnPtr ptr = expr->evaluate(nullptr, &chunk);

        ColumnViewer<TYPE_INT> viewer(ptr);
        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_TRUE(viewer.is_null(j));
        }
    }
}

TEST_F(VectorizedCaseExprTest, whenConstantAndElseVariable) {
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.type = gen_type_desc(TPrimitiveType::INT);
    expr_node.case_expr.has_case_expr = true;
    expr_node.case_expr.has_else_expr = true;

    std::unique_ptr<Expr> expr(VectorizedCaseExprFactory::from_thrift(expr_node));

    MockConstVectorizedExpr<TYPE_INT> case1(expr_node, 1);
    MockConstVectorizedExpr<TYPE_INT> when2(expr_node, 2);
    MockVectorizedExpr<TYPE_INT> then2(expr_node, 10, 10);
    MockVectorizedExpr<TYPE_INT> else1(expr_node, 10, 20);

    expr->_children.push_back(&case1);
    expr->_children.push_back(&when2);
    expr->_children.push_back(&then2);
    expr->_children.push_back(&else1);

    {
        Chunk chunk;
        ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
        ASSERT_TRUE(ptr->is_numeric());

        auto v = ColumnHelper::cast_to_raw<TYPE_INT>(ptr);
        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_EQ(20, v->get_data()[j]);
        }
    }
}

TEST_F(VectorizedCaseExprTest, whenIntCaseAllNullElse) {
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.type = gen_type_desc(TPrimitiveType::INT);
    expr_node.case_expr.has_case_expr = true;
    expr_node.case_expr.has_else_expr = true;

    std::unique_ptr<Expr> expr(VectorizedCaseExprFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_INT> case1(expr_node, 10, 1);
    MockNullVectorizedExpr<TYPE_INT> when2(expr_node, 10, 2);
    MockVectorizedExpr<TYPE_INT> then2(expr_node, 10, 10);
    MockNullVectorizedExpr<TYPE_INT> when3(expr_node, 10, 1);
    MockVectorizedExpr<TYPE_INT> then3(expr_node, 10, 20);
    MockVectorizedExpr<TYPE_INT> else1(expr_node, 10, 30);

    when2.only_null = true;
    when3.only_null = true;

    expr->_children.push_back(&case1);
    expr->_children.push_back(&when2);
    expr->_children.push_back(&then2);
    expr->_children.push_back(&when3);
    expr->_children.push_back(&then3);
    expr->_children.push_back(&else1);

    {
        Chunk chunk;
        ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
        ASSERT_TRUE(ptr->is_numeric());

        auto v = ColumnHelper::cast_to_raw<TYPE_INT>(ptr);
        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_EQ(30, v->get_data()[j]);
        }
    }
}

TEST_F(VectorizedCaseExprTest, NoCaseReturnInt) {
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.type = gen_type_desc(TPrimitiveType::INT);
    expr_node.case_expr.has_case_expr = false;
    expr_node.case_expr.has_else_expr = false;

    std::unique_ptr<Expr> expr(VectorizedCaseExprFactory::from_thrift(expr_node));

    MockVectorizedExpr<TYPE_BOOLEAN> when2(expr_node, 10, true);
    MockVectorizedExpr<TYPE_INT> then2(expr_node, 10, 10);
    MockVectorizedExpr<TYPE_BOOLEAN> when3(expr_node, 10, false);
    MockVectorizedExpr<TYPE_INT> then3(expr_node, 10, 20);

    expr->_children.push_back(&when2);
    expr->_children.push_back(&then2);
    expr->_children.push_back(&when3);
    expr->_children.push_back(&then3);

    {
        Chunk chunk;
        ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
        ASSERT_TRUE(ptr->is_numeric());

        auto v = ColumnHelper::cast_to_raw<TYPE_INT>(ptr);
        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_EQ(10, v->get_data()[j]);
        }
    }
}

TEST_F(VectorizedCaseExprTest, NoCaseAllNull) {
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.type = gen_type_desc(TPrimitiveType::INT);
    expr_node.case_expr.has_case_expr = false;
    expr_node.case_expr.has_else_expr = false;

    std::unique_ptr<Expr> expr(VectorizedCaseExprFactory::from_thrift(expr_node));

    MockNullVectorizedExpr<TYPE_BOOLEAN> when2(expr_node, 10, true);
    MockVectorizedExpr<TYPE_INT> then2(expr_node, 10, 10);
    MockNullVectorizedExpr<TYPE_BOOLEAN> when3(expr_node, 10, false);
    MockVectorizedExpr<TYPE_INT> then3(expr_node, 10, 20);

    when2.only_null = true;
    when3.only_null = true;

    expr->_children.push_back(&when2);
    expr->_children.push_back(&then2);
    expr->_children.push_back(&when3);
    expr->_children.push_back(&then3);

    {
        Chunk chunk;
        ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
        ASSERT_TRUE(ptr->only_null());
    }
}

TEST_F(VectorizedCaseExprTest, NoCaseWhenNullReturnIntElse) {
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.type = gen_type_desc(TPrimitiveType::INT);
    expr_node.case_expr.has_case_expr = false;
    expr_node.case_expr.has_else_expr = true;

    std::unique_ptr<Expr> expr(VectorizedCaseExprFactory::from_thrift(expr_node));

    MockNullVectorizedExpr<TYPE_BOOLEAN> when2(expr_node, 10, true);
    MockVectorizedExpr<TYPE_INT> then2(expr_node, 10, 10);
    MockVectorizedExpr<TYPE_BOOLEAN> when3(expr_node, 10, false);
    MockVectorizedExpr<TYPE_INT> then3(expr_node, 10, 20);
    MockVectorizedExpr<TYPE_INT> else1(expr_node, 10, 30);

    expr->_children.push_back(&when2);
    expr->_children.push_back(&then2);
    expr->_children.push_back(&when3);
    expr->_children.push_back(&then3);
    expr->_children.push_back(&else1);

    {
        Chunk chunk;
        ColumnPtr ptr = expr->evaluate(nullptr, &chunk);
        ASSERT_TRUE(ptr->is_numeric());

        auto v = ColumnHelper::cast_to_raw<TYPE_INT>(ptr);
        for (int j = 0; j < ptr->size(); ++j) {
            if (j % 2) {
                ASSERT_EQ(30, v->get_data()[j]);
            } else {
                ASSERT_EQ(10, v->get_data()[j]);
            }
        }
    }
}

} // namespace vectorized
} // namespace starrocks
