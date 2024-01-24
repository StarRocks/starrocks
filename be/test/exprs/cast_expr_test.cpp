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

#include "exprs/cast_expr.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <limits>

#include "butil/time.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/mock_vectorized_expr.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/datetime_value.h"
#include "runtime/runtime_state.h"
#include "runtime/time_types.h"
#include "testutil/exprs_test_helper.h"
#include "types/date_value.h"
#include "types/logical_type.h"
#include "types/timestamp_value.h"
#include "util/json.h"
#include "util/slice.h"

namespace starrocks {

class VectorizedCastExprTest : public ::testing::Test {
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
    RuntimeState runtime_state;
    TExprNode expr_node;
};

TEST_F(VectorizedCastExprTest, IntCastToDate) {
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.type = gen_type_desc(TPrimitiveType::DATE);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 20111101);

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_date());

        auto v = ColumnHelper::cast_to_raw<TYPE_DATE>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(DateValue::create(2011, 11, 01), v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, BigIntCastToTimestamp) {
    expr_node.child_type = TPrimitiveType::BIGINT;
    expr_node.type = gen_type_desc(TPrimitiveType::DATETIME);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 20220203112345);

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_timestamp());

        auto v = std::static_pointer_cast<TimestampColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(TimestampValue::create(2022, 02, 03, 11, 23, 45), v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, BigIntCastToTimestampError) {
    expr_node.child_type = TPrimitiveType::BIGINT;
    expr_node.type = gen_type_desc(TPrimitiveType::DATETIME);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 20220003112345);

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ASSERT_TRUE(ptr->is_nullable());

        ASSERT_FALSE(ptr->is_timestamp());

        ASSERT_TRUE(ColumnHelper::as_raw_column<NullableColumn>(ptr)->data_column()->is_timestamp());

        for (int j = 0; j < ptr->size(); ++j) {
            ASSERT_TRUE(ptr->is_null(j));
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<TimestampColumn>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, dateCastToBoolean) {
    expr_node.child_type = TPrimitiveType::DATE;
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_DATE> col1(expr_node, 10, DateValue::create(123123, 1, 1));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_numeric());

        // right cast
        auto v = std::static_pointer_cast<BooleanColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(true, v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, timestampCastToBoolean) {
    expr_node.child_type = TPrimitiveType::DATETIME;
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_DATETIME> col1(expr_node, 10, TimestampValue::create(12, 1, 1, 25, 1, 1));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_numeric());

        // right cast
        auto v = std::static_pointer_cast<BooleanColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(true, v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, stringLiteralTrueCastToBoolean) {
    expr_node.child_type = TPrimitiveType::CHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string s = "true";
    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_CHAR> col1(expr_node, 10, Slice(s));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_numeric());

        // right cast
        auto v = std::static_pointer_cast<BooleanColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(true, v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, stringLiteralFalseCastToBoolean) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string s = "false";
    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_CHAR> col1(expr_node, 10, Slice(s));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_numeric());

        // right cast
        auto v = std::static_pointer_cast<BooleanColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(false, v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, stringLiteralIntCastToBoolean) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string s = "1";
    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_CHAR> col1(expr_node, 10, Slice(s));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_numeric());

        // right cast
        auto v = std::static_pointer_cast<BooleanColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(true, v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

template <LogicalType FromType, LogicalType ToType>
static void numeric_cast_with_jit(RuntimeState* runtime_state, TExprNode& cast_expr) {
    ObjectPool pool;
    typedef RunTimeCppType<FromType> FromCppType;
    typedef RunTimeCppType<ToType> ToCppType;
    auto max = std::numeric_limits<FromCppType>::max();
    auto min = std::numeric_limits<FromCppType>::min();
    FromCppType data[] = {0,
                          1,
                          -1,
                          max,
                          min,
                          static_cast<FromCppType>(max - 1),
                          static_cast<FromCppType>(min + 1),
                          static_cast<FromCppType>(max / 2),
                          static_cast<FromCppType>(min / 2),
                          static_cast<FromCppType>(max / 2 + 1),
                          static_cast<FromCppType>(min / 2 - 1)};
    cast_expr.child_type = to_thrift(FromType);
    cast_expr.type = gen_type_desc(to_thrift(ToType));
    if constexpr (std::numeric_limits<ToCppType>::max() < std::numeric_limits<FromCppType>::max()) {
        cast_expr.is_nullable = true;
    } else {
        cast_expr.is_nullable = false;
    }

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(&pool, cast_expr));

    cast_expr.type = gen_type_desc(cast_expr.child_type);

    for (auto& d : data) {
        MockVectorizedExpr<FromType> col1(cast_expr, 1, d);
        expr->_children.push_back(&col1);

        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_result_with_jit(ptr, expr.get(), runtime_state);
    }
}

TEST_F(VectorizedCastExprTest, numericJITCast) {
    numeric_cast_with_jit<TYPE_TINYINT, TYPE_TINYINT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_TINYINT, TYPE_SMALLINT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_TINYINT, TYPE_INT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_TINYINT, TYPE_BIGINT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_TINYINT, TYPE_FLOAT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_TINYINT, TYPE_DOUBLE>(&runtime_state, expr_node);

    numeric_cast_with_jit<TYPE_SMALLINT, TYPE_TINYINT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_SMALLINT, TYPE_SMALLINT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_SMALLINT, TYPE_INT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_SMALLINT, TYPE_BIGINT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_SMALLINT, TYPE_FLOAT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_SMALLINT, TYPE_DOUBLE>(&runtime_state, expr_node);

    numeric_cast_with_jit<TYPE_INT, TYPE_TINYINT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_INT, TYPE_SMALLINT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_INT, TYPE_INT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_INT, TYPE_BIGINT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_INT, TYPE_FLOAT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_INT, TYPE_DOUBLE>(&runtime_state, expr_node);

    numeric_cast_with_jit<TYPE_BIGINT, TYPE_TINYINT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_BIGINT, TYPE_SMALLINT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_BIGINT, TYPE_INT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_BIGINT, TYPE_BIGINT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_BIGINT, TYPE_FLOAT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_BIGINT, TYPE_DOUBLE>(&runtime_state, expr_node);

    numeric_cast_with_jit<TYPE_FLOAT, TYPE_TINYINT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_FLOAT, TYPE_SMALLINT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_FLOAT, TYPE_INT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_FLOAT, TYPE_BIGINT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_FLOAT, TYPE_FLOAT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_FLOAT, TYPE_DOUBLE>(&runtime_state, expr_node);

    numeric_cast_with_jit<TYPE_DOUBLE, TYPE_TINYINT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_DOUBLE, TYPE_SMALLINT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_DOUBLE, TYPE_INT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_DOUBLE, TYPE_BIGINT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_DOUBLE, TYPE_FLOAT>(&runtime_state, expr_node);
    numeric_cast_with_jit<TYPE_DOUBLE, TYPE_DOUBLE>(&runtime_state, expr_node);
}

TEST_F(VectorizedCastExprTest, intCastSelfExpr) {
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.type = gen_type_desc(TPrimitiveType::INT);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 10);

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_TRUE(ptr->is_numeric());

            // right cast
            auto v = std::static_pointer_cast<Int32Column>(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(10, v->get_data()[j]);
            }

            // error cast
            ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
        });
    }
}

TEST_F(VectorizedCastExprTest, intToFloatCastExpr) {
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.type = gen_type_desc(TPrimitiveType::FLOAT);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 10);

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_TRUE(ptr->is_numeric());

            // right cast
            auto v = std::static_pointer_cast<FloatColumn>(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(10, v->get_data()[j]);
            }

            // error cast
            ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
        });
    }
}

TEST_F(VectorizedCastExprTest, intToInt8CastExpr) {
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.type = gen_type_desc(TPrimitiveType::TINYINT);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 10);

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_TRUE(ptr->is_numeric());

            // right cast
            auto v = std::static_pointer_cast<Int8Column>(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(10, v->get_data()[j]);
            }

            // error cast
            ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
        });
    }
}

TEST_F(VectorizedCastExprTest, intToBigIntCastExpr) {
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.type = gen_type_desc(TPrimitiveType::BIGINT);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 10);

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_TRUE(ptr->is_numeric());

            // right cast
            auto v = std::static_pointer_cast<Int64Column>(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(10, v->get_data()[j]);
            }

            // error cast
            ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int8Column>(ptr));
        });
    }
}

TEST_F(VectorizedCastExprTest, NullableBooleanCastExpr) {
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    expr_node.is_nullable = true;

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    MockNullVectorizedExpr<TYPE_INT> col1(expr_node, 10, -1);
    expr_node.is_nullable = false;

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_numeric());
            ASSERT_TRUE(ptr->is_nullable());

            // right cast
            auto v = std::static_pointer_cast<BooleanColumn>(
                    std::static_pointer_cast<NullableColumn>(ptr)->data_column());
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(1, (v->get_data()[j]));
            }

            // error cast
            ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
        });
    }
}

TEST_F(VectorizedCastExprTest, dateCastToDecimalV2) {
    expr_node.child_type = TPrimitiveType::DATE;
    expr_node.type = gen_type_desc(TPrimitiveType::DECIMALV2);
    expr_node.type.types[0].scalar_type.__set_precision(10);
    expr_node.type.types[0].scalar_type.__set_scale(2);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_DATE> col1(expr_node, 10, DateValue::create(2000, 12, 31));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_decimal());

        // right cast
        auto v = std::static_pointer_cast<DecimalColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(DecimalV2Value(20001231, 0), v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, decimalV2CastToTimestamp) {
    expr_node.child_type = TPrimitiveType::DECIMALV2;
    expr_node.type = gen_type_desc(TPrimitiveType::DATETIME);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_DECIMALV2> col1(expr_node, 10, DecimalV2Value("20010129123000"));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_timestamp());

        // right cast
        auto v = std::static_pointer_cast<TimestampColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(TimestampValue::create(2001, 01, 29, 12, 30, 00), v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, dateCastToTimestamp) {
    expr_node.child_type = TPrimitiveType::DATE;
    expr_node.type = gen_type_desc(TPrimitiveType::DATETIME);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_DATE> col1(expr_node, 10, DateValue::create(2010, 10, 20));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_timestamp());

        // right cast
        auto v = std::static_pointer_cast<TimestampColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(TimestampValue::create(2010, 10, 20, 0, 0, 0), v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, decimalCastString) {
    expr_node.child_type = TPrimitiveType::DECIMALV2;
    expr_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
    expr_node.type.types[0].scalar_type.__set_len(10);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_DECIMALV2> col1(expr_node, 10, DecimalV2Value(123, 0));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_binary());

        // right cast
        auto v = std::static_pointer_cast<BinaryColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(std::string("123"), v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, intCastString) {
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
    expr_node.type.types[0].scalar_type.__set_len(10);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, 12345);

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_binary());

        // right cast
        auto v = std::static_pointer_cast<BinaryColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(std::string("12345"), v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, booleanCastString) {
    expr_node.child_type = TPrimitiveType::BOOLEAN;
    expr_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
    expr_node.type.types[0].scalar_type.__set_len(10);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_BOOLEAN> col1(expr_node, 10, true);

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_binary());

        // right cast
        auto v = std::static_pointer_cast<BinaryColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(std::string("1"), v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, timestmapCastString) {
    expr_node.child_type = TPrimitiveType::DATETIME;
    expr_node.type = gen_type_desc(TPrimitiveType::VARCHAR);
    expr_node.type.types[0].scalar_type.__set_len(10);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_DATETIME> col1(expr_node, 10, TimestampValue::create(2020, 02, 03, 1, 23, 45));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_binary());

        // right cast
        auto v = std::static_pointer_cast<BinaryColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(std::string("2020-02-03 01:23:45"), v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, stringCastInt) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::INT);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("1234");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_numeric());

        // right cast
        auto v = ColumnHelper::cast_to_raw<TYPE_INT>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(1234, v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, stringCastIntError) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::INT);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("123ad4");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_nullable());

        // right cast
        auto v = ColumnHelper::cast_to_raw<TYPE_INT>(ColumnHelper::as_raw_column<NullableColumn>(ptr)->data_column());
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_TRUE(ptr->is_null(j));
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, stringCastDouble) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::DOUBLE);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("1234.1234");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_numeric());

        // right cast
        auto v = ColumnHelper::cast_to_raw<TYPE_DOUBLE>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(1234.1234, v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, stringCastDoubleError) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::DOUBLE);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("123ad4.123123");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_nullable());

        // right cast
        auto v =
                ColumnHelper::cast_to_raw<TYPE_DOUBLE>(ColumnHelper::as_raw_column<NullableColumn>(ptr)->data_column());
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_TRUE(ptr->is_null(j));
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, stringCastDecimal) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::DECIMALV2);
    expr_node.type.types[0].scalar_type.__set_precision(10);
    expr_node.type.types[0].scalar_type.__set_scale(2);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    DecimalV2Value d(1794546454654654);
    std::string p = d.to_string();

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_decimal());

        // right cast
        auto v = ColumnHelper::cast_to_raw<TYPE_DECIMALV2>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(d, v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, stringCastDecimalError) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::DECIMALV2);
    expr_node.type.types[0].scalar_type.__set_precision(10);
    expr_node.type.types[0].scalar_type.__set_scale(2);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("asdfadsf");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_nullable());

        // right cast
        auto v = ColumnHelper::cast_to_raw<TYPE_DECIMALV2>(
                ColumnHelper::as_raw_column<NullableColumn>(ptr)->data_column());
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_TRUE(ptr->is_null(j));
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, stringCastDate) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::DATE);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("2023-12-02");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_date());

        // right cast
        auto v = ColumnHelper::cast_to_raw<TYPE_DATE>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(DateValue::create(2023, 12, 02), v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, stringCastDate2) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::DATE);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("   2023-12-02    ");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_date());

        // right cast
        auto v = ColumnHelper::cast_to_raw<TYPE_DATE>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(DateValue::create(2023, 12, 02), v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, stringCastDateError) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::DATE);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("2023-12-asdf");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_nullable());

        // right cast
        auto v = ColumnHelper::cast_to_raw<TYPE_DATE>(ColumnHelper::as_raw_column<NullableColumn>(ptr)->data_column());
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_TRUE(ptr->is_null(j));
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, stringCastTimestmap) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::DATETIME);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("2022-02-03 11:23:45");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_timestamp());

        // right cast
        auto v = std::static_pointer_cast<TimestampColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(TimestampValue::create(2022, 02, 03, 11, 23, 45), v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, stringCastBitmapFailed0) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::OBJECT);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    // readable string
    std::string p("1, 342, 2222");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto v = std::static_pointer_cast<BitmapColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }
    }
}

TEST_F(VectorizedCastExprTest, stringCastBitmapFailed1) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::OBJECT);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::vector<uint64_t> bits;
    bits.push_back(1);
    bits.push_back(342);
    bits.push_back(2222);
    BitmapValue bitmap_value(bits);

    std::string buf;
    buf.resize(bitmap_value.getSizeInBytes());
    bitmap_value.write((char*)buf.c_str());
    // non-exist type bitmap.
    *((uint8_t*)(buf.c_str())) = (uint8_t)14;

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(buf));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto v = std::static_pointer_cast<BitmapColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }
    }
}

TEST_F(VectorizedCastExprTest, stringCastBitmapSingle) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::OBJECT);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    BitmapValue bitmap_value;
    bitmap_value.add(1);

    std::string buf;
    buf.resize(bitmap_value.getSizeInBytes());
    bitmap_value.write((char*)buf.c_str());

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(buf));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto v = std::static_pointer_cast<BitmapColumn>(ptr);
        ASSERT_EQ(10, v->size());

        std::vector<int64_t> expect_array;
        expect_array.push_back(1);

        for (int j = 0; j < v->size(); ++j) {
            std::vector<int64_t> array;
            v->get_data()[j]->to_array(&array);
            ASSERT_EQ(expect_array, array);
        }
    }
}

TEST_F(VectorizedCastExprTest, stringCastBitmapSingleFailed) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::OBJECT);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    BitmapValue bitmap_value;
    bitmap_value.add(1);

    std::string buf;
    buf.resize(bitmap_value.getSizeInBytes());
    bitmap_value.write((char*)buf.c_str());

    size_t half_length = buf.size() / 2;

    // set smaller length.
    buf.resize(half_length);

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(buf));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto v = std::static_pointer_cast<BitmapColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }
    }
}

TEST_F(VectorizedCastExprTest, stringCastBitmapSet) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::OBJECT);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    BitmapValue bitmap_value;
    bitmap_value.add(1);
    bitmap_value.add(2);

    std::string buf;
    buf.resize(bitmap_value.getSizeInBytes());
    bitmap_value.write((char*)buf.c_str());

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(buf));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto v = std::static_pointer_cast<BitmapColumn>(ptr);
        ASSERT_EQ(10, v->size());

        std::vector<int64_t> expect_array;
        expect_array.push_back(1);
        expect_array.push_back(2);

        for (int j = 0; j < v->size(); ++j) {
            std::vector<int64_t> array;
            v->get_data()[j]->to_array(&array);
            ASSERT_EQ(expect_array, array);
        }
    }
}

TEST_F(VectorizedCastExprTest, stringCastBitmapSetFailed) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::OBJECT);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    BitmapValue bitmap_value;
    bitmap_value.add(1);
    bitmap_value.add(2);

    std::string buf;
    buf.resize(bitmap_value.getSizeInBytes());
    bitmap_value.write((char*)buf.c_str());

    size_t half_length = buf.size() / 2;

    // set smaller length.
    buf.resize(half_length);

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(buf));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto v = std::static_pointer_cast<BitmapColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }
    }
}

TEST_F(VectorizedCastExprTest, stringCastBitmapMap) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::OBJECT);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::vector<uint64_t> bits;
    bits.push_back(1);
    bits.push_back(342);
    bits.push_back(2222);
    BitmapValue bitmap_value(bits);

    std::string buf;
    buf.resize(bitmap_value.getSizeInBytes());
    bitmap_value.write((char*)buf.c_str());

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(buf));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto v = std::static_pointer_cast<BitmapColumn>(ptr);
        ASSERT_EQ(10, v->size());

        std::vector<int64_t> expect_array;
        expect_array.push_back(1);
        expect_array.push_back(342);
        expect_array.push_back(2222);

        for (int j = 0; j < v->size(); ++j) {
            std::vector<int64_t> array;
            v->get_data()[j]->to_array(&array);
            ASSERT_EQ(expect_array, array);
        }
    }
}

TEST_F(VectorizedCastExprTest, stringCastBitmapMapFailed) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::OBJECT);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::vector<uint64_t> bits;
    bits.push_back(1);
    bits.push_back(342);
    bits.push_back(2222);
    BitmapValue bitmap_value(bits);

    std::string buf;
    buf.resize(bitmap_value.getSizeInBytes());
    bitmap_value.write((char*)buf.c_str());
    size_t half_length = buf.size() / 2;

    // set smaller length.
    buf.resize(half_length);

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(buf));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto v = std::static_pointer_cast<BitmapColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }
    }
}

TEST_F(VectorizedCastExprTest, stringCastTimestmap2) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::DATETIME);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("    2022-02-03 11:23:45 ");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_timestamp());

        // right cast
        auto v = std::static_pointer_cast<TimestampColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(TimestampValue::create(2022, 02, 03, 11, 23, 45), v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, stringCastTimestmap3) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::DATETIME);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("2022-02-03     11:23:45");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_timestamp());

        // right cast
        auto v = std::static_pointer_cast<TimestampColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(TimestampValue::create(2022, 02, 03, 11, 23, 45), v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, stringCastTimestmap4) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::DATETIME);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("2022-02-03T11:23:45");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_timestamp());

        // right cast
        auto v = std::static_pointer_cast<TimestampColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(TimestampValue::create(2022, 02, 03, 11, 23, 45), v->get_data()[j]);
        }

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, stringCastTimestmapError) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::DATETIME);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("2022-02-03 asdfa");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ASSERT_TRUE(ptr->is_timestamp());

        // right cast
        auto v = ColumnHelper::cast_to_raw<TYPE_DATETIME>(ptr);
        ASSERT_EQ(10, v->size());

        // error cast
        ASSERT_EQ(nullptr, std::dynamic_pointer_cast<Int64Column>(ptr));
    }
}

TEST_F(VectorizedCastExprTest, BigIntCastToInt) {
    expr_node.child_type = TPrimitiveType::BIGINT;
    expr_node.type = gen_type_desc(TPrimitiveType::INT);
    expr_node.is_nullable = true;

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    expr_node.is_nullable = false;
    MockVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, INT64_MAX);

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_TRUE(ptr->is_nullable());

            for (int j = 0; j < ptr->size(); ++j) {
                ASSERT_TRUE(ptr->is_null(j));
            }
        });
    }
}

TEST_F(VectorizedCastExprTest, BigIntCastToInt2) {
    expr_node.child_type = TPrimitiveType::BIGINT;
    expr_node.type = gen_type_desc(TPrimitiveType::INT);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_BIGINT> col1(expr_node, 10, 10);

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_TRUE(ptr->is_numeric());

            // right cast
            auto v = std::static_pointer_cast<Int32Column>(ptr);
            ASSERT_EQ(10, v->size());

            for (int j = 0; j < v->size(); ++j) {
                ASSERT_EQ(10, v->get_data()[j]);
            }
        });
    }
}

TEST_F(VectorizedCastExprTest, IntCastToBigInt3) {
    expr_node.child_type = TPrimitiveType::INT;
    expr_node.type = gen_type_desc(TPrimitiveType::BIGINT);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_INT> col1(expr_node, 10, INT_MAX);

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        ExprsTestHelper::verify_with_jit(ptr, expr.get(), &runtime_state, [](ColumnPtr const& ptr) {
            ASSERT_FALSE(ptr->is_nullable());

            auto p = ColumnHelper::cast_to<TYPE_BIGINT>(ptr);
            for (int j = 0; j < p->size(); ++j) {
                ASSERT_EQ(INT_MAX, p->get_data()[j]);
            }
        });
    }
}

TEST_F(VectorizedCastExprTest, stringCastToTime) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::TIME);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("15:15:15");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto v = std::static_pointer_cast<DoubleColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_EQ(54915, v->get_data()[j]);
        }
    }
}

TEST_F(VectorizedCastExprTest, stringCastToTimeNull1) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::TIME);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("15:15:15:");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto v = ColumnHelper::as_column<NullableColumn>(ptr);

        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }
    }
}

TEST_F(VectorizedCastExprTest, stringCastToTimeNull2) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::TIME);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("15:60:15");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto v = ColumnHelper::as_column<NullableColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }
    }
}

TEST_F(VectorizedCastExprTest, stringCastToTimeNull3) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::TIME);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("15:15");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto v = ColumnHelper::as_column<NullableColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }
    }
}

TEST_F(VectorizedCastExprTest, stringCastToTimeNull4) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::TIME);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("      :60:16");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto v = ColumnHelper::as_column<NullableColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }
    }
}

TEST_F(VectorizedCastExprTest, stringCastToTimeNull5) {
    expr_node.child_type = TPrimitiveType::VARCHAR;
    expr_node.type = gen_type_desc(TPrimitiveType::TIME);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    std::string p("15::15:15");

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(expr_node, 10, Slice(p));

    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto v = ColumnHelper::as_column<NullableColumn>(ptr);
        ASSERT_EQ(10, v->size());

        for (int j = 0; j < v->size(); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }
    }
}

TEST_F(VectorizedCastExprTest, bigintToTime) {
    expr_node.child_type = TPrimitiveType::BIGINT;
    expr_node.type = gen_type_desc(TPrimitiveType::TIME);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    MockMultiVectorizedExpr<TYPE_BIGINT> col1(expr_node, 2, 32020, 346050);
    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto v = ColumnHelper::as_column<NullableColumn>(ptr);
        ASSERT_EQ(2, v->size());

        auto d = ColumnHelper::cast_to<TYPE_TIME>(v->data_column());

        ASSERT_FALSE(v->is_null(0));
        ASSERT_EQ(12020, d->get_data()[0]);
        ASSERT_TRUE(v->is_null(1));
    }
}

TEST_F(VectorizedCastExprTest, dateToTime) {
    expr_node.child_type = TPrimitiveType::DATE;
    expr_node.type = gen_type_desc(TPrimitiveType::TIME);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_DATE> col1(expr_node, 2, DateValue::create(2000, 12, 01));
    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto d = ColumnHelper::cast_to<TYPE_TIME>(ptr);
        ASSERT_EQ(2, d->size());

        ASSERT_EQ(0, d->get_data()[0]);
        ASSERT_EQ(0, d->get_data()[1]);
    }
}

TEST_F(VectorizedCastExprTest, datetimeToTime) {
    expr_node.child_type = TPrimitiveType::DATETIME;
    expr_node.type = gen_type_desc(TPrimitiveType::TIME);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));
    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_DATETIME> col1(expr_node, 2, TimestampValue::create(2000, 12, 1, 12, 30, 00));
    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto d = ColumnHelper::cast_to<TYPE_TIME>(ptr);
        ASSERT_EQ(2, d->size());

        ASSERT_EQ(45000, d->get_data()[0]);
        ASSERT_EQ(45000, d->get_data()[1]);
    }
}

TEST_F(VectorizedCastExprTest, timeToInt) {
    expr_node.child_type = TPrimitiveType::TIME;
    expr_node.type = gen_type_desc(TPrimitiveType::INT);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));
    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_TIME> col1(expr_node, 2, 76862);
    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto d = ColumnHelper::cast_to<TYPE_INT>(ptr);
        ASSERT_EQ(2, d->size());

        ASSERT_EQ(212102, d->get_data()[0]);
        ASSERT_EQ(212102, d->get_data()[1]);
    }
}

TEST_F(VectorizedCastExprTest, timeToVarchar) {
    expr_node.child_type = TPrimitiveType::TIME;
    expr_node.type = gen_type_desc(TPrimitiveType::VARCHAR);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));
    expr_node.type = gen_type_desc(expr_node.child_type);
    MockVectorizedExpr<TYPE_TIME> col1(expr_node, 2, 8521);
    expr->_children.push_back(&col1);

    {
        ColumnPtr ptr = expr->evaluate(nullptr, nullptr);

        // right cast
        auto d = ColumnHelper::cast_to<TYPE_VARCHAR>(ptr);
        ASSERT_EQ(2, d->size());

        ASSERT_EQ("02:22:01", d->get_data()[0]);
        ASSERT_EQ("02:22:01", d->get_data()[1]);
    }
}

template <LogicalType toType, class JsonValueType>
static typename RunTimeColumnType<toType>::Ptr evaluateCastFromJson(TExprNode& cast_expr, JsonValueType json_str) {
    TPrimitiveType::type t_type = to_thrift(toType);
    cast_expr.type = gen_type_desc(t_type);

    std::cerr << "evaluate cast from json: " << json_str << std::endl;

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(cast_expr));

    StatusOr<JsonValue> json = JsonValue::from(json_str);
    if (!json.ok()) {
        return nullptr;
    }
    cast_expr.type = gen_type_desc(cast_expr.child_type);
    MockVectorizedExpr<TYPE_JSON> col1(cast_expr, 2, &json.value());
    expr->_children.push_back(&col1);

    ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
    if (!ptr) {
        return nullptr;
    }
    return ColumnHelper::cast_to<toType>(ptr);
}

template <LogicalType toType, class JsonValueType>
static ColumnPtr evaluateCastJsonNullable(TExprNode& cast_expr, JsonValueType json_str) {
    std::cerr << "evaluate castCast: " << json_str << std::endl;
    TPrimitiveType::type t_type = to_thrift(toType);
    cast_expr.type = gen_type_desc(t_type);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(cast_expr));
    if (!expr) {
        return nullptr;
    }
    StatusOr<JsonValue> json = JsonValue::from(json_str);

    if (!json.ok()) {
        return nullptr;
    }
    cast_expr.type = gen_type_desc(cast_expr.child_type);
    MockVectorizedExpr<TYPE_JSON> col1(cast_expr, 2, &json.value());
    expr->_children.push_back(&col1);

    ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
    return ptr;
}

// Test cast json value to SQL type
TEST_F(VectorizedCastExprTest, jsonToValue) {
    TExprNode cast_expr;
    cast_expr.opcode = TExprOpcode::CAST;
    cast_expr.child_type = TPrimitiveType::JSON;
    cast_expr.node_type = TExprNodeType::CAST_EXPR;
    cast_expr.num_children = 2;
    cast_expr.__isset.opcode = true;
    cast_expr.__isset.child_type = true;

    // cast self
    auto jsonCol = evaluateCastFromJson<TYPE_JSON>(cast_expr, "{\"a\": 1}");
    EXPECT_EQ("{\"a\": 1}", jsonCol->get_data()[0]->to_string().value());

    // cast success
    EXPECT_EQ(1, evaluateCastFromJson<TYPE_INT>(cast_expr, 1)->get_data()[0]);
    EXPECT_EQ(1.1, evaluateCastFromJson<TYPE_DOUBLE>(cast_expr, 1.1)->get_data()[0]);
    EXPECT_EQ(true, evaluateCastFromJson<TYPE_BOOLEAN>(cast_expr, true)->get_data()[0]);
    EXPECT_EQ(false, evaluateCastFromJson<TYPE_BOOLEAN>(cast_expr, false)->get_data()[0]);
    EXPECT_EQ("a", evaluateCastFromJson<TYPE_VARCHAR>(cast_expr, "\"a\"")->get_data()[0]);
    EXPECT_EQ("1", evaluateCastFromJson<TYPE_VARCHAR>(cast_expr, "\"1\"")->get_data()[0]);
    EXPECT_EQ("[1, 2, 3]", evaluateCastFromJson<TYPE_VARCHAR>(cast_expr, "[1,2,3]")->get_data()[0]);
    EXPECT_EQ("1", evaluateCastFromJson<TYPE_VARCHAR>(cast_expr, "1")->get_data()[0]);
    EXPECT_EQ("1.1", evaluateCastFromJson<TYPE_VARCHAR>(cast_expr, "1.1")->get_data()[0]);
    EXPECT_EQ("true", evaluateCastFromJson<TYPE_VARCHAR>(cast_expr, "true")->get_data()[0]);
    EXPECT_EQ("star", evaluateCastFromJson<TYPE_VARCHAR>(cast_expr, "\"star\"")->get_data()[0]);
    EXPECT_EQ("{\"a\": 1}", evaluateCastFromJson<TYPE_VARCHAR>(cast_expr, "{\"a\": 1}")->get_data()[0]);
    EXPECT_EQ("", evaluateCastFromJson<TYPE_VARCHAR>(cast_expr, "")->get_data()[0]);

    // implicit json type case
    EXPECT_EQ(1.0, evaluateCastFromJson<TYPE_DOUBLE>(cast_expr, 1)->get_data()[0]);
    EXPECT_EQ(1, evaluateCastFromJson<TYPE_INT>(cast_expr, 1.1)->get_data()[0]);

    // cast failed
    EXPECT_EQ(2, ColumnHelper::count_nulls(evaluateCastJsonNullable<TYPE_INT>(cast_expr, "\"a\"")));
    EXPECT_EQ(2, ColumnHelper::count_nulls(evaluateCastJsonNullable<TYPE_INT>(cast_expr, "false")));
    EXPECT_EQ(2, ColumnHelper::count_nulls(evaluateCastJsonNullable<TYPE_INT>(cast_expr, "null")));
    EXPECT_EQ(2, ColumnHelper::count_nulls(evaluateCastJsonNullable<TYPE_INT>(cast_expr, "[1,2]")));
    EXPECT_EQ(2, ColumnHelper::count_nulls(evaluateCastJsonNullable<TYPE_BOOLEAN>(cast_expr, "1")));
    EXPECT_EQ(2, ColumnHelper::count_nulls(evaluateCastJsonNullable<TYPE_BOOLEAN>(cast_expr, "\"a\"")));
    EXPECT_EQ(2, ColumnHelper::count_nulls(evaluateCastJsonNullable<TYPE_BOOLEAN>(cast_expr, "1.0")));
    EXPECT_EQ(2, ColumnHelper::count_nulls(evaluateCastJsonNullable<TYPE_BOOLEAN>(cast_expr, "null")));
    EXPECT_EQ(2, ColumnHelper::count_nulls(evaluateCastJsonNullable<TYPE_BOOLEAN>(cast_expr, "[]")));
    EXPECT_EQ(2, ColumnHelper::count_nulls(evaluateCastJsonNullable<TYPE_BOOLEAN>(cast_expr, "{}")));

    // overflow
    EXPECT_EQ(2, ColumnHelper::count_nulls(evaluateCastJsonNullable<TYPE_TINYINT>(cast_expr, 100000)));
    EXPECT_EQ(2, ColumnHelper::count_nulls(evaluateCastJsonNullable<TYPE_TINYINT>(cast_expr, -100000)));
    EXPECT_EQ(2, ColumnHelper::count_nulls(evaluateCastJsonNullable<TYPE_INT>(
                         cast_expr, std::to_string(std::numeric_limits<int64_t>::max()))));
    EXPECT_EQ(2, ColumnHelper::count_nulls(evaluateCastJsonNullable<TYPE_INT>(
                         cast_expr, std::to_string(std::numeric_limits<int64_t>::lowest()))));
    EXPECT_EQ(2, ColumnHelper::count_nulls(evaluateCastJsonNullable<TYPE_FLOAT>(
                         cast_expr, std::to_string(std::numeric_limits<double>::max()))));
    EXPECT_EQ(2, ColumnHelper::count_nulls(evaluateCastJsonNullable<TYPE_FLOAT>(
                         cast_expr, std::to_string(std::numeric_limits<double>::lowest()))));

    // Not supported
    EXPECT_EQ(nullptr, evaluateCastJsonNullable<TYPE_DECIMALV2>(cast_expr, "1"));
    EXPECT_EQ(nullptr, evaluateCastJsonNullable<TYPE_DECIMAL32>(cast_expr, "1"));
    EXPECT_EQ(nullptr, evaluateCastJsonNullable<TYPE_DECIMAL64>(cast_expr, "1"));
    EXPECT_EQ(nullptr, evaluateCastJsonNullable<TYPE_DECIMAL128>(cast_expr, "1"));
    EXPECT_EQ(nullptr, evaluateCastJsonNullable<TYPE_TIME>(cast_expr, "1"));
    EXPECT_EQ(nullptr, evaluateCastJsonNullable<TYPE_DATE>(cast_expr, "1"));
    EXPECT_EQ(nullptr, evaluateCastJsonNullable<TYPE_DATETIME>(cast_expr, "1"));
    EXPECT_EQ(nullptr, evaluateCastJsonNullable<TYPE_HLL>(cast_expr, "1"));
}

template <LogicalType fromType>
static std::string evaluateCastToJson(TExprNode& cast_expr, RunTimeCppType<fromType> value) {
    cast_expr.child_type = to_thrift(fromType);
    cast_expr.type = gen_type_desc(to_thrift(TYPE_JSON));

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(cast_expr));
    if (!expr.get()) {
        return "";
    }
    cast_expr.type = gen_type_desc(cast_expr.child_type);
    MockVectorizedExpr<fromType> col1(cast_expr, 2, value);
    expr->_children.push_back(&col1);

    ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
    if (!ptr) {
        return nullptr;
    }
    if (ptr->has_null()) {
        return "";
    }
    ColumnPtr result_column = ColumnHelper::cast_to<TYPE_JSON>(ptr);
    if (result_column->is_null(0)) {
        return "";
    }
    const JsonValue* json = result_column->get(0).get_json();
    auto json_str = json->to_string();
    if (!json_str.ok()) {
        return "";
    }
    return json_str.value();
}

TEST_F(VectorizedCastExprTest, sqlToJson) {
    TExprNode cast_expr;
    cast_expr.opcode = TExprOpcode::CAST;
    cast_expr.node_type = TExprNodeType::CAST_EXPR;
    cast_expr.num_children = 2;
    cast_expr.__isset.opcode = true;
    cast_expr.__isset.child_type = true;

    // boolean
    {
        EXPECT_EQ("true", evaluateCastToJson<TYPE_BOOLEAN>(cast_expr, true));
        EXPECT_EQ("false", evaluateCastToJson<TYPE_BOOLEAN>(cast_expr, false));
    }
    // int
    {
        EXPECT_EQ("123", evaluateCastToJson<TYPE_INT>(cast_expr, 123));
        EXPECT_EQ("-123", evaluateCastToJson<TYPE_INT>(cast_expr, -123));
        EXPECT_EQ("-1", evaluateCastToJson<TYPE_TINYINT>(cast_expr, -1));
        EXPECT_EQ("-1", evaluateCastToJson<TYPE_SMALLINT>(cast_expr, -1));
        EXPECT_EQ("10000000000", evaluateCastToJson<TYPE_BIGINT>(cast_expr, 1E10));
        EXPECT_EQ("10000000000", evaluateCastToJson<TYPE_LARGEINT>(cast_expr, 1E10));
        EXPECT_EQ("", evaluateCastToJson<TYPE_LARGEINT>(cast_expr, RunTimeTypeLimits<TYPE_LARGEINT>::max_value()));
        EXPECT_EQ("", evaluateCastToJson<TYPE_LARGEINT>(cast_expr, RunTimeTypeLimits<TYPE_LARGEINT>::min_value()));
    }

    // double/float
    {
        EXPECT_EQ("1.23", evaluateCastToJson<TYPE_DOUBLE>(cast_expr, 1.23));
        EXPECT_EQ("-1.23", evaluateCastToJson<TYPE_DOUBLE>(cast_expr, -1.23));

        EXPECT_EQ("1.23", evaluateCastToJson<TYPE_FLOAT>(cast_expr, 1.23).substr(0, 4));
        EXPECT_EQ("-1.23", evaluateCastToJson<TYPE_FLOAT>(cast_expr, -1.23).substr(0, 5));
    }

    // string
    {
        EXPECT_EQ(R"("star")", evaluateCastToJson<TYPE_CHAR>(cast_expr, "star"));
        EXPECT_EQ(R"(" star")", evaluateCastToJson<TYPE_VARCHAR>(cast_expr, " star"));

        EXPECT_EQ(R"(" 1")", evaluateCastToJson<TYPE_CHAR>(cast_expr, " 1"));
        EXPECT_EQ(R"("1")", evaluateCastToJson<TYPE_CHAR>(cast_expr, "\"1\""));
        EXPECT_EQ(R"({})", evaluateCastToJson<TYPE_CHAR>(cast_expr, "{}"));
        EXPECT_EQ(R"({})", evaluateCastToJson<TYPE_CHAR>(cast_expr, "   {}"));
        EXPECT_EQ(R"({"star": "rocks"})", evaluateCastToJson<TYPE_CHAR>(cast_expr, R"({"star": "rocks"})"));
        EXPECT_EQ(R"([])", evaluateCastToJson<TYPE_CHAR>(cast_expr, "[]"));

        std::string str = "上海";
        EXPECT_EQ(R"("上海")", evaluateCastToJson<TYPE_CHAR>(cast_expr, str));
        EXPECT_EQ(R"("上海")", evaluateCastToJson<TYPE_VARCHAR>(cast_expr, str));
    }
    // json
    {
        JsonValue json = JsonValue::from_int(1);
        EXPECT_EQ(R"(1)", evaluateCastToJson<TYPE_JSON>(cast_expr, &json));
    }
    // temporal
    {
        EXPECT_EQ(R"("2023-03-14")", evaluateCastToJson<TYPE_DATE>(cast_expr, DateValue::create(2023, 3, 14)));
        EXPECT_EQ(R"("2023-03-14 01:02:03")",
                  evaluateCastToJson<TYPE_DATETIME>(cast_expr, TimestampValue::create(2023, 3, 14, 1, 2, 3)));
    }
}

TTypeDesc gen_multi_array_type_desc(const TPrimitiveType::type field_type, size_t dim) {
    std::vector<TTypeNode> types_list;
    TTypeDesc type_desc;

    for (auto i = 0; i < dim; ++i) {
        TTypeNode type_array;
        type_array.type = TTypeNodeType::ARRAY;
        types_list.push_back(type_array);
    }

    TTypeNode type_scalar;
    TScalarType scalar_type;
    scalar_type.__set_type(field_type);
    scalar_type.__set_precision(0);
    scalar_type.__set_scale(0);
    scalar_type.__set_len(0);
    type_scalar.__set_scalar_type(scalar_type);
    types_list.push_back(type_scalar);

    type_desc.__set_types(types_list);
    return type_desc;
}

static std::string cast_string_to_array(TExprNode& cast_expr, TTypeDesc type_desc, const std::string& str) {
    cast_expr.child_type = to_thrift(TYPE_VARCHAR);
    cast_expr.type = type_desc;

    ObjectPool pool;
    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(&pool, cast_expr));
    cast_expr.type = gen_type_desc(cast_expr.child_type);
    MockVectorizedExpr<TYPE_VARCHAR> col1(cast_expr, 1, str);
    expr->_children.push_back(&col1);
    ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
    if (ptr->size() != 1) {
        return "EMPTY";
    }
    return ptr->debug_item(0);
}

TTypeDesc gen_array_type_desc(const TPrimitiveType::type field_type) {
    std::vector<TTypeNode> types_list;
    TTypeDesc type_desc;

    TTypeNode type_array;
    type_array.type = TTypeNodeType::ARRAY;
    types_list.push_back(type_array);

    TTypeNode type_scalar;
    TScalarType scalar_type;
    scalar_type.__set_type(field_type);
    scalar_type.__set_precision(0);
    scalar_type.__set_scale(0);
    scalar_type.__set_len(0);
    type_scalar.__set_scalar_type(scalar_type);
    types_list.push_back(type_scalar);

    type_desc.__set_types(types_list);
    return type_desc;
}

static std::string cast_string_to_array(TExprNode& cast_expr, LogicalType element_type, const std::string& str) {
    auto type_desc = gen_array_type_desc(to_thrift(element_type));
    return cast_string_to_array(cast_expr, type_desc, str);
}

static ColumnPtr cast_string_to_array_ptr(TExprNode& cast_expr, LogicalType element_type, const ColumnPtr& src) {
    auto type_desc = gen_array_type_desc(to_thrift(element_type));
    cast_expr.child_type = to_thrift(TYPE_VARCHAR);
    cast_expr.type = type_desc;

    ObjectPool pool;
    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(&pool, cast_expr));
    std::unique_ptr<starrocks::Expr> child_expr = std::make_unique<MockExpr>(cast_expr, src);
    expr->_children.push_back(child_expr.get());
    return expr->evaluate(nullptr, nullptr);
}

TEST_F(VectorizedCastExprTest, string_to_array) {
    TExprNode cast_expr;
    cast_expr.opcode = TExprOpcode::CAST;
    cast_expr.node_type = TExprNodeType::CAST_EXPR;
    cast_expr.num_children = 2;
    cast_expr.__isset.opcode = true;
    cast_expr.__isset.child_type = true;

    EXPECT_EQ("[1,2,3]", cast_string_to_array(cast_expr, TYPE_INT, "[1,2,3]"));
    EXPECT_EQ("[1,2,3]", cast_string_to_array(cast_expr, TYPE_INT, "[1,   2,  3]"));
    EXPECT_EQ("[]", cast_string_to_array(cast_expr, TYPE_INT, "[]"));
    EXPECT_EQ("[NULL,NULL,NULL]", cast_string_to_array(cast_expr, TYPE_INT, "[a,b,c]"));
    EXPECT_EQ("[NULL,NULL]", cast_string_to_array(cast_expr, TYPE_INT, "[\"a\",\"b\"]"));

    EXPECT_EQ("[1.1,2.2,3.3]", cast_string_to_array(cast_expr, TYPE_DOUBLE, "[1.1,2.2,3.3]"));

    // test invalid input
    EXPECT_EQ("NULL", cast_string_to_array(cast_expr, TYPE_INT, ""));
    EXPECT_EQ("NULL", cast_string_to_array(cast_expr, TYPE_INT, "1,2,3"));
    EXPECT_EQ("NULL", cast_string_to_array(cast_expr, TYPE_INT, "[[1,2,3]"));
    EXPECT_EQ("NULL", cast_string_to_array(cast_expr, TYPE_INT, "[]]"));
    EXPECT_EQ("NULL", cast_string_to_array(cast_expr, TYPE_INT, "[\"\']"));
    EXPECT_EQ("NULL", cast_string_to_array(cast_expr, TYPE_VARCHAR, R"(['"'"])"));
    EXPECT_EQ("NULL", cast_string_to_array(cast_expr, TYPE_VARCHAR, R"( 1 )"));
    EXPECT_EQ("NULL", cast_string_to_array(cast_expr, TYPE_VARCHAR, R"( {} )"));
    EXPECT_EQ("NULL", cast_string_to_array(cast_expr, TYPE_VARCHAR, R"( {"a": 1} )"));
    EXPECT_EQ("NULL", cast_string_to_array(cast_expr, TYPE_VARCHAR, R"( "a" )"));
    EXPECT_EQ("NULL", cast_string_to_array(cast_expr, TYPE_VARCHAR, R"( ]]]] )"));
    EXPECT_EQ("NULL", cast_string_to_array(cast_expr, TYPE_VARCHAR, R"(1,2,3)"));

    // test cast to string array
    EXPECT_EQ(R"(['a','b'])", cast_string_to_array(cast_expr, TYPE_VARCHAR, R"(["a","b"])"));
    EXPECT_EQ(R"(['a','b'])", cast_string_to_array(cast_expr, TYPE_VARCHAR, R"([a,b])"));
    EXPECT_EQ(R"(['"a,"b'])", cast_string_to_array(cast_expr, TYPE_VARCHAR, R"(["a,"b])"));
    EXPECT_EQ(R"(['a','b'])", cast_string_to_array(cast_expr, TYPE_VARCHAR, R"(["a", "b"])"));
    EXPECT_EQ(R"(['a',' b'])", cast_string_to_array(cast_expr, TYPE_VARCHAR, R"(["a", " b"])"));
    EXPECT_EQ(R"(['1','2'])", cast_string_to_array(cast_expr, TYPE_VARCHAR, R"([1, 2])"));
    EXPECT_EQ(R"(['['])", cast_string_to_array(cast_expr, TYPE_VARCHAR, R"(['['])"));
    EXPECT_EQ(R"(['"'])", cast_string_to_array(cast_expr, TYPE_VARCHAR, R"(['"'])"));
    EXPECT_EQ(R"(['"xxx'])", cast_string_to_array(cast_expr, TYPE_VARCHAR, R"(['"xxx'])"));
    EXPECT_EQ(R"(['"', ','])", cast_string_to_array(cast_expr, TYPE_VARCHAR, R"(['"', ','])"));

    // test child type
    {
        // select cast('[[["1"]],[["1,3"],["2"],["1"]]]' as array<array<array<string>>>);
        auto type = gen_multi_array_type_desc(to_thrift(TYPE_VARCHAR), 3);
        EXPECT_EQ(R"([[['1']],[['1,3'],['2'],['1']]])",
                  cast_string_to_array(cast_expr, type, R"([[["1"]],[["1,3"],["2"],["1"]]])"));
        // select  cast('[[["1"]],[["1"],["2"],["1"]]]' as array<array<array<string>>>);
        EXPECT_EQ(R"([[['1']],[['1'],['2'],['1']]])",
                  cast_string_to_array(cast_expr, type, R"([[["1"]],[["1"],["2"],["1"]]])"));
        //  select cast('[[4],[[1, 2]]]' as array<array<array<string>>>);
        EXPECT_EQ(R"([[['4']],[['1','2']]])", cast_string_to_array(cast_expr, type, R"([[[4]],[[1, 2]]])"));
    }
}

// Test string to array with const input
TEST_F(VectorizedCastExprTest, string_to_array_with_const_input) {
    TExprNode cast_expr;
    cast_expr.opcode = TExprOpcode::CAST;
    cast_expr.node_type = TExprNodeType::CAST_EXPR;
    cast_expr.num_children = 2;
    cast_expr.__isset.opcode = true;
    cast_expr.__isset.child_type = true;

    // const null
    auto src = ColumnHelper::create_const_null_column(2);
    auto result = cast_string_to_array_ptr(cast_expr, TYPE_VARCHAR, src);
    DCHECK_EQ(result->size(), 2);
    DCHECK(result->is_constant());
    DCHECK(result->only_null());

    // const string
    src = ColumnHelper::create_const_column<TYPE_VARCHAR>(R"(["a","b"])", 2);
    result = cast_string_to_array_ptr(cast_expr, TYPE_VARCHAR, src);
    DCHECK(result->is_constant());
    DCHECK_EQ(result->size(), 2);
    EXPECT_EQ("CONST: ['a','b']", result->debug_item(0));

    // const string: multi-dims
    src = ColumnHelper::create_const_column<TYPE_VARCHAR>(R"([[[4]],[[1, 2]]])", 2);
    result = cast_string_to_array_ptr(cast_expr, TYPE_VARCHAR, src);
    DCHECK(result->is_constant());
    DCHECK_EQ(result->size(), 2);
    EXPECT_EQ("CONST: ['[[4]]','[[1, 2]]']", result->debug_item(0));

    src = ColumnHelper::create_const_column<TYPE_VARCHAR>(R"([[1, 2, 3], [1, 2, 3]])", 2);
    result = cast_string_to_array_ptr(cast_expr, TYPE_VARCHAR, src);
    DCHECK(result->is_constant());
    DCHECK_EQ(result->size(), 2);
    EXPECT_EQ("CONST: ['[1, 2, 3]','[1, 2, 3]']", result->debug_item(0));
}

void array_delimeter_split(const Slice& src, std::vector<Slice>& res, std::vector<char>& stack);
TEST_F(VectorizedCastExprTest, string_split_test) {
    // normal test
    Slice a;
    std::vector<Slice> res;
    std::vector<char> stack;
    {
        // case 1
        res.clear();
        a = "a, b,";
        array_delimeter_split(a, res, stack);
        EXPECT_EQ(res[0], Slice("a"));
        EXPECT_EQ(res[1], Slice(" b"));
        EXPECT_EQ(res[2], Slice(""));

        // case 2
        res.clear();
        a = "aaaaa";
        array_delimeter_split(a, res, stack);
        EXPECT_EQ(res[0], Slice("aaaaa"));

        // case 3
        res.clear();
        a = "[a, b],[c, d]";
        array_delimeter_split(a, res, stack);
        EXPECT_EQ(res[0], Slice("[a, b]"));
        EXPECT_EQ(res[1], Slice("[c, d]"));

        // case 4
        res.clear();
        a = R"([["1"]],[["1,3"],["2"],["1"]])";
        array_delimeter_split(a, res, stack);
        EXPECT_EQ(res[0], Slice(R"([["1"]])"));
        EXPECT_EQ(res[1], Slice(R"([["1,3"],["2"],["1"]])"));

        res.clear();
        a = R"(["1"]][["1,3"],["2"],["1"]])";
        array_delimeter_split(a, res, stack);
    }
}

static std::string cast_json_to_array(TExprNode& cast_expr, LogicalType element_type, const std::string& str) {
    cast_expr.child_type = to_thrift(TYPE_JSON);
    cast_expr.type = gen_array_type_desc(to_thrift(element_type));

    ObjectPool pool;
    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(&pool, cast_expr));

    auto json = JsonValue::parse(str);
    if (!json.ok()) {
        return "INVALID JSON";
    }
    cast_expr.type = gen_type_desc(cast_expr.child_type);
    MockVectorizedExpr<TYPE_JSON> col1(cast_expr, 1, &json.value());
    expr->_children.push_back(&col1);

    ColumnPtr ptr = expr->evaluate(nullptr, nullptr);
    if (ptr->size() != 1) {
        return "EMPTY";
    }
    return ptr->debug_item(0);
}

TEST_F(VectorizedCastExprTest, json_to_array) {
    TExprNode cast_expr;
    cast_expr.opcode = TExprOpcode::CAST;
    cast_expr.node_type = TExprNodeType::CAST_EXPR;
    cast_expr.num_children = 2;
    cast_expr.__isset.opcode = true;
    cast_expr.__isset.child_type = true;

    EXPECT_EQ("[1,2,3]", cast_json_to_array(cast_expr, TYPE_INT, "[1,2,3]"));
    EXPECT_EQ("[1,2,3]", cast_json_to_array(cast_expr, TYPE_INT, "[1,   2,  3]"));
    EXPECT_EQ("[]", cast_json_to_array(cast_expr, TYPE_INT, "[]"));
    EXPECT_EQ("[]", cast_json_to_array(cast_expr, TYPE_INT, ""));
    EXPECT_EQ("[]", cast_json_to_array(cast_expr, TYPE_INT, "a"));
    EXPECT_EQ("[NULL,NULL]", cast_json_to_array(cast_expr, TYPE_INT, "[\"a\",\"b\"]"));

    EXPECT_EQ("[1.1,2.2,3.3]", cast_json_to_array(cast_expr, TYPE_DOUBLE, "[1.1,2.2,3.3]"));

    EXPECT_EQ(R"(['a','b'])", cast_json_to_array(cast_expr, TYPE_VARCHAR, R"(["a","b"])"));
    EXPECT_EQ(R"(['a','b'])", cast_json_to_array(cast_expr, TYPE_VARCHAR, R"(["a", "b"])"));
    EXPECT_EQ(R"(['a',' b'])", cast_json_to_array(cast_expr, TYPE_VARCHAR, R"(["a", " b"])"));
    EXPECT_EQ(R"(['1','2'])", cast_json_to_array(cast_expr, TYPE_VARCHAR, R"([1, 2])"));

    EXPECT_EQ(R"([{"a": 1},{"a": 2}])", cast_json_to_array(cast_expr, TYPE_JSON, R"([{"a": 1}, {"a": 2}])"));
    EXPECT_EQ(R"([null,{"a": 2}])", cast_json_to_array(cast_expr, TYPE_JSON, R"( [null, {"a": 2}] )"));
    EXPECT_EQ(R"([])", cast_json_to_array(cast_expr, TYPE_JSON, R"( {"a": 1} )"));
}

static ColumnPtr cast_json_to_array_ptr(TExprNode& cast_expr, LogicalType element_type, const ColumnPtr& src) {
    cast_expr.child_type = to_thrift(TYPE_JSON);
    cast_expr.type = gen_array_type_desc(to_thrift(element_type));

    ObjectPool pool;
    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(&pool, cast_expr));
    std::unique_ptr<starrocks::Expr> json_col = std::make_unique<MockExpr>(cast_expr, src);
    expr->_children.push_back(json_col.get());

    return expr->evaluate(nullptr, nullptr);
}

static ColumnPtr create_json_const_column(const std::string& str, size_t size) {
    auto json = JsonValue::parse(str);
    if (!json.ok()) {
        return nullptr;
    }
    return ColumnHelper::create_const_column<TYPE_JSON>(&json.value(), size);
}

// Test json to array with const input
TEST_F(VectorizedCastExprTest, json_to_array_with_const_input) {
    TExprNode cast_expr;
    cast_expr.opcode = TExprOpcode::CAST;
    cast_expr.node_type = TExprNodeType::CAST_EXPR;
    cast_expr.num_children = 2;
    cast_expr.__isset.opcode = true;
    cast_expr.__isset.child_type = true;

    // const null
    auto src = ColumnHelper::create_const_null_column(2);
    auto result = cast_json_to_array_ptr(cast_expr, TYPE_JSON, src);
    DCHECK_EQ(result->size(), 2);
    DCHECK(result->is_constant());
    DCHECK(result->only_null());

    // const json
    src = create_json_const_column(R"(["a","b"])", 2);
    result = cast_json_to_array_ptr(cast_expr, TYPE_JSON, src);
    DCHECK(result->is_constant());
    DCHECK_EQ(result->size(), 2);
    EXPECT_EQ("CONST: [\"a\",\"b\"]", result->debug_item(0));

    // const json: multi-dims
    src = create_json_const_column(R"([{"a": 1}, {"a": 2}])", 2);
    result = cast_json_to_array_ptr(cast_expr, TYPE_JSON, src);
    DCHECK(result->is_constant());
    DCHECK_EQ(result->size(), 2);
    EXPECT_EQ("CONST: [{\"a\": 1},{\"a\": 2}]", result->debug_item(0));

    src = create_json_const_column(R"( [null, {"a": 2}] )", 2);
    result = cast_json_to_array_ptr(cast_expr, TYPE_JSON, src);
    DCHECK(result->is_constant());
    DCHECK_EQ(result->size(), 2);
    EXPECT_EQ("CONST: [null,{\"a\": 2}]", result->debug_item(0));
}

TEST_F(VectorizedCastExprTest, unsupported_test) {
    // can't cast arry<array<int>> to array<bool> rather than crash
    expr_node.child_type = to_thrift(LogicalType::TYPE_ARRAY);
    expr_node.child_type_desc = gen_multi_array_type_desc(to_thrift(TYPE_INT), 2);
    expr_node.type = gen_multi_array_type_desc(to_thrift(TYPE_BOOLEAN), 1);

    std::unique_ptr<Expr> expr(VectorizedCastExprFactory::from_thrift(expr_node));

    ASSERT_TRUE(expr == nullptr);
}

} // namespace starrocks
