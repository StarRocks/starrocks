// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <gtest/gtest.h>

#include "butil/time.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "exprs/vectorized/cast_expr.h"
#include "exprs/vectorized/mock_vectorized_expr.h"
#include "runtime/primitive_type.h"
#include "runtime/time_types.h"
#include "testutil/parallel_test.h"

namespace starrocks::vectorized {

template <PrimitiveType Type>
class VerbatimVectorizedExpr : public MockCostExpr {
public:
    VerbatimVectorizedExpr(const TExprNode& t, ColumnPtr column) : MockCostExpr(t), column(column) {}
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, vectorized::Chunk* ptr) override { return column; }

private:
    ColumnPtr column;
};

static void compare_decimal_string(const std::string& expect, const std::string& actual, int scale) {
    if (actual.size() == expect.size()) {
        ASSERT_EQ(actual, expect);
    } else {
        std::cout << "expect=" << expect << ", actual=" << actual << std::endl;
        ASSERT_EQ(actual.size() - scale - 1, expect.size());
        ASSERT_EQ(actual.substr(0, actual.size() - scale - 1), expect);
        ASSERT_EQ(actual[expect.size()], '.');
        ASSERT_EQ(actual.substr(expect.size() + 1), std::string(scale, '0'));
    }
}
// CastTestCase:
// item 0-2: input precision, scale and value in string format
// item 3-5: output precision, scale and value in string format
// item 6: nullable
using CastTestCase = std::tuple<int, int, std::string, int, int, std::string, bool>;
using CastTestCaseArray = std::vector<CastTestCase>;

enum ColumnPackedType {
    SIMPLE,
    CONST_NULL,
    CONST,
    NULLABLE,
};
// create a packed column
template <PrimitiveType Type, ColumnPackedType PackedType, typename... Args>
ColumnPtr create_column(RunTimeCppType<Type> value, size_t front_fill_size, size_t rear_fill_size, Args&&... args) {
    ColumnPtr column;
    auto rows_num = front_fill_size + 1 + rear_fill_size;
    if constexpr (ColumnPackedType::CONST_NULL == PackedType) {
        column = ColumnHelper::create_const_null_column(rows_num);
    } else if constexpr (ColumnPackedType::CONST == PackedType) {
        auto data_column = RunTimeColumnType<Type>::create(std::forward<Args>(args)...);
        data_column->reserve(1);
        data_column->append_datum(Datum(value));
        column = ConstColumn::create(data_column, rows_num);
    } else if constexpr (ColumnPackedType::NULLABLE == PackedType) {
        auto data_column = RunTimeColumnType<Type>::create(std::forward<Args>(args)...);
        auto nulls = NullColumn::create();
        column = NullableColumn::create(data_column, nulls);
        column->reserve(rows_num);
        column->append_nulls(front_fill_size);
        column->append_datum(Datum(value));
        column->append_nulls(rear_fill_size);
    } else {
        column = RunTimeColumnType<Type>::create(std::forward<Args>(args)...);
        column->reserve(rows_num);
        for (auto i = 0; i < rows_num; ++i) {
            column->append_datum(Datum(value));
        }
    }
    return column;
}

// vectorized cast
template <PrimitiveType FromType>
ColumnPtr cast(ColumnPtr from_column, TypeDescriptor const& from_type, TypeDescriptor const& to_type) {
    TExprNode from_node;
    from_node.node_type = TExprNodeType::CAST_EXPR;
    from_type.to_thrift(&from_node.type);
    VerbatimVectorizedExpr<FromType> from_expr(from_node, from_column);
    TExprNode cast_node;
    cast_node.node_type = TExprNodeType::CAST_EXPR;
    to_type.to_thrift(&cast_node.type);
    cast_node.child_type = to_thrift(FromType);
    auto cast_expr = std::unique_ptr<Expr>(VectorizedCastExprFactory::from_thrift(cast_node));
    cast_expr->add_child(&from_expr);
    return cast_expr->evaluate(nullptr, nullptr);
}

template <PrimitiveType FromType, PrimitiveType ToType, ColumnPackedType PackedType>
ColumnPtr cast(RunTimeCppType<FromType> const& value, TypeDescriptor const& from_type, TypeDescriptor const& to_type,
               [[maybe_unused]] int front_fill_size, [[maybe_unused]] int rear_fill_size) {
    TExprNode from_node;
    from_node.node_type = TExprNodeType::CAST_EXPR;
    from_type.to_thrift(&from_node.type);
    ColumnPtr from_column;
    if constexpr (pt_is_decimal<FromType>) {
        from_column = create_column<FromType, PackedType>(value, front_fill_size, rear_fill_size, from_type.precision,
                                                          from_type.scale);
    } else {
        from_column = create_column<FromType, PackedType>(value, front_fill_size, rear_fill_size);
    }
    return cast<FromType>(from_column, from_type, to_type);
}

template <PrimitiveType FromType, PrimitiveType ToType, ColumnPackedType PackedType>
ColumnPtr cast_single_test_case(CastTestCase const& test_case, size_t front_fill_size, size_t rear_fill_size) {
    [[maybe_unused]] auto input_precision = std::get<0>(test_case);
    [[maybe_unused]] auto input_scale = std::get<1>(test_case);
    auto input_value = std::get<2>(test_case);

    [[maybe_unused]] auto output_precision = std::get<3>(test_case);
    [[maybe_unused]] auto output_scale = std::get<4>(test_case);
    TypeDescriptor string_type = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
    TypeDescriptor from_type = TypeDescriptor::from_primtive_type(FromType, TypeDescriptor::MAX_VARCHAR_LENGTH,
                                                                  input_precision, input_scale);
    TypeDescriptor to_type = TypeDescriptor::from_primtive_type(ToType, TypeDescriptor::MAX_VARCHAR_LENGTH,
                                                                output_precision, output_scale);

    auto s = Slice{input_value.c_str(), input_value.size()};
    auto from_column =
            cast<TYPE_VARCHAR, FromType, PackedType>(s, string_type, from_type, front_fill_size, rear_fill_size);
    auto to_column = cast<FromType>(from_column, from_type, to_type);
    auto str_column = cast<ToType>(to_column, to_type, string_type);
    return str_column;
}

template <PrimitiveType FromType, PrimitiveType ToType>
void test_cast_const_null(CastTestCase const& tc, size_t front_fill_size, size_t rear_fill_size) {
    auto column =
            cast_single_test_case<FromType, ToType, ColumnPackedType::CONST_NULL>(tc, front_fill_size, rear_fill_size);
    ASSERT_TRUE(column->is_constant() && column->only_null());
}

template <PrimitiveType Type, typename = guard::Guard>
struct CastParamCppTypeTrait {
    using type = RunTimeCppType<Type>;
};
template <PrimitiveType Type>
struct CastParamCppTypeTrait<Type, StringPTGuard<Type>> {
    using type = std::string;
};

template <PrimitiveType Type>
struct CastParamCppTypeTrait<Type, DecimalV2PTGuard<Type>> {
    using type = int128_t;
};

template <PrimitiveType Type>
using CastParamCppType = typename CastParamCppTypeTrait<Type>::type;

template <PrimitiveType FromType, PrimitiveType ToType>
CastParamCppType<ToType> cast_value(CastParamCppType<FromType> const& value, int from_precision, int from_scale,
                                    int to_precision, int to_scale) {
    TypeDescriptor from_type = TypeDescriptor::from_primtive_type(FromType, 65535, from_precision, from_scale);
    TypeDescriptor to_type = TypeDescriptor::from_primtive_type(ToType, 65535, to_precision, to_scale);
    ColumnPtr column = cast<FromType, ToType, CONST>(RunTimeCppType<FromType>(value), from_type, to_type, 0, 0);
    DCHECK(column->is_constant() && !column->only_null());
    RunTimeCppType<ToType> result = ColumnHelper::get_const_value<ToType>(column);
    if constexpr (pt_is_string<ToType>) {
        return result.to_string();
    } else if constexpr (pt_is_decimalv2<ToType>) {
        return result.value();
    } else {
        return result;
    }
}

VALUE_GUARD(PrimitiveType, RelativeErrorPTGuard, pt_is_relative_error, TYPE_DECIMAL128, TYPE_FLOAT, TYPE_DOUBLE,
            TYPE_DECIMALV2)
template <PrimitiveType Type>
void assert_equal(std::string const& expect, std::string const& actual, [[maybe_unused]] int precision,
                  [[maybe_unused]] int scale) {
    if constexpr (pt_is_boolean<Type>) {
        std::string actual0 = std::string(actual == "1" ? "true" : "false");
        ASSERT_EQ(expect, actual0);
    } else if constexpr (pt_is_relative_error<Type>) {
        using CppType = CastParamCppType<Type>;
        auto expect_value = cast_value<TYPE_VARCHAR, Type>(expect, -1, -1, precision, scale);
        auto actual_value = cast_value<TYPE_VARCHAR, Type>(actual, -1, -1, precision, scale);
        auto delta = (expect_value - actual_value);
        double epsilon = abs(double(delta)) / double(expect_value + (expect_value == CppType(0)));
        ASSERT_TRUE(epsilon < 0.0000001);
    } else {
        compare_decimal_string(expect, actual, scale);
    }
}
template <PrimitiveType FromType, PrimitiveType ToType>
void test_cast_const(CastTestCase const& tc, size_t front_fill_size, size_t rear_fill_size) {
    auto column = cast_single_test_case<FromType, ToType, ColumnPackedType::CONST>(tc, front_fill_size, rear_fill_size);
    ASSERT_TRUE(column->is_constant() && !column->only_null());
    auto actual = ColumnHelper::get_const_value<TYPE_VARCHAR>(column).to_string();
    int precision = std::get<3>(tc);
    int scale = std::get<4>(tc);
    std::string expect = std::get<5>(tc);
    assert_equal<ToType>(expect, actual, precision, scale);
}

template <PrimitiveType FromType, PrimitiveType ToType>
void test_cast_nullable(CastTestCase const& tc, size_t front_fill_size, size_t rear_fill_size) {
    auto column =
            cast_single_test_case<FromType, ToType, ColumnPackedType::NULLABLE>(tc, front_fill_size, rear_fill_size);
    ASSERT_TRUE(column->is_nullable());
    auto data_column = ColumnHelper::get_data_column(column.get());
    auto binary_column = down_cast<BinaryColumn*>(data_column);
    auto actual = binary_column->get_slice(front_fill_size).to_string();
    int precision = std::get<3>(tc);
    int scale = std::get<4>(tc);
    std::string expect = std::get<5>(tc);
    assert_equal<ToType>(expect, actual, precision, scale);
}

template <PrimitiveType FromType, PrimitiveType ToType>
void test_cast_simple(CastTestCase const& tc, size_t front_fill_size, size_t rear_fill_size) {
    auto column =
            cast_single_test_case<FromType, ToType, ColumnPackedType::SIMPLE>(tc, front_fill_size, rear_fill_size);
    ASSERT_TRUE(column->is_binary() && !column->is_constant() && !column->only_null() && !column->is_nullable());
    auto binary_column = ColumnHelper::cast_to_raw<TYPE_VARCHAR>(column);
    auto actual = binary_column->get_slice(front_fill_size).to_string();
    int precision = std::get<3>(tc);
    int scale = std::get<4>(tc);
    std::string expect = std::get<5>(tc);
    assert_equal<ToType>(expect, actual, precision, scale);
}
template <PrimitiveType FromType, PrimitiveType ToType, ColumnPackedType PackedType>
void test_cast_fail(CastTestCase const& tc, size_t front_fill_size, size_t rear_fill_size) {
    auto column = cast_single_test_case<FromType, ToType, PackedType>(tc, front_fill_size, rear_fill_size);
    ASSERT_TRUE((column->is_constant() && column->only_null()) ||
                (column->is_nullable() && ColumnHelper::count_nulls(column) == column->size()));
}

template <PrimitiveType FromType, PrimitiveType ToType>
void test_cast_all(CastTestCaseArray const& test_cases) {
    size_t front_fill_size = 19;
    size_t rear_fill_size = 37;
    int i = -1;
    for (auto& tc : test_cases) {
        ++i;
        VLOG(10) << "test#" << i << ": input_precision=" << std::get<0>(tc) << ", input_scale=" << std::get<1>(tc)
                 << ", input_value=" << std::get<2>(tc) << ", output_precision=" << std::get<3>(tc)
                 << ", output_scale=" << std::get<4>(tc) << ", output_value=" << std::get<5>(tc) << std::endl;

        test_cast_const_null<FromType, ToType>(tc, front_fill_size, rear_fill_size);
        test_cast_simple<FromType, ToType>(tc, front_fill_size, rear_fill_size);
        test_cast_const<FromType, ToType>(tc, front_fill_size, rear_fill_size);
        test_cast_nullable<FromType, ToType>(tc, front_fill_size, rear_fill_size);
    }
}

template <PrimitiveType FromType, PrimitiveType ToType>
void test_cast_all_fail(CastTestCaseArray const& test_cases) {
    size_t front_fill_size = 19;
    size_t rear_fill_size = 37;
    int i = -1;
    for (auto& tc : test_cases) {
        ++i;
        VLOG(10) << "fail_test#" << i << ": input_precision=" << std::get<0>(tc) << ", input_scale=" << std::get<1>(tc)
                 << ", input_value=" << std::get<2>(tc) << ", output_precision=" << std::get<3>(tc)
                 << ", output_scale=" << std::get<4>(tc) << ", output_value=" << std::get<5>(tc) << std::endl;

        test_cast_fail<FromType, ToType, ColumnPackedType::SIMPLE>(tc, front_fill_size, rear_fill_size);
        test_cast_fail<FromType, ToType, ColumnPackedType::CONST_NULL>(tc, front_fill_size, rear_fill_size);
        test_cast_fail<FromType, ToType, ColumnPackedType::CONST>(tc, front_fill_size, rear_fill_size);
        test_cast_fail<FromType, ToType, ColumnPackedType::NULLABLE>(tc, front_fill_size, rear_fill_size);
    }
}

} // namespace starrocks::vectorized
