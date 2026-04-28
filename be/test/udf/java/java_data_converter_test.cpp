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

#include "udf/java/java_data_converter.h"

#include <gtest/gtest.h>

#include <vector>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/array_column.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/decimalv3_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/runtime_type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "types/datum.h"
#include "types/decimalv3.h"
#include "types/logical_type.h"
#include "udf/java/java_udf.h"

namespace starrocks {
// `assign_jvalue` is a free function defined in java_data_converter.cpp and only
// declared in java_window_function.h (which pulls in heavy aggregate-fn deps).
// Forward-declare it here so we can unit-test the DECIMAL branches directly.
Status assign_jvalue(const TypeDescriptor& type_desc, bool is_box, Column* col, int row_num, jvalue val,
                     bool error_if_overflow);

class DataConverterTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(DataConverterTest, cast_to_jval) {
    std::vector<LogicalType> types = {TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_VARCHAR};

    for (auto type : types) {
        TypeDescriptor tdesc(type);
        auto c1 = ColumnHelper::create_column(tdesc, true);
        c1->append_default();
        auto c2 = ColumnHelper::create_column(tdesc, false);
        c2->append_default();
        ASSIGN_OR_ASSERT_FAIL(jvalue val1, cast_to_jvalue(tdesc, true, c1.get(), 0));
        jobject obj1 = val1.l;
        LOCAL_REF_GUARD(obj1);
        ASSIGN_OR_ASSERT_FAIL(jvalue val2, cast_to_jvalue(tdesc, true, c2.get(), 0));
        jobject obj2 = val2.l;
        LOCAL_REF_GUARD(obj2);
    }

    for (auto type : types) {
        TypeDescriptor tdesc(TYPE_ARRAY);
        tdesc.children.emplace_back(type);
        auto c1 = ColumnHelper::create_column(tdesc, true);
        c1->append_default();
        auto c2 = ColumnHelper::create_column(tdesc, false);
        c2->append_default();
        ASSIGN_OR_ASSERT_FAIL(jvalue val1, cast_to_jvalue(tdesc, true, c1.get(), 0));
        jobject obj1 = val1.l;
        LOCAL_REF_GUARD(obj1);
        ASSIGN_OR_ASSERT_FAIL(jvalue val2, cast_to_jvalue(tdesc, true, c2.get(), 0));
        jobject obj2 = val2.l;
        LOCAL_REF_GUARD(obj2);
    }

    {
        TypeDescriptor tdesc(TYPE_ARRAY);
        tdesc.children.emplace_back(TYPE_INT);
        auto i32c = Int32Column::create();
        auto& elements_data = i32c->get_data();
        elements_data.resize(20);
        for (size_t i = 0; i < elements_data.size(); ++i) {
            elements_data[i] = i;
        }
        auto nullable = NullableColumn::wrap_if_necessary(std::move(i32c));
        auto offsets = UInt32Column::create();
        auto& offsets_data = offsets->get_data();
        offsets_data.emplace_back(0);
        offsets_data.emplace_back(2);
        offsets_data.emplace_back(10);
        offsets_data.emplace_back(20);
        std::string target;
        auto arr_col = ArrayColumn::create(std::move(nullable), std::move(offsets));
        auto& instance = JVMFunctionHelper::getInstance();
        for (size_t i = 0; i < 3; ++i) {
            ASSIGN_OR_ASSERT_FAIL(jvalue v, cast_to_jvalue(tdesc, true, arr_col.get(), i));
            jobject obj = v.l;
            target = target + instance.to_string(obj);
            LOCAL_REF_GUARD(obj);
        }
        ASSERT_EQ(target, "[0, 1][2, 3, 4, 5, 6, 7, 8, 9][10, 11, 12, 13, 14, 15, 16, 17, 18, 19]");
    }

    for (auto type2 : types) {
        for (auto type1 : types) {
            TypeDescriptor tdesc(TYPE_MAP);
            tdesc.children.emplace_back(type1);
            tdesc.children.emplace_back(type2);
            auto c1 = ColumnHelper::create_column(tdesc, true);
            c1->append_default();
            auto c2 = ColumnHelper::create_column(tdesc, false);
            c2->append_default();
            ASSIGN_OR_ASSERT_FAIL(jvalue val1, cast_to_jvalue(tdesc, true, c1.get(), 0));
            jobject obj1 = val1.l;
            LOCAL_REF_GUARD(obj1);
            ASSIGN_OR_ASSERT_FAIL(jvalue val2, cast_to_jvalue(tdesc, true, c2.get(), 0));
            jobject obj2 = val2.l;
            LOCAL_REF_GUARD(obj2);
        }
    }

    {
        TypeDescriptor tdesc(TYPE_MAP);
        TypeDescriptor ta(TYPE_ARRAY);
        ta.children.emplace_back(TYPE_INT);
        tdesc.children.emplace_back(ta);
        tdesc.children.emplace_back(ta);
        auto c1 = ColumnHelper::create_column(tdesc, true);
        c1->append_default();
        auto c2 = ColumnHelper::create_column(tdesc, false);
        c2->append_default();
        ASSIGN_OR_ASSERT_FAIL(jvalue val1, cast_to_jvalue(tdesc, true, c1.get(), 0));
        jobject obj1 = val1.l;
        LOCAL_REF_GUARD(obj1);
        ASSIGN_OR_ASSERT_FAIL(jvalue val2, cast_to_jvalue(tdesc, true, c2.get(), 0));
        jobject obj2 = val2.l;
        LOCAL_REF_GUARD(obj2);
    }

    {
        TypeDescriptor tdesc(TYPE_MAP);
        TypeDescriptor ta(TYPE_INT);
        tdesc.children.emplace_back(ta);
        tdesc.children.emplace_back(ta);

        auto keys = Int32Column::create();
        auto& elements_data = keys->get_data();
        elements_data.resize(20);
        for (size_t i = 0; i < elements_data.size(); ++i) {
            elements_data[i] = i;
        }
        auto nullable = NullableColumn::wrap_if_necessary(std::move(keys));
        auto offsets = UInt32Column::create();
        auto& offsets_data = offsets->get_data();

        offsets_data.emplace_back(0);
        offsets_data.emplace_back(2);
        offsets_data.emplace_back(10);
        offsets_data.emplace_back(20);

        auto values = Int32Column::create();
        auto& values_data = values->get_data();
        values_data.resize(elements_data.size());
        for (size_t i = 0; i < elements_data.size(); ++i) {
            values_data[i] = elements_data.size() - i;
        }
        auto vnullable = NullableColumn::wrap_if_necessary(std::move(values));
        auto map_column = MapColumn::create(std::move(nullable), std::move(vnullable), std::move(offsets));
        ASSIGN_OR_ASSERT_FAIL(jvalue val, cast_to_jvalue(tdesc, true, map_column.get(), 0));
        jobject obj = val.l;
        LOCAL_REF_GUARD(obj);
        auto& instance = JVMFunctionHelper::getInstance();
        std::string result = instance.to_string(obj);
        ASSERT_EQ("{0=20, 1=19}", result);
    }
}

TEST_F(DataConverterTest, convert_to_boxed_array) {
    std::vector<LogicalType> types = {TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT, TYPE_VARCHAR};
    FunctionContext context;
    std::vector<FunctionContext::TypeDesc> args;
    for (auto type : types) {
        args.clear();
        args.emplace_back(type);
        context._arg_types = args;
        TypeDescriptor tdesc(TYPE_MAP);
        TypeDescriptor ta(TYPE_ARRAY);
        ta.children.emplace_back(TYPE_INT);
        tdesc.children.emplace_back(ta);
        tdesc.children.emplace_back(ta);

        auto c1 = ColumnHelper::create_column(tdesc, true);
        c1->append_default();
        auto c2 = ColumnHelper::create_column(tdesc, false);
        c2->append_default();

        std::vector<jobject> res;
        std::vector<const Column*> columns;
        columns.resize(1);
        columns[0] = c1.get();
        ASSERT_OK(JavaDataTypeConverter::convert_to_boxed_array(&context, columns.data(), 1, 1, &res));

        res.clear();
        columns[0] = c2.get();
        ASSERT_OK(JavaDataTypeConverter::convert_to_boxed_array(&context, columns.data(), 1, 1, &res));
    }
}

TEST_F(DataConverterTest, append_jvalue) {
    {
        TypeDescriptor tdesc(TYPE_ARRAY);
        tdesc.children.emplace_back(TYPE_INT);
        auto c1 = ColumnHelper::create_column(tdesc, true);
        auto c2 = ColumnHelper::create_column(tdesc, true);
        Datum datum = std::vector<Datum>{1, 2, 3};
        c2->append_datum(datum);
        ASSIGN_OR_ASSERT_FAIL(jvalue val2, cast_to_jvalue(tdesc, true, c2.get(), 0));
        jobject obj = val2.l;
        LOCAL_REF_GUARD(obj);
        ASSERT_OK(append_jvalue(tdesc, true, c1.get(), val2));
    }
    {
        TypeDescriptor tdesc(TYPE_MAP);
        tdesc.children.emplace_back(TYPE_INT);
        tdesc.children.emplace_back(TYPE_INT);
        auto c1 = ColumnHelper::create_column(tdesc, true);
        auto c2 = ColumnHelper::create_column(tdesc, true);
        c2->append_datum(DatumMap{{1, 34}});
        ASSIGN_OR_ASSERT_FAIL(jvalue val2, cast_to_jvalue(tdesc, true, c2.get(), 0));
        jobject obj = val2.l;
        LOCAL_REF_GUARD(obj);
        ASSERT_OK(append_jvalue(tdesc, true, c1.get(), val2));
        ASSERT_EQ(c1->debug_item(0), "{1:34}");
    }
}

namespace {
// Build a single-row nullable DECIMAL column and populate row 0 from a literal.
template <LogicalType TYPE, typename T>
ColumnPtr build_decimal_col(int precision, int scale, const std::string& literal) {
    using ColumnT = RunTimeColumnType<TYPE>;
    auto col = ColumnT::create(precision, scale, 1);
    auto& data = down_cast<ColumnT*>(col.get())->get_data();
    DecimalV3Cast::from_string<T>(&data[0], precision, scale, literal.c_str(), literal.size());
    auto nulls = NullColumn::create(1, 0);
    return NullableColumn::create(std::move(col), std::move(nulls));
}
} // namespace

// Round-trip a DECIMAL value through cast_to_jvalue -> append_jvalue to make sure
// DECIMAL32/64/128/256 ↔ java.math.BigDecimal plumbing is correct end-to-end.
TEST_F(DataConverterTest, decimal_roundtrip) {
    struct Case {
        LogicalType type;
        int precision;
        int scale;
        std::string literal;
    };

    auto check = [](LogicalType type, int precision, int scale, const std::string& literal, const ColumnPtr& src) {
        TypeDescriptor tdesc = TypeDescriptor::create_decimalv3_type(type, precision, scale);
        ASSIGN_OR_ASSERT_FAIL(jvalue val, cast_to_jvalue(tdesc, true, src.get(), 0));
        jobject obj = val.l;
        LOCAL_REF_GUARD(obj);
        ASSERT_NE(obj, nullptr) << "cast_to_jvalue returned null for " << literal;

        auto dst = ColumnHelper::create_column(tdesc, true);
        ASSERT_OK(append_jvalue(tdesc, true, dst.get(), val));
        EXPECT_EQ(src->debug_item(0), dst->debug_item(0))
                << "round-trip mismatch for " << tdesc.debug_string() << " literal=" << literal;
    };

    check(TYPE_DECIMAL32, 9, 2, "12345.67", build_decimal_col<TYPE_DECIMAL32, int32_t>(9, 2, "12345.67"));
    check(TYPE_DECIMAL32, 9, 0, "-987654321", build_decimal_col<TYPE_DECIMAL32, int32_t>(9, 0, "-987654321"));
    check(TYPE_DECIMAL64, 18, 4, "1234567890.1234",
          build_decimal_col<TYPE_DECIMAL64, int64_t>(18, 4, "1234567890.1234"));
    check(TYPE_DECIMAL64, 18, 0, "9223372036854775807",
          build_decimal_col<TYPE_DECIMAL64, int64_t>(18, 0, "9223372036854775807"));
    check(TYPE_DECIMAL128, 38, 10, "12345678901234567890.1234567890",
          build_decimal_col<TYPE_DECIMAL128, int128_t>(38, 10, "12345678901234567890.1234567890"));
    check(TYPE_DECIMAL128, 38, 0, "-170141183460469231731687303715884105727",
          build_decimal_col<TYPE_DECIMAL128, int128_t>(38, 0, "-170141183460469231731687303715884105727"));
    check(TYPE_DECIMAL256, 76, 10, "1234567890123456789012345678901234567890.1234567890",
          build_decimal_col<TYPE_DECIMAL256, int256_t>(76, 10, "1234567890123456789012345678901234567890.1234567890"));
    check(TYPE_DECIMAL256, 76, 0, "-1234567890123456789012345678901234567890",
          build_decimal_col<TYPE_DECIMAL256, int256_t>(76, 0, "-1234567890123456789012345678901234567890"));
}

// BigDecimal values that exceed the target DECIMAL(p,s) range are either rejected (REPORT_ERROR
// = default) or silently nulled out (OUTPUT_NULL), matching built-in decimal cast semantics.
TEST_F(DataConverterTest, append_jvalue_decimal_overflow) {
    auto& helper = JVMFunctionHelper::getInstance();
    TypeDescriptor tdesc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 5, 2); // max 999.99

    // Build a BigDecimal with a value that fits in BigDecimal but not in DECIMAL64(5,2).
    auto source_tdesc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 0);
    auto src = build_decimal_col<TYPE_DECIMAL128, int128_t>(38, 0, "1234567890");
    ASSIGN_OR_ASSERT_FAIL(jvalue overflow_val, cast_to_jvalue(source_tdesc, true, src.get(), 0));
    jobject obj = overflow_val.l;
    LOCAL_REF_GUARD(obj);

    // REPORT_ERROR: append_jvalue returns a non-OK Status.
    {
        auto dst = ColumnHelper::create_column(tdesc, true);
        auto st = append_jvalue(tdesc, true, dst.get(), overflow_val, /*error_if_overflow=*/true);
        EXPECT_FALSE(st.ok());
    }
    // OUTPUT_NULL: append_jvalue succeeds and appends a null row.
    {
        auto dst = ColumnHelper::create_column(tdesc, true);
        auto st = append_jvalue(tdesc, true, dst.get(), overflow_val, /*error_if_overflow=*/false);
        ASSERT_OK(st);
        ASSERT_EQ(dst->size(), 1);
        EXPECT_TRUE(dst->is_null(0));
    }
}

// Round-trip a DECIMAL value through cast_to_jvalue -> assign_jvalue at a specified row.
// This is the JavaWindowFunction get_values path (BigDecimal -> per-row DECIMAL slot)
// and exercises both the DECIMAL32/64 unscaled-long branch and the DECIMAL128/256
// LE-bytes branch.
TEST_F(DataConverterTest, assign_jvalue_decimal_roundtrip) {
    auto check = [](LogicalType type, int precision, int scale, const std::string& literal, const ColumnPtr& src) {
        TypeDescriptor tdesc = TypeDescriptor::create_decimalv3_type(type, precision, scale);
        ASSIGN_OR_ASSERT_FAIL(jvalue val, cast_to_jvalue(tdesc, true, src.get(), 0));
        jobject obj = val.l;
        LOCAL_REF_GUARD(obj);
        ASSERT_NE(obj, nullptr) << "cast_to_jvalue returned null for " << literal;

        auto dst = ColumnHelper::create_column(tdesc, true);
        dst->resize(2);
        ASSERT_OK(assign_jvalue(tdesc, true, dst.get(), 1, val, /*error_if_overflow=*/true));
        EXPECT_EQ(src->debug_item(0), dst->debug_item(1))
                << "round-trip mismatch for " << tdesc.debug_string() << " literal=" << literal;
    };

    check(TYPE_DECIMAL32, 9, 2, "12345.67", build_decimal_col<TYPE_DECIMAL32, int32_t>(9, 2, "12345.67"));
    check(TYPE_DECIMAL64, 18, 4, "1234567890.1234",
          build_decimal_col<TYPE_DECIMAL64, int64_t>(18, 4, "1234567890.1234"));
    check(TYPE_DECIMAL128, 38, 10, "12345678901234567890.1234567890",
          build_decimal_col<TYPE_DECIMAL128, int128_t>(38, 10, "12345678901234567890.1234567890"));
    check(TYPE_DECIMAL256, 76, 10, "1234567890123456789012345678901234567890.1234567890",
          build_decimal_col<TYPE_DECIMAL256, int256_t>(76, 10, "1234567890123456789012345678901234567890.1234567890"));
}

// `assign_jvalue` writes per-row, so a null jvalue on a nullable column must set the
// null bit at the chosen row without touching the data slot.
TEST_F(DataConverterTest, assign_jvalue_decimal_null) {
    TypeDescriptor tdesc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 0);
    auto dst = ColumnHelper::create_column(tdesc, true);
    dst->resize(1);
    ASSERT_OK(assign_jvalue(tdesc, true, dst.get(), 0, jvalue{.l = nullptr}, /*error_if_overflow=*/true));
    EXPECT_TRUE(dst->is_null(0));
}

// assign_jvalue must surface the JNI ArithmeticException (REPORT_ERROR) and clear
// it from the JVM, leaving the destination cell untouched. With OUTPUT_NULL the
// row is nulled and Status is OK.
TEST_F(DataConverterTest, assign_jvalue_decimal_overflow) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();
    auto narrow_tdesc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 5, 2); // narrow target
    auto wide_tdesc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 0);

    // Build a too-large BigDecimal that does not fit into DECIMAL64(5,2).
    auto src = build_decimal_col<TYPE_DECIMAL128, int128_t>(38, 0, "1234567890");
    ASSIGN_OR_ASSERT_FAIL(jvalue overflow_val, cast_to_jvalue(wide_tdesc, true, src.get(), 0));
    jobject obj = overflow_val.l;
    LOCAL_REF_GUARD(obj);

    // REPORT_ERROR: returns a non-OK Status and clears the pending exception.
    {
        auto dst = ColumnHelper::create_column(narrow_tdesc, true);
        dst->resize(1);
        auto st = assign_jvalue(narrow_tdesc, true, dst.get(), 0, overflow_val, /*error_if_overflow=*/true);
        EXPECT_FALSE(st.ok());
        EXPECT_FALSE(env->ExceptionCheck());
    }
    // OUTPUT_NULL: returns OK and nullifies the row.
    {
        auto dst = ColumnHelper::create_column(narrow_tdesc, true);
        dst->resize(1);
        auto st = assign_jvalue(narrow_tdesc, true, dst.get(), 0, overflow_val, /*error_if_overflow=*/false);
        ASSERT_OK(st);
        EXPECT_TRUE(dst->is_null(0));
        EXPECT_FALSE(env->ExceptionCheck());
    }

    // Same overflow check on the DECIMAL128 wide-byte path.
    auto narrow128 = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 5, 2);
    {
        auto dst = ColumnHelper::create_column(narrow128, true);
        dst->resize(1);
        auto st = assign_jvalue(narrow128, true, dst.get(), 0, overflow_val, /*error_if_overflow=*/true);
        EXPECT_FALSE(st.ok());
        EXPECT_FALSE(env->ExceptionCheck());
    }
    {
        auto dst = ColumnHelper::create_column(narrow128, true);
        dst->resize(1);
        auto st = assign_jvalue(narrow128, true, dst.get(), 0, overflow_val, /*error_if_overflow=*/false);
        ASSERT_OK(st);
        EXPECT_TRUE(dst->is_null(0));
        EXPECT_FALSE(env->ExceptionCheck());
    }
}

// `check_type_matched` rejects mismatched objects for DECIMAL columns and accepts
// BigDecimal. Used by UDTF return-row validation, so make sure the DECIMAL branch
// is exercised.
TEST_F(DataConverterTest, check_type_matched_decimal) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();

    // BigDecimal -> DECIMAL is OK (also tests the four LogicalType variants).
    auto src = build_decimal_col<TYPE_DECIMAL64, int64_t>(18, 4, "1234.5678");
    auto wide_tdesc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 4);
    ASSIGN_OR_ASSERT_FAIL(jvalue val, cast_to_jvalue(wide_tdesc, true, src.get(), 0));
    jobject big_decimal_obj = val.l;
    LOCAL_REF_GUARD(big_decimal_obj);

    for (auto type : {TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128, TYPE_DECIMAL256}) {
        auto td = TypeDescriptor::create_decimalv3_type(type, 18, 4);
        EXPECT_OK(check_type_matched(td, big_decimal_obj));
    }

    // null val -> always OK regardless of expected type.
    EXPECT_OK(check_type_matched(wide_tdesc, nullptr));

    // String -> DECIMAL is rejected.
    jobject jstr = helper.newString("not-a-decimal", 13);
    LOCAL_REF_GUARD(jstr);
    auto st = check_type_matched(wide_tdesc, jstr);
    EXPECT_FALSE(st.ok());
    EXPECT_FALSE(env->ExceptionCheck());
}

// convert_to_boxed_array on a DECIMAL column drives `build_decimal_boxed_array`,
// which is the batched UDF input path for native DECIMAL columns.
TEST_F(DataConverterTest, convert_to_boxed_array_decimal) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();

    auto run = [&](LogicalType type, int precision, int scale, const ColumnPtr& col) {
        auto tdesc = TypeDescriptor::create_decimalv3_type(type, precision, scale);
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context({tdesc}, tdesc));

        std::vector<jobject> res;
        std::vector<const Column*> cols = {col.get()};
        ASSERT_OK(JavaDataTypeConverter::convert_to_boxed_array(ctx.get(), cols.data(), 1, col->size(), &res));
        ASSERT_EQ(res.size(), 1);
        ASSERT_NE(res[0], nullptr);
        env->DeleteLocalRef(res[0]);
    };

    // Build a populated nullable DECIMAL column with the given precision/scale and
    // dispatch it through the decimal-aware boxed array helper. Covers the four
    // DECIMAL widths via wrap_decimal_data<TYPE_DECIMAL*>.
    run(TYPE_DECIMAL32, 9, 2, build_decimal_col<TYPE_DECIMAL32, int32_t>(9, 2, "12345.67"));
    run(TYPE_DECIMAL64, 18, 4, build_decimal_col<TYPE_DECIMAL64, int64_t>(18, 4, "1234567890.1234"));
    run(TYPE_DECIMAL128, 38, 10,
        build_decimal_col<TYPE_DECIMAL128, int128_t>(38, 10, "12345678901234567890.1234567890"));
    run(TYPE_DECIMAL256, 76, 10,
        build_decimal_col<TYPE_DECIMAL256, int256_t>(76, 10, "1234567890123456789012345678901234567890.1234567890"));

    // all-null shortcut path: every row null hits the create_array branch instead of
    // the decimal helper. Keep this case to exercise the early-return.
    {
        auto tdesc = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 4);
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context({tdesc}, tdesc));

        auto col = ColumnHelper::create_column(tdesc, true);
        col->append_nulls(3);

        std::vector<jobject> res;
        std::vector<const Column*> cols = {col.get()};
        ASSERT_OK(JavaDataTypeConverter::convert_to_boxed_array(ctx.get(), cols.data(), 1, 3, &res));
        ASSERT_EQ(res.size(), 1);
        ASSERT_NE(res[0], nullptr);
        env->DeleteLocalRef(res[0]);
    }
}

// ARRAY<DECIMAL> drives the DECIMAL specialization of JavaArrayConverter through the
// elements_column accept(). The outer ARRAY routes to JavaArrayConverter (not the
// build_decimal_boxed_array fast path) because is_decimalv3_field_type(TYPE_ARRAY)
// is false; the elements then hit the int32/int64/int128/int256 branches.
TEST_F(DataConverterTest, convert_to_boxed_array_array_of_decimal) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();

    auto run = [&](LogicalType type, int precision, int scale) {
        TypeDescriptor element = TypeDescriptor::create_decimalv3_type(type, precision, scale);
        TypeDescriptor array_desc = TypeDescriptor::create_array_type(element);
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context({array_desc}, array_desc));

        // Non-nullable ARRAY<DECIMAL> with one empty array row. The visitor recurses
        // into the (empty) DECIMAL elements column regardless, which is what we want
        // to cover for JavaArrayConverter::do_visit(DecimalV3Column<T>).
        auto col = ColumnHelper::create_column(array_desc, false);
        col->append_default();

        std::vector<jobject> res;
        std::vector<const Column*> cols = {col.get()};
        ASSERT_OK(JavaDataTypeConverter::convert_to_boxed_array(ctx.get(), cols.data(), 1, col->size(), &res));
        ASSERT_EQ(res.size(), 1);
        ASSERT_NE(res[0], nullptr);
        env->DeleteLocalRef(res[0]);
    };

    run(TYPE_DECIMAL32, 9, 2);
    run(TYPE_DECIMAL64, 18, 4);
    run(TYPE_DECIMAL128, 38, 10);
    run(TYPE_DECIMAL256, 76, 10);
}

} // namespace starrocks