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
#include "column/const_column.h"
#include "column/decimalv3_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/runtime_type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/statusor.h"
#include "types/date_value.h"
#include "types/datum.h"
#include "types/decimalv3.h"
#include "types/logical_type.h"
#include "types/timestamp_value.h"
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

// ============================================================================
// DATE / DATETIME tests. Wire format mirrors BE column storage:
//   - DATE     : int32 Julian day (DateValue::_julian)
//   - DATETIME : packed int64 (julian << 40 | microseconds_of_day)
// Round-trips exercise cast_to_jvalue (BE -> LocalDate/LocalDateTime) and
// assign_jvalue / append_jvalue (LocalDate/LocalDateTime -> BE).
// ============================================================================

namespace {
ColumnPtr make_date_column(std::initializer_list<DateValue> rows) {
    auto data = DateColumn::create();
    for (auto v : rows) {
        data->append(v);
    }
    auto nulls = NullColumn::create(rows.size(), 0);
    return NullableColumn::create(std::move(data), std::move(nulls));
}

ColumnPtr make_datetime_column(std::initializer_list<TimestampValue> rows) {
    auto data = TimestampColumn::create();
    for (auto v : rows) {
        data->append(v);
    }
    auto nulls = NullColumn::create(rows.size(), 0);
    return NullableColumn::create(std::move(data), std::move(nulls));
}
} // namespace

// cast_to_jvalue must produce a LocalDate / LocalDateTime whose Java value
// matches the BE-side row, validated via the corresponding val{LocalDate,
// LocalDateTime} accessor.
TEST_F(DataConverterTest, cast_to_jvalue_date_datetime) {
    auto& helper = JVMFunctionHelper::getInstance();

    {
        TypeDescriptor td(TYPE_DATE);
        DateValue dv = DateValue::create(2024, 1, 15);
        auto src = make_date_column({dv});
        ASSIGN_OR_ASSERT_FAIL(jvalue val, cast_to_jvalue(td, true, src.get(), 0));
        jobject obj = val.l;
        LOCAL_REF_GUARD(obj);
        ASSERT_NE(obj, nullptr);
        EXPECT_EQ(dv._julian, helper.valLocalDate(obj));
    }
    {
        TypeDescriptor td(TYPE_DATETIME);
        TimestampValue tv = TimestampValue::create(2024, 1, 15, 12, 34, 56, 789000);
        auto src = make_datetime_column({tv});
        ASSIGN_OR_ASSERT_FAIL(jvalue val, cast_to_jvalue(td, true, src.get(), 0));
        jobject obj = val.l;
        LOCAL_REF_GUARD(obj);
        ASSERT_NE(obj, nullptr);
        EXPECT_EQ(tv.timestamp(), helper.valLocalDateTime(obj));
    }
}

// Null cells short-circuit cast_to_jvalue to a null jobject (no JNI call), so
// the resulting jvalue.l must be nullptr without throwing.
TEST_F(DataConverterTest, cast_to_jvalue_date_datetime_null) {
    {
        TypeDescriptor td(TYPE_DATE);
        auto col = ColumnHelper::create_column(td, true);
        col->append_nulls(1);
        ASSIGN_OR_ASSERT_FAIL(jvalue val, cast_to_jvalue(td, true, col.get(), 0));
        EXPECT_EQ(val.l, nullptr);
    }
    {
        TypeDescriptor td(TYPE_DATETIME);
        auto col = ColumnHelper::create_column(td, true);
        col->append_nulls(1);
        ASSIGN_OR_ASSERT_FAIL(jvalue val, cast_to_jvalue(td, true, col.get(), 0));
        EXPECT_EQ(val.l, nullptr);
    }
}

// Round-trip a DATE / DATETIME value through cast_to_jvalue -> append_jvalue
// (the UDAF finalize / UDTF emit path).
TEST_F(DataConverterTest, append_jvalue_date_datetime_roundtrip) {
    {
        TypeDescriptor td(TYPE_DATE);
        DateValue values[] = {
                DateValue::create(1970, 1, 1),
                DateValue::create(2024, 12, 31),
                DateValue::create(1, 1, 1),
        };
        for (auto v : values) {
            auto src = make_date_column({v});
            ASSIGN_OR_ASSERT_FAIL(jvalue val, cast_to_jvalue(td, true, src.get(), 0));
            jobject obj = val.l;
            LOCAL_REF_GUARD(obj);
            auto dst = ColumnHelper::create_column(td, true);
            ASSERT_OK(append_jvalue(td, true, dst.get(), val));
            EXPECT_EQ(src->debug_item(0), dst->debug_item(0));
        }
    }
    {
        TypeDescriptor td(TYPE_DATETIME);
        TimestampValue values[] = {
                TimestampValue::create(1970, 1, 1, 0, 0, 0, 0),
                TimestampValue::create(2024, 1, 15, 12, 34, 56, 789000),
                TimestampValue::create(9999, 12, 31, 23, 59, 59, 999999),
        };
        for (auto v : values) {
            auto src = make_datetime_column({v});
            ASSIGN_OR_ASSERT_FAIL(jvalue val, cast_to_jvalue(td, true, src.get(), 0));
            jobject obj = val.l;
            LOCAL_REF_GUARD(obj);
            auto dst = ColumnHelper::create_column(td, true);
            ASSERT_OK(append_jvalue(td, true, dst.get(), val));
            EXPECT_EQ(src->debug_item(0), dst->debug_item(0));
        }
    }
}

// append_jvalue with a null jvalue on a nullable column appends a null row.
TEST_F(DataConverterTest, append_jvalue_date_datetime_null) {
    for (auto type : {TYPE_DATE, TYPE_DATETIME}) {
        TypeDescriptor td(type);
        auto dst = ColumnHelper::create_column(td, true);
        ASSERT_OK(append_jvalue(td, true, dst.get(), jvalue{.l = nullptr}));
        ASSERT_EQ(dst->size(), 1);
        EXPECT_TRUE(dst->is_null(0));
    }
}

// Round-trip via assign_jvalue at a specified row (Java window function path).
TEST_F(DataConverterTest, assign_jvalue_date_datetime_roundtrip) {
    {
        TypeDescriptor td(TYPE_DATE);
        DateValue dv = DateValue::create(2024, 1, 15);
        auto src = make_date_column({dv});
        ASSIGN_OR_ASSERT_FAIL(jvalue val, cast_to_jvalue(td, true, src.get(), 0));
        jobject obj = val.l;
        LOCAL_REF_GUARD(obj);
        auto dst = ColumnHelper::create_column(td, true);
        dst->resize(2);
        ASSERT_OK(assign_jvalue(td, true, dst.get(), 1, val, /*error_if_overflow=*/true));
        EXPECT_EQ(src->debug_item(0), dst->debug_item(1));
    }
    {
        TypeDescriptor td(TYPE_DATETIME);
        TimestampValue tv = TimestampValue::create(2024, 1, 15, 12, 34, 56, 789000);
        auto src = make_datetime_column({tv});
        ASSIGN_OR_ASSERT_FAIL(jvalue val, cast_to_jvalue(td, true, src.get(), 0));
        jobject obj = val.l;
        LOCAL_REF_GUARD(obj);
        auto dst = ColumnHelper::create_column(td, true);
        dst->resize(2);
        ASSERT_OK(assign_jvalue(td, true, dst.get(), 1, val, /*error_if_overflow=*/true));
        EXPECT_EQ(src->debug_item(0), dst->debug_item(1));
    }
}

// assign_jvalue with a null jvalue on a nullable column sets the null bit at
// the chosen row without touching the data slot.
TEST_F(DataConverterTest, assign_jvalue_date_datetime_null) {
    for (auto type : {TYPE_DATE, TYPE_DATETIME}) {
        TypeDescriptor td(type);
        auto dst = ColumnHelper::create_column(td, true);
        dst->resize(1);
        ASSERT_OK(assign_jvalue(td, true, dst.get(), 0, jvalue{.l = nullptr}, /*error_if_overflow=*/true));
        EXPECT_TRUE(dst->is_null(0));
    }
}

// check_type_matched accepts LocalDate/LocalDateTime for their respective types
// and rejects mismatches (e.g. swapping the two, or passing a String).
TEST_F(DataConverterTest, check_type_matched_date_datetime) {
    auto& helper = JVMFunctionHelper::getInstance();
    TypeDescriptor date_td(TYPE_DATE);
    TypeDescriptor dt_td(TYPE_DATETIME);

    auto date_src = make_date_column({DateValue::create(2024, 1, 15)});
    ASSIGN_OR_ASSERT_FAIL(jvalue date_val, cast_to_jvalue(date_td, true, date_src.get(), 0));
    jobject date_obj = date_val.l;
    LOCAL_REF_GUARD(date_obj);

    auto dt_src = make_datetime_column({TimestampValue::create(2024, 1, 15, 12, 0, 0, 0)});
    ASSIGN_OR_ASSERT_FAIL(jvalue dt_val, cast_to_jvalue(dt_td, true, dt_src.get(), 0));
    jobject dt_obj = dt_val.l;
    LOCAL_REF_GUARD(dt_obj);

    EXPECT_OK(check_type_matched(date_td, date_obj));
    EXPECT_OK(check_type_matched(dt_td, dt_obj));
    // null val is always OK.
    EXPECT_OK(check_type_matched(date_td, nullptr));
    EXPECT_OK(check_type_matched(dt_td, nullptr));

    // Cross-type: LocalDate vs DATETIME and LocalDateTime vs DATE both fail.
    EXPECT_FALSE(check_type_matched(date_td, dt_obj).ok());
    EXPECT_FALSE(check_type_matched(dt_td, date_obj).ok());

    // String against DATE / DATETIME is rejected.
    jobject jstr = helper.newString("not-a-date", 10);
    LOCAL_REF_GUARD(jstr);
    EXPECT_FALSE(check_type_matched(date_td, jstr).ok());
    EXPECT_FALSE(check_type_matched(dt_td, jstr).ok());
}

// convert_to_boxed_array drives the batch input path via JavaArrayConverter.
// For DATE / DATETIME this routes through the FixedLengthColumn<T> overload
// using the JNIPrimTypeId<DateValue> / JNIPrimTypeId<TimestampValue> id, which
// must be registered against createBoxedLocalDate{,Time}Array in _method_map.
TEST_F(DataConverterTest, convert_to_boxed_array_date_datetime) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();

    auto run = [&](LogicalType type, const ColumnPtr& col) {
        TypeDescriptor td(type);
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context({td}, td));
        std::vector<jobject> res;
        std::vector<const Column*> cols = {col.get()};
        ASSERT_OK(JavaDataTypeConverter::convert_to_boxed_array(ctx.get(), cols.data(), 1, col->size(), &res));
        ASSERT_EQ(res.size(), 1);
        ASSERT_NE(res[0], nullptr);
        env->DeleteLocalRef(res[0]);
    };

    run(TYPE_DATE, make_date_column({DateValue::create(2024, 1, 15), DateValue::create(1970, 1, 1)}));
    run(TYPE_DATETIME, make_datetime_column({TimestampValue::create(2024, 1, 15, 12, 34, 56, 0),
                                             TimestampValue::create(1970, 1, 1, 0, 0, 0, 0)}));

    // all-null short-circuit
    {
        TypeDescriptor td(TYPE_DATE);
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context({td}, td));
        auto col = ColumnHelper::create_column(td, true);
        col->append_nulls(3);
        std::vector<jobject> res;
        std::vector<const Column*> cols = {col.get()};
        ASSERT_OK(JavaDataTypeConverter::convert_to_boxed_array(ctx.get(), cols.data(), 1, 3, &res));
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

// build_udf_type_desc on a leaf scalar slot just dispatches to
// JVMFunctionHelper::new_udf_type_desc and ignores formal_type. Drive every
// scalar / decimal LogicalType to make sure the leaf branch is exercised
// across the type spectrum used by the UDF analyzer.
TEST_F(DataConverterTest, build_udf_type_desc_scalar_leaves) {
    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();

    auto check_leaf = [&](const TypeDescriptor& td) {
        ASSIGN_OR_ASSERT_FAIL(jobject desc, build_udf_type_desc(env, td, /*formal_type=*/nullptr));
        LOCAL_REF_GUARD(desc);
        ASSERT_NE(desc, nullptr);

        jclass cls = helper.udf_type_desc_class();
        EXPECT_EQ(static_cast<jint>(td.type), env->GetIntField(desc, env->GetFieldID(cls, "logicalType", "I")));
        // build_udf_type_desc forwards td.precision / td.scale verbatim. For
        // scalars TypeDescriptor leaves them at the sentinel (-1); DECIMAL
        // slots carry the declared precision/scale.
        EXPECT_EQ(td.precision, env->GetIntField(desc, env->GetFieldID(cls, "precision", "I")));
        EXPECT_EQ(td.scale, env->GetIntField(desc, env->GetFieldID(cls, "scale", "I")));
        // Leaf: no children, no record class.
        EXPECT_EQ(nullptr, env->GetObjectField(desc, helper.udf_type_desc_children_field()));
        EXPECT_EQ(nullptr, env->GetObjectField(desc, helper.udf_type_desc_record_class_field()));
    };

    for (auto t : {TYPE_BOOLEAN, TYPE_INT, TYPE_BIGINT, TYPE_DOUBLE, TYPE_VARCHAR, TYPE_DATE, TYPE_DATETIME}) {
        check_leaf(TypeDescriptor(t));
    }
    check_leaf(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 2));
    check_leaf(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 4));
    check_leaf(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 10));
    check_leaf(TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL256, 76, 10));
}

// convert_to_boxed_array passes a non-null arg_type_descs vector but with all
// entries null (no STRUCT in any subtree). The boxer must still fall back to
// JavaArrayConverter for ARRAY<scalar> / scalar slots — STRUCT-bearing routing
// is gated on the per-slot UdfTypeDesc being non-null.
TEST_F(DataConverterTest, convert_to_boxed_array_with_null_descs) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();

    // ARRAY<INT> column with one populated row.
    TypeDescriptor td = TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT));
    auto col = ColumnHelper::create_column(td, true);
    Datum datum = std::vector<Datum>{1, 2, 3};
    col->append_datum(datum);

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context({td}, td));
    std::vector<const Column*> cols = {col.get()};
    std::vector<jobject> arg_descs = {nullptr};
    std::vector<jobject> res;
    ASSERT_OK(JavaDataTypeConverter::convert_to_boxed_array(ctx.get(), cols.data(), 1, col->size(), &res, &arg_descs));
    ASSERT_EQ(res.size(), 1);
    ASSERT_NE(res[0], nullptr);
    env->DeleteLocalRef(res[0]);
}

// Constant column shortcut path of convert_to_boxed_array: the value is
// broadcast across num_rows via create_object_array. Drive a const INT to
// cover the non-STRUCT constant branch alongside the STRUCT-aware routing.
TEST_F(DataConverterTest, convert_to_boxed_array_constant_short_circuit) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();

    TypeDescriptor td(TYPE_INT);
    auto inner = ColumnHelper::create_column(td, false);
    inner->append_datum(Datum(int32_t{42}));
    auto const_col = ConstColumn::create(std::move(inner));
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context({td}, td));

    std::vector<const Column*> cols = {const_col.get()};
    std::vector<jobject> res;
    ASSERT_OK(JavaDataTypeConverter::convert_to_boxed_array(ctx.get(), cols.data(), 1, /*num_rows=*/4, &res));
    ASSERT_EQ(res.size(), 1);
    ASSERT_NE(res[0], nullptr);
    env->DeleteLocalRef(res[0]);
}

// only-null column shortcut: convert_to_boxed_array bypasses both the STRUCT
// path and the per-type Java helper, returning a plain Object[num_rows] of
// nulls via JVMFunctionHelper::create_array. Exercise the early-return so the
// branch is not stranded uncovered.
TEST_F(DataConverterTest, convert_to_boxed_array_all_null_short_circuit) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();

    TypeDescriptor td(TYPE_BIGINT);
    auto col = ColumnHelper::create_column(td, true);
    col->append_nulls(5);

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context({td}, td));
    std::vector<const Column*> cols = {col.get()};
    std::vector<jobject> res;
    ASSERT_OK(JavaDataTypeConverter::convert_to_boxed_array(ctx.get(), cols.data(), 1, /*num_rows=*/5, &res));
    ASSERT_EQ(res.size(), 1);
    ASSERT_NE(res[0], nullptr);
    env->DeleteLocalRef(res[0]);
}

namespace {
// Resolve a UdfTestSupport static method by name and return its
// java.lang.reflect.Method.getGenericReturnType() — gives the BE tests a
// concretely-parameterized java.lang.reflect.Type to feed into
// build_udf_type_desc without standing up a real UDF classloader.
jobject reflected_return_generic_type(JNIEnv* env, const char* method_name, const char* signature) {
    jclass support_cls = env->FindClass("com/starrocks/udf/UdfTestSupport");
    EXPECT_NE(support_cls, nullptr);
    if (support_cls == nullptr) return nullptr;

    jmethodID mid = env->GetStaticMethodID(support_cls, method_name, signature);
    EXPECT_NE(mid, nullptr) << method_name;
    jobject method_obj = env->ToReflectedMethod(support_cls, mid, JNI_TRUE);
    env->DeleteLocalRef(support_cls);
    if (method_obj == nullptr) return nullptr;

    jclass method_cls = env->FindClass("java/lang/reflect/Method");
    jmethodID get_generic = env->GetMethodID(method_cls, "getGenericReturnType", "()Ljava/lang/reflect/Type;");
    jobject formal = env->CallObjectMethod(method_obj, get_generic);
    env->DeleteLocalRef(method_cls);
    env->DeleteLocalRef(method_obj);
    return formal;
}

// Resolve UdfTestStructRecord.class.
jclass test_struct_record_class(JNIEnv* env) {
    return env->FindClass("com/starrocks/udf/UdfTestStructRecord");
}

// Build the canonical SQL TypeDescriptor matching UdfTestStructRecord:
// STRUCT<key INT, value VARCHAR>.
TypeDescriptor make_test_struct_typedesc() {
    TypeDescriptor td(TYPE_STRUCT);
    td.children.emplace_back(TYPE_INT);
    td.children.emplace_back(TYPE_VARCHAR);
    td.field_names = {"key", "value"};
    return td;
}
} // namespace

// build_udf_type_desc for a top-level STRUCT slot drills into the formal record
// class via getRecordComponents() and recursively builds child UdfTypeDescs for
// each field. Verifies recordClass round-trips and that the children array has
// the right shape.
TEST_F(DataConverterTest, build_udf_type_desc_struct_via_reflected_method) {
    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();

    jobject formal = reflected_return_generic_type(env, "structReturn", "()Lcom/starrocks/udf/UdfTestStructRecord;");
    ASSERT_NE(formal, nullptr);
    LOCAL_REF_GUARD(formal);

    TypeDescriptor td = make_test_struct_typedesc();
    ASSIGN_OR_ASSERT_FAIL(jobject desc, build_udf_type_desc(env, td, formal));
    LOCAL_REF_GUARD(desc);
    ASSERT_NE(desc, nullptr);

    jclass cls = helper.udf_type_desc_class();
    EXPECT_EQ(static_cast<jint>(TYPE_STRUCT), env->GetIntField(desc, env->GetFieldID(cls, "logicalType", "I")));

    // recordClass must be UdfTestStructRecord.
    jobject record_class = env->GetObjectField(desc, helper.udf_type_desc_record_class_field());
    LOCAL_REF_GUARD(record_class);
    ASSERT_NE(record_class, nullptr);
    jclass expected = test_struct_record_class(env);
    LOCAL_REF_GUARD(expected);
    EXPECT_TRUE(env->IsSameObject(record_class, expected));

    // children = [INT, VARCHAR]. Walking via the cached jfieldID exercises the
    // exact accessor the BE-side input boxer reads at runtime.
    jobjectArray children = (jobjectArray)env->GetObjectField(desc, helper.udf_type_desc_children_field());
    LOCAL_REF_GUARD(children);
    ASSERT_NE(children, nullptr);
    ASSERT_EQ(2, env->GetArrayLength(children));

    jobject child0 = env->GetObjectArrayElement(children, 0);
    LOCAL_REF_GUARD(child0);
    EXPECT_EQ(static_cast<jint>(TYPE_INT), env->GetIntField(child0, env->GetFieldID(cls, "logicalType", "I")));

    jobject child1 = env->GetObjectArrayElement(children, 1);
    LOCAL_REF_GUARD(child1);
    EXPECT_EQ(static_cast<jint>(TYPE_VARCHAR), env->GetIntField(child1, env->GetFieldID(cls, "logicalType", "I")));
}

// ARRAY<STRUCT> formal type drives the ARRAY branch: build_udf_type_desc reads
// the ParameterizedType actual arguments and recurses for the element. Element
// child carries the formal record class.
TEST_F(DataConverterTest, build_udf_type_desc_array_of_struct) {
    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();

    jobject formal = reflected_return_generic_type(env, "arrayOfStruct", "()Ljava/util/List;");
    ASSERT_NE(formal, nullptr);
    LOCAL_REF_GUARD(formal);

    TypeDescriptor element = make_test_struct_typedesc();
    TypeDescriptor td = TypeDescriptor::create_array_type(element);
    ASSIGN_OR_ASSERT_FAIL(jobject desc, build_udf_type_desc(env, td, formal));
    LOCAL_REF_GUARD(desc);
    ASSERT_NE(desc, nullptr);

    jclass cls = helper.udf_type_desc_class();
    EXPECT_EQ(static_cast<jint>(TYPE_ARRAY), env->GetIntField(desc, env->GetFieldID(cls, "logicalType", "I")));
    EXPECT_EQ(nullptr, env->GetObjectField(desc, helper.udf_type_desc_record_class_field()));

    jobjectArray children = (jobjectArray)env->GetObjectField(desc, helper.udf_type_desc_children_field());
    LOCAL_REF_GUARD(children);
    ASSERT_NE(children, nullptr);
    ASSERT_EQ(1, env->GetArrayLength(children));

    jobject elem_desc = env->GetObjectArrayElement(children, 0);
    LOCAL_REF_GUARD(elem_desc);
    EXPECT_EQ(static_cast<jint>(TYPE_STRUCT), env->GetIntField(elem_desc, env->GetFieldID(cls, "logicalType", "I")));
    jobject elem_record_class = env->GetObjectField(elem_desc, helper.udf_type_desc_record_class_field());
    LOCAL_REF_GUARD(elem_record_class);
    jclass expected = test_struct_record_class(env);
    LOCAL_REF_GUARD(expected);
    EXPECT_TRUE(env->IsSameObject(elem_record_class, expected));
}

// MAP<varchar, STRUCT> formal type drives the MAP branch: both the key and
// value types are walked. STRUCT-only-on-value still triggers the "extract two
// type arguments" recursion.
TEST_F(DataConverterTest, build_udf_type_desc_map_of_struct) {
    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();

    jobject formal = reflected_return_generic_type(env, "mapOfStruct", "()Ljava/util/Map;");
    ASSERT_NE(formal, nullptr);
    LOCAL_REF_GUARD(formal);

    TypeDescriptor td(TYPE_MAP);
    td.children.emplace_back(TYPE_VARCHAR);
    td.children.emplace_back(make_test_struct_typedesc());
    ASSIGN_OR_ASSERT_FAIL(jobject desc, build_udf_type_desc(env, td, formal));
    LOCAL_REF_GUARD(desc);
    ASSERT_NE(desc, nullptr);

    jclass cls = helper.udf_type_desc_class();
    EXPECT_EQ(static_cast<jint>(TYPE_MAP), env->GetIntField(desc, env->GetFieldID(cls, "logicalType", "I")));

    jobjectArray children = (jobjectArray)env->GetObjectField(desc, helper.udf_type_desc_children_field());
    LOCAL_REF_GUARD(children);
    ASSERT_NE(children, nullptr);
    ASSERT_EQ(2, env->GetArrayLength(children));

    jobject key = env->GetObjectArrayElement(children, 0);
    LOCAL_REF_GUARD(key);
    EXPECT_EQ(static_cast<jint>(TYPE_VARCHAR), env->GetIntField(key, env->GetFieldID(cls, "logicalType", "I")));

    jobject val = env->GetObjectArrayElement(children, 1);
    LOCAL_REF_GUARD(val);
    EXPECT_EQ(static_cast<jint>(TYPE_STRUCT), env->GetIntField(val, env->GetFieldID(cls, "logicalType", "I")));
    jobject val_record_class = env->GetObjectField(val, helper.udf_type_desc_record_class_field());
    LOCAL_REF_GUARD(val_record_class);
    jclass expected = test_struct_record_class(env);
    LOCAL_REF_GUARD(expected);
    EXPECT_TRUE(env->IsSameObject(val_record_class, expected));
}

// ARRAY<ARRAY<STRUCT>> exercises the recursion through two parameterized
// layers before hitting the STRUCT leaf, ensuring intermediate ARRAY rebuilds
// keep threading the formal record class through children[0].children[0].
TEST_F(DataConverterTest, build_udf_type_desc_array_of_array_of_struct) {
    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();

    jobject formal = reflected_return_generic_type(env, "arrayOfArrayOfStruct", "()Ljava/util/List;");
    ASSERT_NE(formal, nullptr);
    LOCAL_REF_GUARD(formal);

    TypeDescriptor inner_arr = TypeDescriptor::create_array_type(make_test_struct_typedesc());
    TypeDescriptor outer = TypeDescriptor::create_array_type(inner_arr);
    ASSIGN_OR_ASSERT_FAIL(jobject desc, build_udf_type_desc(env, outer, formal));
    LOCAL_REF_GUARD(desc);

    jclass cls = helper.udf_type_desc_class();
    EXPECT_EQ(static_cast<jint>(TYPE_ARRAY), env->GetIntField(desc, env->GetFieldID(cls, "logicalType", "I")));
    jobjectArray outer_children = (jobjectArray)env->GetObjectField(desc, helper.udf_type_desc_children_field());
    LOCAL_REF_GUARD(outer_children);
    jobject mid = env->GetObjectArrayElement(outer_children, 0);
    LOCAL_REF_GUARD(mid);
    EXPECT_EQ(static_cast<jint>(TYPE_ARRAY), env->GetIntField(mid, env->GetFieldID(cls, "logicalType", "I")));

    jobjectArray mid_children = (jobjectArray)env->GetObjectField(mid, helper.udf_type_desc_children_field());
    LOCAL_REF_GUARD(mid_children);
    jobject inner_struct = env->GetObjectArrayElement(mid_children, 0);
    LOCAL_REF_GUARD(inner_struct);
    EXPECT_EQ(static_cast<jint>(TYPE_STRUCT), env->GetIntField(inner_struct, env->GetFieldID(cls, "logicalType", "I")));
    jobject inner_record_class = env->GetObjectField(inner_struct, helper.udf_type_desc_record_class_field());
    LOCAL_REF_GUARD(inner_record_class);
    jclass expected = test_struct_record_class(env);
    LOCAL_REF_GUARD(expected);
    EXPECT_TRUE(env->IsSameObject(inner_record_class, expected));
}

// build_udf_type_desc rejects formal types that aren't Class / ParameterizedType
// in spots where it expects them. Hand a primitive-array formal at a STRUCT
// slot to make sure type_to_raw_class's negative path returns Status::Internal.
TEST_F(DataConverterTest, build_udf_type_desc_struct_rejects_non_class_formal) {
    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();

    // null formal at a STRUCT slot → "formal Java type is null".
    TypeDescriptor td = make_test_struct_typedesc();
    auto status = build_udf_type_desc(env, td, /*formal_type=*/nullptr);
    EXPECT_FALSE(status.ok());
}

// Drive convert_to_boxed_array with a populated STRUCT input column and a
// per-arg UdfTypeDesc that carries the UdfTestStructRecord class. Covers the
// build_struct_boxed_array path including JniLocalFrame, parent null buffer
// wrapping, per-field JavaArrayConverter dispatch, and the createBoxedStructArray
// JNI bridge.
TEST_F(DataConverterTest, convert_to_boxed_array_struct_input) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();

    // Construct UdfTypeDesc for STRUCT<key INT, value VARCHAR> with
    // UdfTestStructRecord as recordClass.
    jobject formal = reflected_return_generic_type(env, "structReturn", "()Lcom/starrocks/udf/UdfTestStructRecord;");
    ASSERT_NE(formal, nullptr);
    LOCAL_REF_GUARD(formal);
    TypeDescriptor td = make_test_struct_typedesc();
    ASSIGN_OR_ASSERT_FAIL(jobject desc_local, build_udf_type_desc(env, td, formal));
    LOCAL_REF_GUARD(desc_local);

    // Build a 2-row STRUCT column: [(1, "hello"), (2, "world")].
    auto col = ColumnHelper::create_column(td, /*nullable=*/true);
    col->append_datum(DatumStruct{Datum(int32_t{1}), Datum(Slice("hello"))});
    col->append_datum(DatumStruct{Datum(int32_t{2}), Datum(Slice("world"))});

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context({td}, td));
    std::vector<const Column*> cols = {col.get()};
    std::vector<jobject> arg_descs = {desc_local};
    std::vector<jobject> res;
    ASSERT_OK(JavaDataTypeConverter::convert_to_boxed_array(ctx.get(), cols.data(), 1, col->size(), &res, &arg_descs));
    ASSERT_EQ(res.size(), 1);
    ASSERT_NE(res[0], nullptr);

    // Returned Object[] should hold UdfTestStructRecord instances. Verify the
    // first row's component values via reflection.
    jobjectArray boxed = reinterpret_cast<jobjectArray>(res[0]);
    ASSERT_EQ(2, env->GetArrayLength(boxed));
    jobject row0 = env->GetObjectArrayElement(boxed, 0);
    LOCAL_REF_GUARD(row0);
    ASSERT_NE(row0, nullptr);

    jclass record_cls = test_struct_record_class(env);
    LOCAL_REF_GUARD(record_cls);
    EXPECT_TRUE(env->IsInstanceOf(row0, record_cls));

    jmethodID key_accessor = env->GetMethodID(record_cls, "key", "()Ljava/lang/Integer;");
    jmethodID value_accessor = env->GetMethodID(record_cls, "value", "()Ljava/lang/String;");
    ASSERT_NE(key_accessor, nullptr);
    ASSERT_NE(value_accessor, nullptr);
    jobject key_box = env->CallObjectMethod(row0, key_accessor);
    LOCAL_REF_GUARD(key_box);
    EXPECT_EQ(1, helper.valint32_t(key_box));
    jstring value_jstr = (jstring)env->CallObjectMethod(row0, value_accessor);
    LOCAL_REF_GUARD(value_jstr);
    std::string buf;
    EXPECT_EQ("hello", helper.sliceVal(value_jstr, &buf).to_string());

    env->DeleteLocalRef(res[0]);
}

namespace {
// Resolve a UdfTestSupport static method as a java.lang.reflect.Method object.
// Used by the build_method_udf_type_descs tests below to drive the helper with
// concrete UDAF / UDTF method shapes without standing up a real UDF classloader.
jobject reflected_test_support_method(JNIEnv* env, const char* method_name, const char* signature) {
    jclass support_cls = env->FindClass("com/starrocks/udf/UdfTestSupport");
    EXPECT_NE(support_cls, nullptr);
    if (support_cls == nullptr) return nullptr;
    jmethodID mid = env->GetStaticMethodID(support_cls, method_name, signature);
    EXPECT_NE(mid, nullptr) << method_name;
    jobject method_obj = env->ToReflectedMethod(support_cls, mid, JNI_TRUE);
    env->DeleteLocalRef(support_cls);
    return method_obj;
}

// Read UdfTypeDesc.logicalType field via the cached jfieldID.
jint read_logical_type(JVMFunctionHelper& helper, JNIEnv* env, jobject desc) {
    return env->GetIntField(desc, env->GetFieldID(helper.udf_type_desc_class(), "logicalType", "I"));
}
} // namespace

// build_method_udf_type_descs short-circuits when no STRUCT appears anywhere in
// the signature: returns one null-handle entry per SQL arg and a null-handle ret.
// Skips the JNI walk over Method.getGenericParameterTypes entirely.
TEST_F(DataConverterTest, build_method_udf_type_descs_no_struct_short_circuit) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();

    // Any method handle works since we early-return before reading it.
    jobject method = reflected_test_support_method(
            env, "udafUpdateWithStruct",
            "(Lcom/starrocks/udf/UdfTestSupport$State;Lcom/starrocks/udf/UdfTestStructRecord;)V");
    ASSERT_NE(method, nullptr);
    LOCAL_REF_GUARD(method);

    // SQL signature: udf(INT, BIGINT) -> VARCHAR — no STRUCT anywhere.
    std::vector<TypeDescriptor> args = {TypeDescriptor(TYPE_INT), TypeDescriptor(TYPE_BIGINT)};
    TypeDescriptor ret(TYPE_VARCHAR);

    ASSIGN_OR_ASSERT_FAIL(JavaUdfMethodTypeDescs descs,
                          build_method_udf_type_descs(env, method, args, ret, /*state_offset=*/0));
    ASSERT_EQ(2u, descs.args.size());
    EXPECT_EQ(nullptr, descs.args[0].handle());
    EXPECT_EQ(nullptr, descs.args[1].handle());
    EXPECT_EQ(nullptr, descs.ret.handle());
}

// UDAF shape: state_offset=1 makes the helper skip the leading State parameter
// and start pairing SQL args at parameter index 1. Verifies the SQL-arg
// UdfTypeDesc carries the correct STRUCT recordClass; return desc is suppressed
// here by passing a TYPE_UNKNOWN sentinel (the production UDAF path uses this
// to avoid walking update.getGenericReturnType() which is `void`).
TEST_F(DataConverterTest, build_method_udf_type_descs_udaf_state_offset) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();

    // void udafUpdateWithStruct(State state, UdfTestStructRecord record)
    jobject method = reflected_test_support_method(
            env, "udafUpdateWithStruct",
            "(Lcom/starrocks/udf/UdfTestSupport$State;Lcom/starrocks/udf/UdfTestStructRecord;)V");
    ASSERT_NE(method, nullptr);
    LOCAL_REF_GUARD(method);

    std::vector<TypeDescriptor> args = {make_test_struct_typedesc()};
    // Sentinel return type: TYPE_UNKNOWN — type_subtree_has_struct returns false
    // and the helper skips the return-type pass for `update` (Java return is void).
    TypeDescriptor ret = TypeDescriptor();

    ASSIGN_OR_ASSERT_FAIL(JavaUdfMethodTypeDescs descs,
                          build_method_udf_type_descs(env, method, args, ret, /*state_offset=*/1));
    ASSERT_EQ(1u, descs.args.size());
    ASSERT_NE(nullptr, descs.args[0].handle());
    EXPECT_EQ(static_cast<jint>(TYPE_STRUCT), read_logical_type(helper, env, descs.args[0].handle()));
    jobject record_class = env->GetObjectField(descs.args[0].handle(), helper.udf_type_desc_record_class_field());
    LOCAL_REF_GUARD(record_class);
    ASSERT_NE(nullptr, record_class);
    jclass expected = test_struct_record_class(env);
    LOCAL_REF_GUARD(expected);
    EXPECT_TRUE(env->IsSameObject(record_class, expected));
    // ret is TYPE_UNKNOWN — return desc must be null (no walk performed).
    EXPECT_EQ(nullptr, descs.ret.handle());
}

// UDAF finalize variant: empty SQL args plus a real STRUCT-bearing return type.
// Confirms the helper builds the return UdfTypeDesc by reading
// finalize.getGenericReturnType() (List<UdfTestStructRecord>) and the resulting
// desc carries logicalType=ARRAY with an element child carrying the record class.
TEST_F(DataConverterTest, build_method_udf_type_descs_udaf_finalize_array_of_struct) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();

    jobject method = reflected_test_support_method(env, "udafFinalizeArrayOfStruct",
                                                   "(Lcom/starrocks/udf/UdfTestSupport$State;)Ljava/util/List;");
    ASSERT_NE(method, nullptr);
    LOCAL_REF_GUARD(method);

    TypeDescriptor ret = TypeDescriptor::create_array_type(make_test_struct_typedesc());
    ASSIGN_OR_ASSERT_FAIL(JavaUdfMethodTypeDescs descs,
                          build_method_udf_type_descs(env, method, /*sql_arg_types=*/{}, ret, /*state_offset=*/1));
    EXPECT_TRUE(descs.args.empty());
    ASSERT_NE(nullptr, descs.ret.handle());
    EXPECT_EQ(static_cast<jint>(TYPE_ARRAY), read_logical_type(helper, env, descs.ret.handle()));

    jobjectArray children =
            (jobjectArray)env->GetObjectField(descs.ret.handle(), helper.udf_type_desc_children_field());
    LOCAL_REF_GUARD(children);
    ASSERT_NE(nullptr, children);
    ASSERT_EQ(1, env->GetArrayLength(children));
    jobject elem_desc = env->GetObjectArrayElement(children, 0);
    LOCAL_REF_GUARD(elem_desc);
    EXPECT_EQ(static_cast<jint>(TYPE_STRUCT), read_logical_type(helper, env, elem_desc));
    jobject elem_record_class = env->GetObjectField(elem_desc, helper.udf_type_desc_record_class_field());
    LOCAL_REF_GUARD(elem_record_class);
    jclass expected = test_struct_record_class(env);
    LOCAL_REF_GUARD(expected);
    EXPECT_TRUE(env->IsSameObject(elem_record_class, expected));
}

// UDTF shape: process(...) returns `T[]` while the SQL return is the per-row
// element type. unwrap_return_array_layer=true must drop the array layer off
// the formal return so getRecordComponents() lands on the record class, not
// the array class (Class<Record[]>).
TEST_F(DataConverterTest, build_method_udf_type_descs_udtf_unwrap_return_array) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();

    // UdfTestStructRecord[] udtfProcessReturnsRecordArray(UdfTestStructRecord)
    jobject method = reflected_test_support_method(
            env, "udtfProcessReturnsRecordArray",
            "(Lcom/starrocks/udf/UdfTestStructRecord;)[Lcom/starrocks/udf/UdfTestStructRecord;");
    ASSERT_NE(method, nullptr);
    LOCAL_REF_GUARD(method);

    std::vector<TypeDescriptor> args = {make_test_struct_typedesc()};
    // SQL return = STRUCT (per-row element). Java return = StructRecord[].
    TypeDescriptor ret = make_test_struct_typedesc();

    ASSIGN_OR_ASSERT_FAIL(JavaUdfMethodTypeDescs descs,
                          build_method_udf_type_descs(env, method, args, ret, /*state_offset=*/0,
                                                      /*unwrap_return_array_layer=*/true));
    ASSERT_EQ(1u, descs.args.size());
    ASSERT_NE(nullptr, descs.args[0].handle());
    EXPECT_EQ(static_cast<jint>(TYPE_STRUCT), read_logical_type(helper, env, descs.args[0].handle()));
    ASSERT_NE(nullptr, descs.ret.handle());
    EXPECT_EQ(static_cast<jint>(TYPE_STRUCT), read_logical_type(helper, env, descs.ret.handle()));

    // The unwrap must yield Class<UdfTestStructRecord>, not Class<UdfTestStructRecord[]>.
    jobject record_class = env->GetObjectField(descs.ret.handle(), helper.udf_type_desc_record_class_field());
    LOCAL_REF_GUARD(record_class);
    ASSERT_NE(nullptr, record_class);
    jclass expected = test_struct_record_class(env);
    LOCAL_REF_GUARD(expected);
    EXPECT_TRUE(env->IsSameObject(record_class, expected));
}

// Regression coverage for the bug fix that made build_method_udf_type_descs
// honor the `unwrap_return_array_layer` flag: without the unwrap, the helper
// would feed Class<Record[]> into build_udf_type_desc and then
// getRecordComponents() returns null — the helper must propagate that as an
// InternalError. Drives the helper with unwrap=false on the same UDTF-shaped
// method to confirm the path is wired.
TEST_F(DataConverterTest, build_method_udf_type_descs_struct_return_without_unwrap_fails) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();

    jobject method = reflected_test_support_method(
            env, "udtfProcessReturnsRecordArray",
            "(Lcom/starrocks/udf/UdfTestStructRecord;)[Lcom/starrocks/udf/UdfTestStructRecord;");
    ASSERT_NE(method, nullptr);
    LOCAL_REF_GUARD(method);

    std::vector<TypeDescriptor> args = {make_test_struct_typedesc()};
    TypeDescriptor ret = make_test_struct_typedesc();
    auto status_or = build_method_udf_type_descs(env, method, args, ret, /*state_offset=*/0,
                                                 /*unwrap_return_array_layer=*/false);
    ASSERT_FALSE(status_or.ok())
            << "build_method_udf_type_descs should reject Class<Record[]> for a STRUCT SQL return when unwrap=false";
}

// cast_to_jvalue STRUCT path: per-row materialization of a record from a
// StructColumn cell, used by UDTF process(). Verifies the returned jvalue is a
// non-null record instance whose components match the column row.
TEST_F(DataConverterTest, cast_to_jvalue_struct_per_row) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();

    // Build the per-arg UdfTypeDesc from a method shape — same path UDTF uses.
    jobject method = reflected_test_support_method(
            env, "udtfProcessReturnsRecordArray",
            "(Lcom/starrocks/udf/UdfTestStructRecord;)[Lcom/starrocks/udf/UdfTestStructRecord;");
    ASSERT_NE(method, nullptr);
    LOCAL_REF_GUARD(method);
    TypeDescriptor td = make_test_struct_typedesc();
    ASSIGN_OR_ASSERT_FAIL(JavaUdfMethodTypeDescs descs,
                          build_method_udf_type_descs(env, method, {td}, td, /*state_offset=*/0,
                                                      /*unwrap_return_array_layer=*/true));
    ASSERT_NE(descs.args[0].handle(), nullptr);

    // Two-row STRUCT column with a null on row 1 to also exercise the null-row branch.
    auto col = ColumnHelper::create_column(td, /*nullable=*/true);
    col->append_datum(DatumStruct{Datum(int32_t{42}), Datum(Slice("answer"))});
    col->append_nulls(1);

    // Row 0 — populated.
    ASSIGN_OR_ASSERT_FAIL(jvalue v0, cast_to_jvalue(td, /*is_boxed=*/true, col.get(), 0, descs.args[0].handle()));
    jobject v0_obj = v0.l;
    LOCAL_REF_GUARD(v0_obj);
    ASSERT_NE(nullptr, v0_obj);
    jclass record_cls = test_struct_record_class(env);
    LOCAL_REF_GUARD(record_cls);
    EXPECT_TRUE(env->IsInstanceOf(v0_obj, record_cls));
    jmethodID key_acc = env->GetMethodID(record_cls, "key", "()Ljava/lang/Integer;");
    jobject key_box = env->CallObjectMethod(v0_obj, key_acc);
    LOCAL_REF_GUARD(key_box);
    EXPECT_EQ(42, helper.valint32_t(key_box));

    // Row 1 — null. cast_to_jvalue must short-circuit to {.l = nullptr} via the
    // NullableColumn prologue, without reading the per-row record_class.
    ASSIGN_OR_ASSERT_FAIL(jvalue v1, cast_to_jvalue(td, /*is_boxed=*/true, col.get(), 1, descs.args[0].handle()));
    EXPECT_EQ(nullptr, v1.l);
}

// cast_to_jvalue STRUCT requires a non-null type_desc_obj — the formal record
// class lives there. Production callers (UDTF process) always supply it; this
// guards the explicit error if a future caller forgets.
TEST_F(DataConverterTest, cast_to_jvalue_struct_requires_type_desc) {
    auto& helper = JVMFunctionHelper::getInstance();
    (void)helper;

    TypeDescriptor td = make_test_struct_typedesc();
    auto col = ColumnHelper::create_column(td, /*nullable=*/true);
    col->append_datum(DatumStruct{Datum(int32_t{1}), Datum(Slice("x"))});
    auto status_or = cast_to_jvalue(td, /*is_boxed=*/true, col.get(), 0, /*type_desc_obj=*/nullptr);
    EXPECT_FALSE(status_or.ok());
}

// append_jvalue STRUCT path: per-row drain of a record into a StructColumn
// (UDTF result-write hot path). Builds a record via cast_to_jvalue, then
// round-trips it through append_jvalue and verifies the resulting column's
// shape and values.
TEST_F(DataConverterTest, append_jvalue_struct_per_row_roundtrip) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();

    jobject method = reflected_test_support_method(
            env, "udtfProcessReturnsRecordArray",
            "(Lcom/starrocks/udf/UdfTestStructRecord;)[Lcom/starrocks/udf/UdfTestStructRecord;");
    ASSERT_NE(method, nullptr);
    LOCAL_REF_GUARD(method);
    TypeDescriptor td = make_test_struct_typedesc();
    ASSIGN_OR_ASSERT_FAIL(JavaUdfMethodTypeDescs descs,
                          build_method_udf_type_descs(env, method, {td}, td, /*state_offset=*/0,
                                                      /*unwrap_return_array_layer=*/true));

    auto src = ColumnHelper::create_column(td, /*nullable=*/true);
    src->append_datum(DatumStruct{Datum(int32_t{7}), Datum(Slice("seven"))});
    ASSIGN_OR_ASSERT_FAIL(jvalue val, cast_to_jvalue(td, /*is_boxed=*/true, src.get(), 0, descs.args[0].handle()));
    jobject val_obj = val.l;
    LOCAL_REF_GUARD(val_obj);
    ASSERT_NE(nullptr, val_obj);

    auto dst = ColumnHelper::create_column(td, /*nullable=*/true);
    ASSERT_OK(append_jvalue(td, /*is_box=*/true, dst.get(), val,
                            /*error_if_overflow=*/true, descs.ret.handle()));
    ASSERT_EQ(1u, dst->size());
    EXPECT_EQ("{key:7,value:'seven'}", dst->debug_item(0));
}

// check_type_matched STRUCT path: UDTF result-row validator. Accepts the
// declared record class and rejects an unrelated runtime class.
TEST_F(DataConverterTest, check_type_matched_struct) {
    auto& helper = JVMFunctionHelper::getInstance();
    auto* env = helper.getEnv();

    jobject method = reflected_test_support_method(
            env, "udtfProcessReturnsRecordArray",
            "(Lcom/starrocks/udf/UdfTestStructRecord;)[Lcom/starrocks/udf/UdfTestStructRecord;");
    ASSERT_NE(method, nullptr);
    LOCAL_REF_GUARD(method);
    TypeDescriptor td = make_test_struct_typedesc();
    ASSIGN_OR_ASSERT_FAIL(JavaUdfMethodTypeDescs descs,
                          build_method_udf_type_descs(env, method, {td}, td, /*state_offset=*/0,
                                                      /*unwrap_return_array_layer=*/true));

    // Build a real record instance via cast_to_jvalue.
    auto src = ColumnHelper::create_column(td, /*nullable=*/true);
    src->append_datum(DatumStruct{Datum(int32_t{1}), Datum(Slice("a"))});
    ASSIGN_OR_ASSERT_FAIL(jvalue val, cast_to_jvalue(td, /*is_boxed=*/true, src.get(), 0, descs.args[0].handle()));
    jobject val_obj = val.l;
    LOCAL_REF_GUARD(val_obj);
    EXPECT_OK(check_type_matched(td, val_obj, descs.ret.handle()));

    // A String is not a UdfTestStructRecord → reject.
    jstring stranger = env->NewStringUTF("not a record");
    LOCAL_REF_GUARD(stranger);
    EXPECT_FALSE(check_type_matched(td, stranger, descs.ret.handle()).ok());

    // Null value short-circuits to OK regardless of type_desc_obj.
    EXPECT_OK(check_type_matched(td, /*val=*/nullptr, descs.ret.handle()));
}

} // namespace starrocks