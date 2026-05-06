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

#include "udf/java/java_udf.h"

#include <gtest/gtest.h>

#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "exprs/function_context.h"
#include "gutil/casts.h"
#include "types/date_value.h"
#include "types/logical_type.h"
#include "types/timestamp_value.h"

namespace starrocks {
class JavaUDFTest : public testing::Test {
public:
    void SetUp() override;
    void TearDown() override;

protected:
    void _set_timezone(const std::string& timezone);
    jstring _get_timezone();

    JNIEnv* _env = nullptr;
    jstring _timezone;
};

void JavaUDFTest::SetUp() {
    _env = getJNIEnv();
    _timezone = _get_timezone();
    _set_timezone("Asia/Singapore");
}

void JavaUDFTest::TearDown() {
    const char* cstr = _env->GetStringUTFChars(_timezone, nullptr);
    if (cstr != nullptr) {
        _set_timezone(std::string(cstr));
        _env->ReleaseStringUTFChars(_timezone, cstr);
    }
}

jstring JavaUDFTest::_get_timezone() {
    jclass time_zone_cls = _env->FindClass("java/util/TimeZone");
    EXPECT_TRUE(time_zone_cls != nullptr);

    jmethodID get_method = _env->GetStaticMethodID(time_zone_cls, "getDefault", "()Ljava/util/TimeZone;");
    EXPECT_TRUE(get_method != nullptr);

    jobject time_zone_obj = _env->CallStaticObjectMethod(time_zone_cls, get_method);
    EXPECT_TRUE(time_zone_obj != nullptr);

    jmethodID get_id_method = _env->GetMethodID(time_zone_cls, "getID", "()Ljava/lang/String;");
    EXPECT_TRUE(get_id_method != nullptr);

    jstring time_zone_id = (jstring)_env->CallObjectMethod(time_zone_obj, get_id_method);

    _env->DeleteLocalRef(time_zone_obj);
    _env->DeleteLocalRef(time_zone_cls);

    return time_zone_id;
}

void JavaUDFTest::_set_timezone(const std::string& timezone) {
    jclass tz_class = _env->FindClass("java/util/TimeZone");

    jmethodID get_time_zone_mid =
            _env->GetStaticMethodID(tz_class, "getTimeZone", "(Ljava/lang/String;)Ljava/util/TimeZone;");

    jstring j_tz_id = _env->NewStringUTF(timezone.c_str());

    jobject tz_obj = _env->CallStaticObjectMethod(tz_class, get_time_zone_mid, j_tz_id);

    jmethodID set_default_mid = _env->GetStaticMethodID(tz_class, "setDefault", "(Ljava/util/TimeZone;)V");

    _env->CallStaticVoidMethod(tz_class, set_default_mid, tz_obj);

    _env->DeleteLocalRef(tz_obj);
    _env->DeleteLocalRef(j_tz_id);
    _env->DeleteLocalRef(tz_class);
}

TEST_F(JavaUDFTest, test_time_convert) {
    jclass time_class = _env->FindClass("java/sql/Time");
    ASSERT_TRUE(time_class != NULL);

    jmethodID constructor = _env->GetMethodID(time_class, "<init>", "(III)V");
    ASSERT_TRUE(constructor != NULL);

    jobjectArray time_array = _env->NewObjectArray(1, time_class, NULL);
    ASSERT_TRUE(time_array != NULL);

    jint hour = 1;
    jint minute = 10;
    jint seconds = 20;
    jobject time_obj = _env->NewObject(time_class, constructor, hour, minute, seconds);
    _env->SetObjectArrayElement(time_array, 0, time_obj);

    TypeDescriptor time_desc(TYPE_TIME);
    auto result_column = ColumnHelper::create_column(time_desc, true);

    auto& helper = JVMFunctionHelper::getInstance();
    ASSERT_OK(helper.get_result_from_boxed_array(TYPE_TIME, result_column.get(), time_array, 1));
    // 1 * 3600 + 10 * 60 + 20 = 4220
    ASSERT_EQ(result_column->debug_string(), "[4220]");

    _env->DeleteLocalRef(time_obj);
    _env->DeleteLocalRef(time_array);
    _env->DeleteLocalRef(time_class);
}

// ============================================================================
// LocalDate / LocalDateTime helper round-trips and end-to-end batch output.
// Wire format mirrors BE column storage:
//   DATE     -> int32 Julian day (DateValue::_julian)
//   DATETIME -> packed int64 (julian << 40 | microseconds_of_day)
// ============================================================================

// newLocalDate(julian) -> LocalDate -> valLocalDate(...) must round-trip the
// underlying int32 Julian day exactly across the JNI boundary.
TEST_F(JavaUDFTest, local_date_helper_roundtrip) {
    auto& helper = JVMFunctionHelper::getInstance();
    DateValue cases[] = {
            DateValue::create(1970, 1, 1),
            DateValue::create(1, 1, 1),
            DateValue::create(2024, 1, 15),
            DateValue::create(9999, 12, 31),
    };
    for (auto v : cases) {
        jobject ld = helper.newLocalDate(v._julian);
        ASSERT_NE(ld, nullptr);
        LOCAL_REF_GUARD(ld);
        EXPECT_EQ(v._julian, helper.valLocalDate(ld));
        EXPECT_TRUE(_env->IsInstanceOf(ld, helper.local_date_class()));
    }
}

// newLocalDateTime(packed) -> LocalDateTime -> valLocalDateTime(...) round-trip.
// The packed encoding bridges the BE Julian-time format and Java LocalDateTime
// via UDFHelper's packedTimestampFromLocalDateTime / localDateTimeFromPackedTimestamp.
TEST_F(JavaUDFTest, local_datetime_helper_roundtrip) {
    auto& helper = JVMFunctionHelper::getInstance();
    TimestampValue cases[] = {
            TimestampValue::create(1970, 1, 1, 0, 0, 0, 0),
            TimestampValue::create(1970, 1, 1, 0, 0, 0, 1), // 1 microsecond
            TimestampValue::create(2024, 1, 15, 12, 34, 56, 789000),
            TimestampValue::create(9999, 12, 31, 23, 59, 59, 999999),
    };
    for (auto v : cases) {
        jobject ldt = helper.newLocalDateTime(v.timestamp());
        ASSERT_NE(ldt, nullptr);
        LOCAL_REF_GUARD(ldt);
        EXPECT_EQ(v.timestamp(), helper.valLocalDateTime(ldt));
        EXPECT_TRUE(_env->IsInstanceOf(ldt, helper.local_datetime_class()));
    }
}

// End-to-end batch output for DATE: build a Java LocalDate[] (with one null)
// and let UDFHelper.getResultFromBoxedArray write into a native DateColumn.
// Mirrors the path a UDF returning DATE goes through.
TEST_F(JavaUDFTest, get_result_from_boxed_array_date) {
    auto& helper = JVMFunctionHelper::getInstance();

    DateValue d0 = DateValue::create(2024, 1, 15);
    DateValue d2 = DateValue::create(1970, 1, 1);
    jobject ld0 = helper.newLocalDate(d0._julian);
    jobject ld2 = helper.newLocalDate(d2._julian);
    LOCAL_REF_GUARD(ld0);
    LOCAL_REF_GUARD(ld2);

    jobjectArray arr = _env->NewObjectArray(3, helper.local_date_class(), nullptr);
    ASSERT_NE(arr, nullptr);
    LOCAL_REF_GUARD(arr);
    _env->SetObjectArrayElement(arr, 0, ld0);
    // index 1 stays null
    _env->SetObjectArrayElement(arr, 2, ld2);

    auto result = ColumnHelper::create_column(TypeDescriptor(TYPE_DATE), true);
    ASSERT_OK(helper.get_result_from_boxed_array(TYPE_DATE, result.get(), arr, 3));
    // Java memcpys the null buffer directly; mirror the production path
    // (java_function_call_expr.cpp::get_boxed_result) and refresh _has_null.
    down_cast<NullableColumn*>(result.get())->update_has_null();
    ASSERT_EQ(result->size(), 3);
    EXPECT_FALSE(result->is_null(0));
    EXPECT_EQ(result->debug_item(0), d0.to_string());
    EXPECT_TRUE(result->is_null(1));
    EXPECT_FALSE(result->is_null(2));
    EXPECT_EQ(result->debug_item(2), d2.to_string());
}

// End-to-end batch output for DATETIME, mirroring the DATE test.
TEST_F(JavaUDFTest, get_result_from_boxed_array_datetime) {
    auto& helper = JVMFunctionHelper::getInstance();

    TimestampValue t0 = TimestampValue::create(2024, 1, 15, 12, 34, 56, 789000);
    TimestampValue t2 = TimestampValue::create(1970, 1, 1, 0, 0, 0, 0);
    jobject ldt0 = helper.newLocalDateTime(t0.timestamp());
    jobject ldt2 = helper.newLocalDateTime(t2.timestamp());
    LOCAL_REF_GUARD(ldt0);
    LOCAL_REF_GUARD(ldt2);

    jobjectArray arr = _env->NewObjectArray(3, helper.local_datetime_class(), nullptr);
    ASSERT_NE(arr, nullptr);
    LOCAL_REF_GUARD(arr);
    _env->SetObjectArrayElement(arr, 0, ldt0);
    _env->SetObjectArrayElement(arr, 2, ldt2);

    auto result = ColumnHelper::create_column(TypeDescriptor(TYPE_DATETIME), true);
    ASSERT_OK(helper.get_result_from_boxed_array(TYPE_DATETIME, result.get(), arr, 3));
    down_cast<NullableColumn*>(result.get())->update_has_null();
    ASSERT_EQ(result->size(), 3);
    EXPECT_FALSE(result->is_null(0));
    EXPECT_EQ(result->debug_item(0), t0.to_string());
    EXPECT_TRUE(result->is_null(1));
    EXPECT_FALSE(result->is_null(2));
    EXPECT_EQ(result->debug_item(2), t2.to_string());
}

// Drives the unified get_result_from_boxed_array's DECIMAL dispatch (the merged
// DECIMAL / scalar entry point introduced when get_decimal_result_from_boxed_array
// was folded in). Verifies that passing a non-zero precision/scale routes to the
// DECIMAL Java helper and writes a BigDecimal[] correctly into a DECIMAL column.
TEST_F(JavaUDFTest, get_result_from_boxed_array_decimal_dispatch) {
    auto& helper = JVMFunctionHelper::getInstance();

    // BigDecimal[] { "12345.67", null, "0.00" } over DECIMAL64(9,2).
    jobject bd0 = helper.newBigDecimal(static_cast<int64_t>(1234567), 2);
    LOCAL_REF_GUARD(bd0);
    jobject bd2 = helper.newBigDecimal(static_cast<int64_t>(0), 2);
    LOCAL_REF_GUARD(bd2);

    jobjectArray arr = _env->NewObjectArray(3, helper.big_decimal_class(), nullptr);
    ASSERT_NE(arr, nullptr);
    LOCAL_REF_GUARD(arr);
    _env->SetObjectArrayElement(arr, 0, bd0);
    // index 1 stays null
    _env->SetObjectArrayElement(arr, 2, bd2);

    auto td = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 9, 2);
    auto result = ColumnHelper::create_column(td, true);
    ASSERT_OK(helper.get_result_from_boxed_array(TYPE_DECIMAL64, result.get(), arr, 3, 9, 2,
                                                 /*error_if_overflow=*/true));
    down_cast<NullableColumn*>(result.get())->update_has_null();
    ASSERT_EQ(result->size(), 3);
    EXPECT_FALSE(result->is_null(0));
    EXPECT_TRUE(result->is_null(1));
    EXPECT_FALSE(result->is_null(2));
    EXPECT_EQ(result->debug_item(0), "12345.67");
    EXPECT_EQ(result->debug_item(2), "0.00");
}

// FunctionContext-overload of the merged get_result_from_boxed_array (UDAF
// finalize path). Reports JNI exceptions through ctx->set_error rather than
// returning Status. Drive a successful scalar write to make sure the new
// signature is wired and the dispatch keeps the non-DECIMAL fast path intact.
TEST_F(JavaUDFTest, get_result_from_boxed_array_with_function_context) {
    auto& helper = JVMFunctionHelper::getInstance();
    TypeDescriptor td(TYPE_INT);
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context({td}, td));

    // Integer[] { 1, 2, 3 } via valueOf.
    jclass integer_cls = helper.int32_t_class();
    jmethodID value_of = _env->GetStaticMethodID(integer_cls, "valueOf", "(I)Ljava/lang/Integer;");
    ASSERT_NE(value_of, nullptr);
    jobjectArray arr = _env->NewObjectArray(3, integer_cls, nullptr);
    LOCAL_REF_GUARD(arr);
    for (int i = 0; i < 3; ++i) {
        jobject boxed = _env->CallStaticObjectMethod(integer_cls, value_of, i + 1);
        _env->SetObjectArrayElement(arr, i, boxed);
        _env->DeleteLocalRef(boxed);
    }

    auto result = ColumnHelper::create_column(td, true);
    helper.get_result_from_boxed_array(ctx.get(), TYPE_INT, result.get(), arr, 3);
    EXPECT_FALSE(ctx->has_error());
    ASSERT_EQ(result->size(), 3);
    EXPECT_EQ(result->debug_item(0), "1");
    EXPECT_EQ(result->debug_item(1), "2");
    EXPECT_EQ(result->debug_item(2), "3");
}

// JVMFunctionHelper::new_udf_type_desc constructs a com.starrocks.udf.UdfTypeDesc
// Java object via the cached constructor. Verify field round-trip via the cached
// jfieldIDs the BE input boxer relies on.
TEST_F(JavaUDFTest, new_udf_type_desc_scalar_leaf) {
    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();

    ASSIGN_OR_ASSERT_FAIL(jobject desc,
                          helper.new_udf_type_desc(/*logicalType=*/TYPE_INT, /*children=*/nullptr,
                                                   /*precision=*/0, /*scale=*/0, /*record_class=*/nullptr));
    LOCAL_REF_GUARD(desc);
    ASSERT_NE(desc, nullptr);

    jclass cls = helper.udf_type_desc_class();
    jfieldID lt_field = env->GetFieldID(cls, "logicalType", "I");
    ASSERT_NE(lt_field, nullptr);
    EXPECT_EQ(static_cast<jint>(TYPE_INT), env->GetIntField(desc, lt_field));

    // Leaf nodes carry no children and no recordClass.
    EXPECT_EQ(nullptr, env->GetObjectField(desc, helper.udf_type_desc_children_field()));
    EXPECT_EQ(nullptr, env->GetObjectField(desc, helper.udf_type_desc_record_class_field()));
}

// DECIMAL slots carry precision/scale. STRUCT slots carry the formal record
// Class<?>. Both flow through new_udf_type_desc and must round-trip through
// the cached field IDs.
TEST_F(JavaUDFTest, new_udf_type_desc_decimal_and_struct) {
    auto& helper = JVMFunctionHelper::getInstance();
    JNIEnv* env = helper.getEnv();

    // DECIMAL64(18,4)
    {
        ASSIGN_OR_ASSERT_FAIL(jobject desc,
                              helper.new_udf_type_desc(/*logicalType=*/TYPE_DECIMAL64, nullptr, 18, 4, nullptr));
        LOCAL_REF_GUARD(desc);
        jclass cls = helper.udf_type_desc_class();
        jfieldID precision_field = env->GetFieldID(cls, "precision", "I");
        jfieldID scale_field = env->GetFieldID(cls, "scale", "I");
        EXPECT_EQ(18, env->GetIntField(desc, precision_field));
        EXPECT_EQ(4, env->GetIntField(desc, scale_field));
    }

    // STRUCT with String.class as a stand-in record class plus a non-empty children array.
    // Verifies the children array slot and record-class slot both populate the
    // jfieldIDs the BE input boxer reads at runtime.
    {
        ASSIGN_OR_ASSERT_FAIL(jobject child, helper.new_udf_type_desc(TYPE_INT, nullptr, 0, 0, nullptr));
        LOCAL_REF_GUARD(child);
        jobjectArray children = env->NewObjectArray(1, helper.udf_type_desc_class(), child);
        LOCAL_REF_GUARD(children);

        jclass string_cls = env->FindClass("java/lang/String");
        ASSERT_NE(string_cls, nullptr);
        ASSIGN_OR_ASSERT_FAIL(jobject desc, helper.new_udf_type_desc(TYPE_STRUCT, children, 0, 0, string_cls));
        LOCAL_REF_GUARD(desc);

        jobject record_class = env->GetObjectField(desc, helper.udf_type_desc_record_class_field());
        LOCAL_REF_GUARD(record_class);
        EXPECT_TRUE(env->IsSameObject(record_class, string_cls));

        jobject children_back = env->GetObjectField(desc, helper.udf_type_desc_children_field());
        LOCAL_REF_GUARD(children_back);
        ASSERT_NE(children_back, nullptr);
        EXPECT_EQ(1, env->GetArrayLength(reinterpret_cast<jobjectArray>(children_back)));
        env->DeleteLocalRef(string_cls);
    }
}

// ============================================================================
// Tests for strip_jni_generic_types():
//   Strips generic type parameters from JNI method signatures.
// ============================================================================

TEST(StripJniGenericTypesTest, NoGenerics) {
    std::string sign = "(Ljava/lang/String;I)V";
    ClassAnalyzer::strip_jni_generic_types(&sign);
    EXPECT_EQ(sign, "(Ljava/lang/String;I)V");
}

TEST(StripJniGenericTypesTest, SimpleGeneric) {
    std::string sign = "(Ljava/util/List<Ljava/lang/String;>;)V";
    ClassAnalyzer::strip_jni_generic_types(&sign);
    EXPECT_EQ(sign, "(Ljava/util/List;)V");
}

TEST(StripJniGenericTypesTest, MultipleGenericParams) {
    std::string sign = "(Ljava/lang/String;Ljava/util/List<Ljava/lang/String;>;Ljava/util/List<Ljava/lang/Integer;>;)V";
    ClassAnalyzer::strip_jni_generic_types(&sign);
    EXPECT_EQ(sign, "(Ljava/lang/String;Ljava/util/List;Ljava/util/List;)V");
}

TEST(StripJniGenericTypesTest, NestedGenerics) {
    std::string sign = "(Ljava/util/Map<Ljava/lang/String;Ljava/util/List<Ljava/lang/Integer;>;>;)V";
    ClassAnalyzer::strip_jni_generic_types(&sign);
    EXPECT_EQ(sign, "(Ljava/util/Map;)V");
}

TEST(StripJniGenericTypesTest, EmptyString) {
    std::string sign;
    ClassAnalyzer::strip_jni_generic_types(&sign);
    EXPECT_EQ(sign, "");
}

// ============================================================================
// Tests for ClassAnalyzer::get_udaf_method_desc bug fixes:
//   1. '[' branch missing continue — caused spurious method_desc entries
//   2. List<> generic in signature — caused type matching failure
//   3. Combined: UDTF with List params + String[] return — the crash scenario
// ============================================================================

// Test: '[' branch continue fix — array param must not produce spurious entries.
// Before fix, processing "[Ljava/lang/String;" left i on ';', which fell through
// to the else branch and added an extra {TYPE_UNKNOWN, false} entry.
TEST(GetUdafMethodDescTest, ArrayBranchContinueFix) {
    ClassAnalyzer analyzer;
    std::vector<MethodTypeDescriptor> desc;
    // Signature: process(String[] arr, int n) -> void
    ASSERT_OK(analyzer.get_udaf_method_desc("([Ljava/lang/String;I)V", &desc));
    // Must be exactly 3: [return=V, param1=[String, param2=I]
    // Before fix: 4 entries (spurious {TYPE_UNKNOWN,false} from ';')
    ASSERT_EQ(desc.size(), 3);
    EXPECT_EQ(desc[0].type, TYPE_UNKNOWN); // return V
    EXPECT_EQ(desc[1].type, TYPE_VARCHAR); // [String array
    EXPECT_EQ(desc[1].is_array, true);     // [String array
    EXPECT_EQ(desc[2].type, TYPE_INT);     // int
}

// Test: List type matching — after get_signature strips generics,
// "Ljava/util/List;" must be recognized as TYPE_ARRAY.
// Before fix, generic signature "Ljava/util/List<java/lang/String>;" caused
// type == "java/util/List<java/lang/String>" which didn't match "java/util/List".
TEST(GetUdafMethodDescTest, ListTypeMatchAfterGenericStrip) {
    ClassAnalyzer analyzer;
    std::vector<MethodTypeDescriptor> desc;
    // Clean signature (generics already stripped by get_signature)
    ASSERT_OK(analyzer.get_udaf_method_desc("(Ljava/lang/String;Ljava/util/List;Ljava/util/List;)V", &desc));
    ASSERT_EQ(desc.size(), 4);
    EXPECT_EQ(desc[0].type, TYPE_UNKNOWN); // return V
    EXPECT_EQ(desc[1].type, TYPE_VARCHAR); // String
    EXPECT_EQ(desc[2].type, TYPE_ARRAY);   // List → TYPE_ARRAY
    EXPECT_EQ(desc[3].type, TYPE_ARRAY);   // List → TYPE_ARRAY
}

// Test: realistic UDTF crash scenario — process(String, List, List) -> String[]
// This is the exact signature pattern that caused the CN crash.
// Verifies method_desc.size() == num_cols + 1 so process() won't OOB access.
TEST(GetUdafMethodDescTest, UDTFCrashScenarioSignature) {
    ClassAnalyzer analyzer;
    std::vector<MethodTypeDescriptor> desc;
    std::string sign = "(Ljava/lang/String;Ljava/util/List;Ljava/util/List;)[Ljava/lang/String;";
    ASSERT_OK(analyzer.get_udaf_method_desc(sign, &desc));
    // 1 return + 3 params = 4
    ASSERT_EQ(desc.size(), 4);
    EXPECT_EQ(desc[0].type, TYPE_VARCHAR); // return [String
    EXPECT_EQ(desc[0].is_array, true);     // return [String
    EXPECT_EQ(desc[1].type, TYPE_VARCHAR); // param String
    EXPECT_EQ(desc[2].type, TYPE_ARRAY);   // param List
    EXPECT_EQ(desc[3].type, TYPE_ARRAY);   // param List
}

// Test: Primitive array parsing — [I, [J, etc. don't end with ';'.
TEST(GetUdafMethodDescTest, PrimitiveIntArray) {
    ClassAnalyzer analyzer;
    std::vector<MethodTypeDescriptor> desc;
    // Signature: process(int, int[]) -> void
    ASSERT_OK(analyzer.get_udaf_method_desc("(I[I)V", &desc));
    ASSERT_EQ(desc.size(), 3);
    EXPECT_EQ(desc[0].type, TYPE_UNKNOWN); // return V
    EXPECT_EQ(desc[1].type, TYPE_INT);     // param int
    EXPECT_EQ(desc[2].type, TYPE_UNKNOWN); // param int[] (array)
}

// Test: Multi-dimensional primitive array — [[J (long[][])
TEST(GetUdafMethodDescTest, MultiDimensionalPrimitiveArray) {
    ClassAnalyzer analyzer;
    std::vector<MethodTypeDescriptor> desc;
    ASSERT_OK(analyzer.get_udaf_method_desc("([[J)V", &desc));
    ASSERT_EQ(desc.size(), 2);
    EXPECT_EQ(desc[0].type, TYPE_UNKNOWN); // return V
    EXPECT_EQ(desc[1].type, TYPE_UNKNOWN); // param long[][] (array)
}

// Test: java.time.LocalDate / LocalDateTime must map to TYPE_DATE / TYPE_DATETIME.
// Before the fix, the L-type parser silently skipped these classes, leaving
// method_desc empty and producing a SIGSEGV at method_desc[0].is_box.
TEST(GetUdafMethodDescTest, LocalDateAndLocalDateTime) {
    ClassAnalyzer analyzer;
    std::vector<MethodTypeDescriptor> desc;
    // Signature: evaluate(LocalDate, LocalDateTime) -> LocalDateTime
    std::string sign = "(Ljava/time/LocalDate;Ljava/time/LocalDateTime;)Ljava/time/LocalDateTime;";
    ASSERT_OK(analyzer.get_udaf_method_desc(sign, &desc));
    ASSERT_EQ(desc.size(), 3);
    EXPECT_EQ(desc[0].type, TYPE_DATETIME); // return LocalDateTime
    EXPECT_TRUE(desc[0].is_box);
    EXPECT_EQ(desc[1].type, TYPE_DATE); // param LocalDate
    EXPECT_TRUE(desc[1].is_box);
    EXPECT_EQ(desc[2].type, TYPE_DATETIME); // param LocalDateTime
    EXPECT_TRUE(desc[2].is_box);
}

// Test: any other object class is treated as a user record bound to a STRUCT
// parameter/return — the FE analyzer already validates at CREATE FUNCTION time
// that records only appear in STRUCT slots, so the BE signature parser may
// safely surface them as TYPE_STRUCT and accept the method.
TEST(GetUdafMethodDescTest, UnknownObjectClassTreatedAsStruct) {
    ClassAnalyzer analyzer;
    std::vector<MethodTypeDescriptor> desc;
    // process(String, com/example/Mystery) -> void
    ASSERT_OK(analyzer.get_udaf_method_desc("(Ljava/lang/String;Lcom/example/Mystery;)V", &desc));
    ASSERT_EQ(desc.size(), 3);
    EXPECT_EQ(desc[0].type, TYPE_UNKNOWN); // return V
    EXPECT_EQ(desc[1].type, TYPE_VARCHAR); // String
    EXPECT_EQ(desc[2].type, TYPE_STRUCT);  // unknown class -> STRUCT
    EXPECT_TRUE(desc[2].is_box);
    // get_method_desc validation must accept the parameter (return type V is
    // ignored; only param entries are checked for TYPE_UNKNOWN).
    desc.clear();
    ASSERT_OK(analyzer.get_method_desc("(Ljava/lang/String;Lcom/example/Mystery;)V", &desc));
}

// Test: the original user-reported scenario — `evaluate(StructA): StructA`
// produces signature `(LStructA;)LStructA;`. Both the param and the return must
// be classified as TYPE_STRUCT, and get_method_desc must accept the full
// signature so the UDF can be registered and dispatched.
//
// Convention (see end of get_udaf_method_desc): the parser appends entries
// left-to-right then moves the last (return) entry to desc[0], so the final
// layout is [return, param1, param2, ...].
TEST(GetUdafMethodDescTest, RecordParamAndReturn) {
    ClassAnalyzer analyzer;
    std::vector<MethodTypeDescriptor> desc;
    std::string sign = "(LStructA;)LStructA;";
    ASSERT_OK(analyzer.get_udaf_method_desc(sign, &desc));
    ASSERT_EQ(desc.size(), 2);
    EXPECT_EQ(desc[0].type, TYPE_STRUCT); // return StructA
    EXPECT_TRUE(desc[0].is_box);
    EXPECT_EQ(desc[1].type, TYPE_STRUCT); // param StructA
    EXPECT_TRUE(desc[1].is_box);

    desc.clear();
    ASSERT_OK(analyzer.get_method_desc(sign, &desc));
}

// Test: STRUCT mixed with primitives in the same signature.
// evaluate(StructA, int, String) -> StructB — exercises the boundary between
// the L-class branch (which now emits TYPE_STRUCT for unknown classes) and
// the existing primitive / known-class branches in the same parser pass.
TEST(GetUdafMethodDescTest, RecordWithPrimitiveAndStringMix) {
    ClassAnalyzer analyzer;
    std::vector<MethodTypeDescriptor> desc;
    std::string sign = "(LStructA;ILjava/lang/String;)LStructB;";
    ASSERT_OK(analyzer.get_udaf_method_desc(sign, &desc));
    ASSERT_EQ(desc.size(), 4);
    EXPECT_EQ(desc[0].type, TYPE_STRUCT);  // return StructB (record)
    EXPECT_EQ(desc[1].type, TYPE_STRUCT);  // param StructA (record)
    EXPECT_EQ(desc[2].type, TYPE_INT);     // param int (primitive)
    EXPECT_EQ(desc[3].type, TYPE_VARCHAR); // param String

    desc.clear();
    ASSERT_OK(analyzer.get_method_desc(sign, &desc));
}

// Test: nested record class name with package qualifier and inner-class `$`.
// JNI signatures use the slash-separated binary name and `$` for inner classes;
// the parser should treat the whole `L<binary-name>;` as a single record
// reference and emit TYPE_STRUCT regardless of the surface form.
TEST(GetUdafMethodDescTest, RecordWithPackageAndInnerClass) {
    ClassAnalyzer analyzer;
    std::vector<MethodTypeDescriptor> desc;
    std::string sign = "(Lcom/example/Outer$Inner;)Lcom/example/pkg/Result;";
    ASSERT_OK(analyzer.get_udaf_method_desc(sign, &desc));
    ASSERT_EQ(desc.size(), 2);
    EXPECT_EQ(desc[0].type, TYPE_STRUCT); // return Result
    EXPECT_TRUE(desc[0].is_box);
    EXPECT_EQ(desc[1].type, TYPE_STRUCT); // param Outer$Inner
    EXPECT_TRUE(desc[1].is_box);
}

} // namespace starrocks
