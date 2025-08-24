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

#include "column/column_helper.h"
#include "testutil/assert.h"

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
} // namespace starrocks