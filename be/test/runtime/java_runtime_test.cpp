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

#include "runtime/env/java/java_runtime.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

#include "base/testutil/assert.h"
#include "runtime/env/java/java_env.h"

namespace starrocks {
namespace {

class JavaRuntimeTest : public testing::Test {
protected:
    void SetUp() override {
        if (std::getenv("JAVA_HOME") == nullptr) {
            GTEST_SKIP() << "JAVA_HOME is not set";
        }

        _env = getJNIEnv();
        if (_env == nullptr) {
            GTEST_SKIP() << "JVM is unavailable";
        }

        jclass exception_util = _env->FindClass("org/apache/commons/lang3/exception/ExceptionUtils");
        if (exception_util == nullptr) {
            _env->ExceptionClear();
            GTEST_SKIP() << "commons-lang3 ExceptionUtils is unavailable";
        }
        _env->DeleteLocalRef(exception_util);
    }

    JNIEnv* env() const { return _env; }
    JavaRuntime& runtime() const { return JavaRuntime::getInstance(); }

private:
    JNIEnv* _env = nullptr;
};

} // namespace

TEST(JavaRuntimeStaticTest, ToJniClassName) {
    ASSERT_EQ("java/lang/String", JavaRuntime::to_jni_class_name("java.lang.String"));
    ASSERT_EQ("com/starrocks/udf/UDFHelper", JavaRuntime::to_jni_class_name("com.starrocks.udf.UDFHelper"));
    ASSERT_EQ("java/lang/String", JavaRuntime::to_jni_class_name("java/lang/String"));
}

TEST_F(JavaRuntimeTest, GetEnvReturnsStableEnvForCurrentThread) {
    ASSERT_NE(runtime().getEnv(), nullptr);
    ASSERT_EQ(env(), runtime().getEnv());
}

TEST_F(JavaRuntimeTest, StringRoundTrip) {
    ASSIGN_OR_ASSERT_FAIL(jstring jstr, runtime().to_jstring("starrocks-runtime"));
    ASSERT_NE(jstr, nullptr);

    ASSERT_EQ("starrocks-runtime", runtime().to_cxx_string(jstr));
    ASSERT_EQ("starrocks-runtime", runtime().to_string(jstr));

    env()->DeleteLocalRef(jstr);
}

TEST_F(JavaRuntimeTest, CreateObjectArray) {
    jobjectArray nulls = runtime().create_object_array(2);
    ASSERT_NE(nulls, nullptr);
    ASSERT_EQ(2, env()->GetArrayLength(nulls));
    jobject first_null = env()->GetObjectArrayElement(nulls, 0);
    ASSERT_EQ(nullptr, first_null);
    env()->DeleteLocalRef(nulls);

    ASSIGN_OR_ASSERT_FAIL(jstring fill, runtime().to_jstring("fill"));
    jobjectArray filled = runtime().create_object_array(fill, 2);
    ASSERT_NE(filled, nullptr);
    ASSERT_EQ(2, env()->GetArrayLength(filled));
    jobject first = env()->GetObjectArrayElement(filled, 0);
    ASSERT_TRUE(env()->IsSameObject(fill, first));

    env()->DeleteLocalRef(first);
    env()->DeleteLocalRef(filled);
    env()->DeleteLocalRef(fill);
}

TEST_F(JavaRuntimeTest, ArrayToString) {
    ASSIGN_OR_ASSERT_FAIL(jstring alpha, runtime().to_jstring("alpha"));
    ASSIGN_OR_ASSERT_FAIL(jstring beta, runtime().to_jstring("beta"));
    jobject elements[] = {alpha, beta};
    jobjectArray array = runtime().create_object_array(elements, 2);
    ASSERT_NE(array, nullptr);

    jclass object_array_class = env()->FindClass("[Ljava/lang/Object;");
    ASSERT_NE(object_array_class, nullptr);
    jobject array_class = env()->GetObjectClass(array);
    ASSERT_TRUE(env()->IsSameObject(object_array_class, array_class));

    ASSERT_EQ("[alpha, beta]", runtime().array_to_string(array));

    env()->DeleteLocalRef(array_class);
    env()->DeleteLocalRef(object_array_class);
    env()->DeleteLocalRef(array);
    env()->DeleteLocalRef(beta);
    env()->DeleteLocalRef(alpha);
}

TEST_F(JavaRuntimeTest, CreateObject2DArray) {
    jobjectArray row0 = runtime().create_object_array(1);
    ASSERT_NE(row0, nullptr);
    jobjectArray row1 = runtime().create_object_array(1);
    ASSERT_NE(row1, nullptr);
    jobject elements[] = {row0, row1};

    jobjectArray array = runtime().create_object_2d_array(elements, 2);
    ASSERT_NE(array, nullptr);
    ASSERT_EQ(2, env()->GetArrayLength(array));

    jclass object_2d_array_class = env()->FindClass("[[Ljava/lang/Object;");
    ASSERT_NE(object_2d_array_class, nullptr);
    jobject array_class = env()->GetObjectClass(array);
    ASSERT_TRUE(env()->IsSameObject(object_2d_array_class, array_class));

    env()->DeleteLocalRef(array_class);
    env()->DeleteLocalRef(object_2d_array_class);
    env()->DeleteLocalRef(array);
    env()->DeleteLocalRef(row1);
    env()->DeleteLocalRef(row0);
}

TEST_F(JavaRuntimeTest, DumpExceptionString) {
    jclass clazz = env()->FindClass("java/lang/IllegalArgumentException");
    ASSERT_NE(clazz, nullptr);
    ASSERT_EQ(0, env()->ThrowNew(clazz, "runtime-boom"));

    jthrowable throwable = env()->ExceptionOccurred();
    ASSERT_NE(throwable, nullptr);
    std::string details = runtime().dump_exception_string(throwable);
    ASSERT_NE(std::string::npos, details.find("runtime-boom"));

    env()->ExceptionClear();
    env()->DeleteLocalRef(throwable);
    env()->DeleteLocalRef(clazz);
}

TEST_F(JavaRuntimeTest, DetectJavaRuntime) {
    ASSERT_OK(detect_java_runtime());
}

} // namespace starrocks
