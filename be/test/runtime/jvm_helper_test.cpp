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

#include "runtime/java/jvm_helper.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <string>

#include "base/testutil/assert.h"
#include "runtime/java/java_env.h"

namespace starrocks {
namespace {

class JVMHelperTest : public testing::Test {
protected:
    void SetUp() override {
        if (std::getenv("JAVA_HOME") == nullptr) {
            GTEST_SKIP() << "JAVA_HOME is not set";
        }

        _env = getJNIEnv();
        if (_env == nullptr) {
            GTEST_SKIP() << "JVM is unavailable";
        }
    }

    JNIEnv* env() const { return _env; }

private:
    JNIEnv* _env = nullptr;
};

} // namespace

TEST_F(JVMHelperTest, GetInstanceInitializesThreadLocalEnv) {
    auto [helper_env, helper] = JVMHelper::getInstanceWithEnv();
    ASSERT_NE(helper_env, nullptr);
    ASSERT_EQ(helper_env, helper.getEnv());
    ASSERT_EQ(helper_env, getJNIEnv());
}

TEST_F(JVMHelperTest, ConvertsStringAndObjectArrays) {
    auto& helper = JVMHelper::getInstance();
    auto* env = this->env();

    ASSIGN_OR_ASSERT_FAIL(jstring first, helper.to_jstring("alpha"));
    LOCAL_REF_GUARD_ENV(env, first);
    ASSIGN_OR_ASSERT_FAIL(jstring second, helper.to_jstring("beta"));
    LOCAL_REF_GUARD_ENV(env, second);

    ASSERT_EQ("alpha", helper.to_cxx_string(first));

    jobject elements[] = {first, second};
    jobjectArray array = helper.build_object_array(helper.object_class(), elements, 2);
    ASSERT_NE(array, nullptr);
    LOCAL_REF_GUARD_ENV(env, array);
    ASSERT_EQ("[alpha, beta]", helper.array_to_string(array));
}

TEST_F(JVMHelperTest, BoxesPrimitiveAndBigDecimalValues) {
    auto& helper = JVMHelper::getInstance();
    auto* env = this->env();

    jobject integer = helper.newInteger(123);
    ASSERT_NE(integer, nullptr);
    LOCAL_REF_GUARD_ENV(env, integer);
    ASSERT_EQ(123, helper.valint32_t(integer));

    jobject big_decimal = helper.newBigDecimal(12345, 2);
    ASSERT_NE(big_decimal, nullptr);
    LOCAL_REF_GUARD_ENV(env, big_decimal);
    ASSERT_EQ("123.45", helper.to_string(big_decimal));
}

TEST_F(JVMHelperTest, ConvertsLocalDateEpochDay) {
    auto& helper = JVMHelper::getInstance();
    auto* env = this->env();

    jobject local_date = helper.newLocalDateFromEpochDay(0);
    ASSERT_NE(local_date, nullptr);
    LOCAL_REF_GUARD_ENV(env, local_date);
    ASSERT_EQ(0, helper.valLocalDateToEpochDay(local_date));
}

TEST_F(JVMHelperTest, UsesJavaListStub) {
    auto& helper = JVMHelper::getInstance();
    auto* env = this->env();

    ASSIGN_OR_ASSERT_FAIL(jobject list, helper.list_meta().array_list_class->newLocalInstance());
    LOCAL_REF_GUARD_ENV(env, list);

    ASSIGN_OR_ASSERT_FAIL(jstring first, helper.to_jstring("x"));
    LOCAL_REF_GUARD_ENV(env, first);
    ASSIGN_OR_ASSERT_FAIL(jstring second, helper.to_jstring("y"));
    LOCAL_REF_GUARD_ENV(env, second);

    JavaListStub stub(list);
    ASSERT_OK(stub.add(first));
    ASSERT_OK(stub.add(second));
    ASSIGN_OR_ASSERT_FAIL(auto size, stub.size());
    ASSERT_EQ(2, size);

    ASSIGN_OR_ASSERT_FAIL(jobject value, stub.get(1));
    LOCAL_REF_GUARD_ENV(env, value);
    ASSERT_EQ("y", helper.to_cxx_string(static_cast<jstring>(value)));
}

TEST_F(JVMHelperTest, WrapsDirectByteBuffer) {
    std::array<char, 4> data{'a', 'b', 'c', 'd'};
    DirectByteBuffer buffer(data.data(), data.size());

    ASSERT_NE(buffer.handle(), nullptr);
    ASSERT_EQ(data.data(), env()->GetDirectBufferAddress(buffer.handle()));
    ASSERT_EQ(data.size(), env()->GetDirectBufferCapacity(buffer.handle()));
}

} // namespace starrocks
