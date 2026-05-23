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

#include "runtime/env/java/java_env.h"

#include <bthread/bthread.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>

#include "base/bthreads/util.h"
#include "base/metrics.h"
#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "common/system/cpu_info.h"
#include "runtime/env/global_env.h"
#include "runtime/runtime_env_test_util.h"

namespace starrocks {
namespace {

class JavaEnvTest : public testing::Test {
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

TEST_F(JavaEnvTest, GetJNIEnvReturnsStableEnvForCurrentThread) {
    ASSERT_NE(env(), nullptr);
    ASSERT_EQ(env(), getJNIEnv());
}

TEST_F(JavaEnvTest, JVMClassCreatesStringBuilderInstances) {
    jclass local_clazz = env()->FindClass("java/lang/StringBuilder");
    ASSERT_NE(local_clazz, nullptr);

    jobject global_clazz = env()->NewGlobalRef(local_clazz);
    env()->DeleteLocalRef(local_clazz);
    ASSERT_NE(global_clazz, nullptr);

    JVMClass clazz(global_clazz);

    auto local_instance_or = clazz.newLocalInstance();
    ASSERT_OK(local_instance_or);
    jobject local_instance = local_instance_or.value();
    ASSERT_NE(local_instance, nullptr);
    env()->DeleteLocalRef(local_instance);

    auto global_instance_or = clazz.newInstance();
    ASSERT_OK(global_instance_or);
    JavaGlobalRef global_instance = std::move(global_instance_or).value();
    ASSERT_NE(global_instance.handle(), nullptr);
}

TEST_F(JavaEnvTest, JavaGlobalRefCanClearFromBthread) {
    CpuInfo::init();
    runtime_env_test::set_small_thread_pool_configs();

    auto* global_env = GlobalEnv::GetInstance();
    global_env->destroy_thread_pools();
    DeferOp cleanup([global_env]() {
        global_env->shutdown_thread_pools();
        global_env->destroy_thread_pools();
    });

    MetricRegistry metrics("java_env_test");
    ASSERT_OK(global_env->init_execution_thread_pools(&metrics));
    ASSERT_NE(global_env->jvm_call_pool(), nullptr);

    jclass local_clazz = env()->FindClass("java/lang/StringBuilder");
    ASSERT_NE(local_clazz, nullptr);

    jobject global_clazz = env()->NewGlobalRef(local_clazz);
    env()->DeleteLocalRef(local_clazz);
    ASSERT_NE(global_clazz, nullptr);

    JavaGlobalRef ref(global_clazz);
    std::atomic<bool> ran_on_bthread = false;
    ASSERT_OK(bthreads::start_bthread_and_join([&]() {
        ran_on_bthread.store(bthread_self() != 0, std::memory_order_relaxed);
        ref.clear();
    }));

    ASSERT_TRUE(ran_on_bthread.load(std::memory_order_relaxed));
    ASSERT_EQ(ref.handle(), nullptr);
}

} // namespace starrocks
