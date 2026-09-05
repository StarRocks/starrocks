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

#include "runtime/java/java_env.h"

#include <bthread/bthread.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <string>

#include "base/bthreads/util.h"
#include "base/testutil/assert.h"
#include "base/utility/defer_op.h"
#include "common/system/cpu_info.h"
#include "runtime/current_thread.h"
#include "runtime/java/java_global_ref.h"
#include "runtime/java/java_runtime.h"
#include "runtime/java/jni_env.h"
#include "runtime/java/jvm_class.h"
#include "runtime/runtime_env.h"
#include "runtime/runtime_env_test_util.h"
#include "runtime/runtime_state.h"

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

TEST_F(JavaEnvTest, DetectJavaRuntimeReturnsOk) {
    ASSERT_OK(detect_java_runtime());
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

    auto* java_env = RuntimeEnv::GetInstance()->java_env();
    java_env->destroy();
    DeferOp cleanup([java_env]() {
        java_env->shutdown();
        java_env->destroy();
    });

    ASSERT_OK(java_env->init());
    ASSERT_NE(java_env->jvm_call_pool(), nullptr);

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

TEST_F(JavaEnvTest, DetectJavaRuntimeCanRunFromBthread) {
    CpuInfo::init();
    runtime_env_test::set_small_thread_pool_configs();

    auto* java_env = RuntimeEnv::GetInstance()->java_env();
    java_env->destroy();
    DeferOp cleanup([java_env]() {
        java_env->shutdown();
        java_env->destroy();
    });

    ASSERT_OK(java_env->init());

    Status st;
    ASSERT_OK(bthreads::start_bthread_and_join([&]() { st = detect_java_runtime(); }));
    ASSERT_OK(st);
}

TEST_F(JavaEnvTest, CallFunctionInPthreadUsesJvmPoolFromBthread) {
    CpuInfo::init();
    runtime_env_test::set_small_thread_pool_configs();

    auto* java_env = RuntimeEnv::GetInstance()->java_env();
    java_env->destroy();
    DeferOp cleanup([java_env]() {
        java_env->shutdown();
        java_env->destroy();
    });

    ASSERT_OK(java_env->init());

    Status st;
    std::atomic<bool> caller_was_bthread = false;
    std::atomic<bool> func_ran_on_pthread = false;
    ASSERT_OK(bthreads::start_bthread_and_join([&]() {
        caller_was_bthread.store(bthread_self() != 0, std::memory_order_relaxed);
        st = java_env->call_function_in_pthread([&]() {
            func_ran_on_pthread.store(bthread_self() == 0, std::memory_order_relaxed);
            return Status::OK();
        });
    }));

    ASSERT_TRUE(caller_was_bthread.load(std::memory_order_relaxed));
    ASSERT_TRUE(func_ran_on_pthread.load(std::memory_order_relaxed));
    ASSERT_OK(st);
}

TEST_F(JavaEnvTest, CallFunctionInPthreadPropagatesStatusFromBthread) {
    CpuInfo::init();
    runtime_env_test::set_small_thread_pool_configs();

    auto* java_env = RuntimeEnv::GetInstance()->java_env();
    java_env->destroy();
    DeferOp cleanup([java_env]() {
        java_env->shutdown();
        java_env->destroy();
    });

    ASSERT_OK(java_env->init());

    Status st;
    ASSERT_OK(bthreads::start_bthread_and_join([&]() {
        st = java_env->call_function_in_pthread(
                []() { return Status::InternalError("expected java env dispatch failure"); });
    }));

    ASSERT_ERROR(st);
    ASSERT_EQ("expected java env dispatch failure", std::string(st.message()));
}

TEST_F(JavaEnvTest, SubmitJavaUdfCallUsesUdfPoolFromBthread) {
    CpuInfo::init();
    runtime_env_test::set_small_thread_pool_configs();

    auto* java_env = RuntimeEnv::GetInstance()->java_env();
    java_env->destroy();
    DeferOp cleanup([java_env]() {
        java_env->shutdown();
        java_env->destroy();
    });

    ASSERT_OK(java_env->init());
    ASSERT_NE(java_env->udf_call_pool(), nullptr);

    RuntimeState state;
    state.init_instance_mem_tracker();
    auto* expected_tracker = state.instance_mem_tracker();

    Status st;
    std::atomic<bool> caller_was_bthread = false;
    std::atomic<bool> func_ran_on_pthread = false;
    std::atomic<bool> tracker_matched = false;
    ASSERT_OK(bthreads::start_bthread_and_join([&]() {
        caller_was_bthread.store(bthread_self() != 0, std::memory_order_relaxed);
        auto promise = java_env->submit_java_udf_call(&state, [&]() {
            func_ran_on_pthread.store(bthread_self() == 0, std::memory_order_relaxed);
            auto* observed_tracker = tls_thread_status.set_mem_tracker(nullptr);
            tracker_matched.store(observed_tracker == expected_tracker, std::memory_order_relaxed);
            tls_thread_status.set_mem_tracker(observed_tracker);
            return Status::OK();
        });
        st = promise->get_future().get();
    }));

    ASSERT_TRUE(caller_was_bthread.load(std::memory_order_relaxed));
    ASSERT_TRUE(func_ran_on_pthread.load(std::memory_order_relaxed));
    ASSERT_TRUE(tracker_matched.load(std::memory_order_relaxed));
    ASSERT_OK(st);
}

TEST_F(JavaEnvTest, SubmitJavaUdfCallReportsUninitializedPoolFromBthread) {
    CpuInfo::init();
    runtime_env_test::set_small_thread_pool_configs();

    auto* java_env = RuntimeEnv::GetInstance()->java_env();
    java_env->destroy();
    DeferOp cleanup([java_env]() {
        java_env->shutdown();
        java_env->destroy();
    });

    RuntimeState state;
    state.init_instance_mem_tracker();

    Status st;
    ASSERT_OK(bthreads::start_bthread_and_join([&]() {
        auto promise = java_env->submit_java_udf_call(&state, []() { return Status::OK(); });
        st = promise->get_future().get();
    }));

    ASSERT_ERROR(st);
    ASSERT_EQ("udf_call_pool is not initialized", std::string(st.message()));
}

} // namespace starrocks
