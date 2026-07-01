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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <future>
#include <limits>
#include <string>

#include "base/logging.h"
#include "base/status.h"
#include "common/config_exec_env_fwd.h"
#include "common/thread/priority_thread_pool.hpp"

namespace starrocks {
namespace {

Status jni_exception_status(JNIEnv* env, const std::string& message) {
    if (!env->ExceptionCheck()) {
        return Status::OK();
    }
    env->ExceptionDescribe();
    env->ExceptionClear();
    return Status::InternalError(message);
}

} // namespace

JavaEnv::JavaEnv() = default;

JavaEnv::~JavaEnv() = default;

Status JavaEnv::init() {
    if (_jvm_call_pool != nullptr) {
        return Status::OK();
    }
    _jvm_call_pool = std::make_unique<PriorityThreadPool>(
            "jvm", std::max<int32_t>(1, config::jvm_call_thread_pool_size), std::numeric_limits<uint32_t>::max());
    return Status::OK();
}

void JavaEnv::shutdown() {
    if (_jvm_call_pool) {
        _jvm_call_pool->shutdown();
    }
}

void JavaEnv::destroy() {
    _jvm_call_pool.reset();
}

Status JavaEnv::call_function_in_pthread(const std::function<Status()>& func) {
    if (!bthread_self()) {
        return func();
    }

    if (_jvm_call_pool == nullptr) {
        return Status::InternalError("jvm_call_pool is not initialized");
    }

    std::promise<Status> promise;
    auto future = promise.get_future();
    if (!_jvm_call_pool->offer([func, &promise]() { promise.set_value(func()); })) {
        return Status::InternalError("failed to submit JVM call to jvm_call_pool");
    }
    return future.get();
}

JavaGlobalRef::~JavaGlobalRef() {
    clear();
}

void JavaGlobalRef::clear() {
    if (_handle) {
        auto st = JavaEnv::GetInstance()->call_function_in_pthread([this]() {
            JNIEnv* env = getJNIEnv();
            if (env == nullptr) {
                return Status::InternalError("couldn't get a JNIEnv");
            }
            env->DeleteGlobalRef(_handle);
            _handle = nullptr;
            return Status::OK();
        });
        LOG_IF(WARNING, !st.ok()) << "failed to clear Java global ref: " << st;
    }
}

StatusOr<JavaGlobalRef> JVMClass::newInstance() const {
    JNIEnv* env = getJNIEnv();
    if (env == nullptr) {
        return Status::InternalError("couldn't get a JNIEnv");
    }
    // get default constructor
    jmethodID constructor = env->GetMethodID((jclass)_clazz.handle(), "<init>", "()V");
    if (constructor == nullptr) {
        if (env->ExceptionCheck()) {
            env->ExceptionClear();
        }
        return Status::InternalError("couldn't found default constructor for Java Object");
    }
    auto local_ref = env->NewObject((jclass)_clazz.handle(), constructor);
    RETURN_IF_ERROR(jni_exception_status(env, "JNI Exception"));
    JavaGlobalRef global_ref(env->NewGlobalRef(local_ref));
    env->DeleteLocalRef(local_ref);
    return std::move(global_ref);
}

StatusOr<jobject> JVMClass::newLocalInstance() const {
    JNIEnv* env = getJNIEnv();
    if (env == nullptr) {
        return Status::InternalError("couldn't get a JNIEnv");
    }
    // get default constructor
    jmethodID constructor = env->GetMethodID((jclass)_clazz.handle(), "<init>", "()V");
    if (constructor == nullptr) {
        if (env->ExceptionCheck()) {
            env->ExceptionClear();
        }
        return Status::InternalError("couldn't found default constructor for Java Object");
    }
    auto res = env->NewObject((jclass)_clazz.handle(), constructor);
    RETURN_IF_ERROR(jni_exception_status(env, "JNI Exception"));
    return res;
}

} // namespace starrocks
