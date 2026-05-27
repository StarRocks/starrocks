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

#include <functional>
#include <future>
#include <string>

#include "base/logging.h"
#include "base/status.h"
#include "common/thread/priority_thread_pool.hpp"
#include "runtime/env/global_env.h"

namespace starrocks {
namespace {

void run_jvm_cleanup_in_pthread(std::function<void()> func) {
    if (!bthread_self()) {
        func();
        return;
    }

    std::promise<void> promise;
    auto future = promise.get_future();
    if (!GlobalEnv::GetInstance()->jvm_call_pool()->offer([func = std::move(func), &promise]() mutable {
            func();
            promise.set_value();
        })) {
        LOG(WARNING) << "failed to submit JVM cleanup to jvm_call_pool";
        return;
    }
    future.get();
}

Status jni_exception_status(JNIEnv* env, const std::string& message) {
    if (!env->ExceptionCheck()) {
        return Status::OK();
    }
    env->ExceptionDescribe();
    env->ExceptionClear();
    return Status::InternalError(message);
}

} // namespace

JavaGlobalRef::~JavaGlobalRef() {
    clear();
}

void JavaGlobalRef::clear() {
    if (_handle) {
        run_jvm_cleanup_in_pthread([this]() {
            JNIEnv* env = getJNIEnv();
            DCHECK(env != nullptr) << "couldn't get a JNIEnv";
            env->DeleteGlobalRef(_handle);
            _handle = nullptr;
        });
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
