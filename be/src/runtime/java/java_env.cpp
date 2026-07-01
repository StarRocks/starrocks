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

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <future>
#include <limits>

#include "base/status.h"
#include "common/config_exec_env_fwd.h"
#include "common/thread/priority_thread_pool.hpp"

namespace starrocks {

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

Status detect_java_runtime() {
    const char* p = std::getenv("JAVA_HOME");
    if (p == nullptr) {
        return Status::RuntimeError("env 'JAVA_HOME' is not set");
    }
    return JavaEnv::GetInstance()->call_function_in_pthread([]() {
        if (getJNIEnv() == nullptr) {
            return Status::RuntimeError("couldn't get JNIEnv, please check your java runtime");
        }
        return Status::OK();
    });
}

} // namespace starrocks
