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
#include <functional>
#include <future>
#include <limits>

#include "base/logging.h"
#include "base/status.h"
#include "common/config_exec_env_fwd.h"
#include "common/thread/priority_thread_pool.hpp"
#include "runtime/java/jvm_metrics.h"

namespace starrocks {

JavaEnv::JavaEnv() = default;

JavaEnv::~JavaEnv() = default;

Status JavaEnv::init(MetricRegistry* metrics, bool enable_jvm_metrics) {
    if (_jvm_call_pool == nullptr) {
        _jvm_call_pool = std::make_unique<PriorityThreadPool>(
                "jvm", std::max<int32_t>(1, config::jvm_call_thread_pool_size), std::numeric_limits<uint32_t>::max());
    }
    return _init_jvm_metrics(metrics, enable_jvm_metrics);
}

void JavaEnv::shutdown() {
    if (_jvm_call_pool) {
        _jvm_call_pool->shutdown();
    }
}

void JavaEnv::destroy() {
    _jvm_call_pool.reset();
    _jvm_metrics_registry = nullptr;
}

Status JavaEnv::_init_jvm_metrics(MetricRegistry* metrics, bool enable_jvm_metrics) {
#ifndef __APPLE__
    if (!enable_jvm_metrics || metrics == nullptr || _jvm_metrics_registry == metrics) {
        return Status::OK();
    }
    if (_jvm_metrics_registry != nullptr) {
        LOG(WARNING) << "jvm metrics are already installed on another registry, skip duplicate install";
        return Status::OK();
    }

    auto* jvm_metrics = JVMMetrics::instance();
    auto status = jvm_metrics->init();
    if (!status.ok()) {
        LOG(WARNING) << "init jvm metrics failed: " << status.to_string();
        return Status::OK();
    }
    jvm_metrics->install(metrics);
    _jvm_metrics_registry = metrics;
#endif
    return Status::OK();
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

} // namespace starrocks
