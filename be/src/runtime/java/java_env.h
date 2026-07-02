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

#pragma once

#include <functional>
#include <memory>

#include "base/status.h"

namespace starrocks {

class MetricRegistry;
class PriorityThreadPool;

class JavaEnv {
public:
    static JavaEnv* GetInstance();

    JavaEnv();
    ~JavaEnv();

    JavaEnv(const JavaEnv&) = delete;
    const JavaEnv& operator=(const JavaEnv&) = delete;

    Status init(MetricRegistry* metrics = nullptr, bool enable_jvm_metrics = false);
    void shutdown();
    void destroy();

    PriorityThreadPool* jvm_call_pool() const { return _jvm_call_pool.get(); }
    Status call_function_in_pthread(const std::function<Status()>& func);

private:
    Status _init_jvm_metrics(MetricRegistry* metrics, bool enable_jvm_metrics);

    std::unique_ptr<PriorityThreadPool> _jvm_call_pool;
    MetricRegistry* _jvm_metrics_registry = nullptr;
};

} // namespace starrocks
