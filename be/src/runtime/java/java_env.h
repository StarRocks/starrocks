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

#include <jni.h>

#include <functional>
#include <memory>

#include "base/status.h"

// implemented by libhdfs
// hadoop-hdfs-native-client/src/main/native/libhdfs/jni_helper.c
// Why do we need to use this function?
// 1. a thread can not attach to more than one virtual machine
// 2. libhdfs depends on this function and does some initialization,
// if the JVM has already created it, it won't create it anymore.
// If we skip this function call will cause libhdfs to miss some initialization operations
extern "C" JNIEnv* getJNIEnv(void);

namespace starrocks {

class PriorityThreadPool;

Status detect_java_runtime();

class JavaEnv {
public:
    static JavaEnv* GetInstance();

    JavaEnv();
    ~JavaEnv();

    JavaEnv(const JavaEnv&) = delete;
    const JavaEnv& operator=(const JavaEnv&) = delete;

    Status init();
    void shutdown();
    void destroy();

    PriorityThreadPool* jvm_call_pool() const { return _jvm_call_pool.get(); }
    Status call_function_in_pthread(const std::function<Status()>& func);

private:
    std::unique_ptr<PriorityThreadPool> _jvm_call_pool;
};

} // namespace starrocks
