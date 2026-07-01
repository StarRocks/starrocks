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

#include "udf/java/java_runtime.h"

#include <cstdlib>

#include "runtime/env/java/java_env.h"
#include "udf/java/utils.h"

namespace starrocks {

Status detect_java_runtime() {
    const char* p = std::getenv("JAVA_HOME");
    if (p == nullptr) {
        return Status::RuntimeError("env 'JAVA_HOME' is not set");
    }
    auto st = call_hdfs_scan_function_in_pthread([]() {
        if (getJNIEnv() == nullptr) {
            return Status::RuntimeError("couldn't get JNIEnv, please check your java runtime");
        }
        return Status::OK();
    });
    return st->get_future().get();
}

} // namespace starrocks
