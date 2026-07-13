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

#include "runtime/java/java_global_ref.h"

#include "base/logging.h"
#include "base/status.h"
#include "runtime/java/java_env.h"
#include "runtime/java/jni_env.h"

namespace starrocks {

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

} // namespace starrocks
