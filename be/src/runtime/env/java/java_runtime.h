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

#include <string>

#include "common/status.h"
#include "common/statusor.h"

namespace starrocks {

// Process-wide Java runtime helper for generic JNI operations. JNIEnv is still
// cached per native thread because JNI explicitly scopes JNIEnv* to one thread.
class JavaRuntime {
public:
    static JavaRuntime& getInstance();

    JavaRuntime(const JavaRuntime&) = delete;
    JavaRuntime& operator=(const JavaRuntime&) = delete;

    JNIEnv* getEnv();

    std::string array_to_string(jobject object);
    std::string to_string(jobject obj);
    std::string to_cxx_string(jstring str);
    std::string dump_exception_string(jthrowable throwable);

    jmethodID get_to_string_method(jclass clazz);
    StatusOr<jstring> to_jstring(const std::string& str);
    jmethodID get_method(jclass clazz, const std::string& method, const std::string& sig);
    jmethodID get_static_method(jclass clazz, const std::string& method, const std::string& sig);

    jobjectArray create_object_array(int size);
    jobjectArray create_object_array(jobject fill, int size);
    jobjectArray create_object_array(jobject* elements, int size);
    jobjectArray create_object_2d_array(jobject* elements, int size);

    jclass object_class() const { return _object_class; }
    jclass object_array_class() const { return _object_array_class; }

    static std::string to_jni_class_name(const std::string& name);

private:
    JavaRuntime();

    jclass _find_global_class(const char* name);
    bool _check_exception(const std::string& message);

private:
    inline static thread_local JNIEnv* _env = nullptr;

    jclass _object_class = nullptr;
    jclass _object_array_class = nullptr;
    jclass _arrays_class = nullptr;
    jclass _exception_util_class = nullptr;

    jmethodID _arrays_to_string = nullptr;
    jmethodID _exception_get_stack_trace = nullptr;
};

Status detect_java_runtime();

} // namespace starrocks
