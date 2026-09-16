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

#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "common/statusor.h"
#include "jni.h"
#include "runtime/java/java_global_ref.h"
#include "runtime/java/jvm_class.h"
#include "types/logical_type.h"

namespace starrocks {

// For loading UDF classes. Not thread safe.
class JavaUdfClassLoader {
public:
    static const inline int BATCH_SINGLE_UPDATE = 1;
    static const inline int BATCH_EVALUATE = 2;

    explicit JavaUdfClassLoader(std::string path) : _path(std::move(path)) {}
    ~JavaUdfClassLoader();

    JavaUdfClassLoader& operator=(const JavaUdfClassLoader& other) = delete;
    JavaUdfClassLoader(const JavaUdfClassLoader&) = delete;

    StatusOr<JVMClass> getClass(const std::string& className);
    StatusOr<JVMClass> genCallStub(const std::string& stubClassName, jclass clazz, jobject method, int type,
                                   int numActualVarArgs = 0);

    Status init();

private:
    std::string _path;
    jmethodID _get_class = nullptr;
    jmethodID _get_call_stub = nullptr;
    JavaGlobalRef _handle = nullptr;
    JavaGlobalRef _clazz = nullptr;
};

struct JavaUdfMethodTypeDescriptor {
    LogicalType type;
    bool is_box;
    bool is_array = false;
};

struct JavaUdfMethodDescriptor {
    std::string signature;
    std::string name;
    std::vector<JavaUdfMethodTypeDescriptor> method_desc;
    JavaGlobalRef method = nullptr;

    // thread safe
    jmethodID get_method_id() const;
};

// Used to get UDF function signatures.
class JavaUdfClassAnalyzer {
public:
    JavaUdfClassAnalyzer() = default;
    ~JavaUdfClassAnalyzer() = default;

    // Strip generic type parameters from a JNI method signature.
    // e.g. "(Ljava/util/List<java/lang/String>;)V" -> "(Ljava/util/List;)V"
    static void strip_jni_generic_types(std::string* sign);

    Status has_method(jclass clazz, const std::string& method, bool* has);
    Status get_signature(jclass clazz, const std::string& method, std::string* sign);
    Status get_method_desc(const std::string& sign, std::vector<JavaUdfMethodTypeDescriptor>* desc);
    StatusOr<jobject> get_method_object(jclass clazz, const std::string& method_name);
    Status get_udaf_method_desc(const std::string& sign, std::vector<JavaUdfMethodTypeDescriptor>* desc);
};

} // namespace starrocks
