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

#include "runtime/env/java/java_runtime.h"

#include <bthread/bthread.h>

#include <cstdlib>
#include <future>
#include <utility>

#include "base/logging.h"
#include "common/thread/priority_thread_pool.hpp"
#include "fmt/core.h"
#include "runtime/env/global_env.h"
#include "runtime/env/java/java_env.h"

namespace starrocks {
namespace {

class ScopedLocalRef {
public:
    ScopedLocalRef(JNIEnv* env, jobject ref) : _env(env), _ref(ref) {}
    ~ScopedLocalRef() {
        if (_ref != nullptr) {
            _env->DeleteLocalRef(_ref);
        }
    }

    ScopedLocalRef(const ScopedLocalRef&) = delete;
    ScopedLocalRef& operator=(const ScopedLocalRef&) = delete;

private:
    JNIEnv* _env;
    jobject _ref;
};

Status check_java_runtime_on_current_thread() {
    if (getJNIEnv() == nullptr) {
        return Status::RuntimeError("couldn't get JNIEnv, please check your java runtime");
    }
    return Status::OK();
}

Status check_java_runtime_in_pthread() {
    if (!bthread_self()) {
        return check_java_runtime_on_current_thread();
    }

    auto* pool = GlobalEnv::GetInstance()->jvm_call_pool();
    if (pool == nullptr) {
        return Status::RuntimeError("jvm_call_pool is not initialized");
    }

    std::promise<Status> promise;
    auto future = promise.get_future();
    if (!pool->offer([&promise]() { promise.set_value(check_java_runtime_on_current_thread()); })) {
        return Status::RuntimeError("failed to submit JVM runtime detection to jvm_call_pool");
    }
    return future.get();
}

} // namespace

JavaRuntime& JavaRuntime::getInstance() {
    static JavaRuntime runtime;
    return runtime;
}

JavaRuntime::JavaRuntime() {
    _object_class = _find_global_class("java/lang/Object");
    _object_array_class = _find_global_class("[Ljava/lang/Object;");
    _arrays_class = _find_global_class("java/util/Arrays");
    _exception_util_class = _find_global_class("org/apache/commons/lang3/exception/ExceptionUtils");

    _arrays_to_string =
            getEnv()->GetStaticMethodID(_arrays_class, "toString", "([Ljava/lang/Object;)Ljava/lang/String;");
    CHECK(_arrays_to_string != nullptr);
    _exception_get_stack_trace = getEnv()->GetStaticMethodID(_exception_util_class, "getStackTrace",
                                                             "(Ljava/lang/Throwable;)Ljava/lang/String;");
    CHECK(_exception_get_stack_trace != nullptr);
}

JNIEnv* JavaRuntime::getEnv() {
    if (_env == nullptr) {
        _env = getJNIEnv();
        CHECK(_env != nullptr) << "couldn't get a JNIEnv";
    }
    return _env;
}

jclass JavaRuntime::_find_global_class(const char* name) {
    JNIEnv* env = getEnv();
    jclass local_clazz = env->FindClass(name);
    CHECK(local_clazz != nullptr) << "not found class " << name << " plz check JDK and jni-packages";
    ScopedLocalRef local_guard(env, local_clazz);
    auto global_clazz = static_cast<jclass>(env->NewGlobalRef(local_clazz));
    CHECK(global_clazz != nullptr) << "failed to create global class ref for " << name;
    return global_clazz;
}

bool JavaRuntime::_check_exception(const std::string& message) {
    JNIEnv* env = getEnv();
    if (jthrowable throwable = env->ExceptionOccurred()) {
        ScopedLocalRef throwable_guard(env, throwable);
        std::string details = dump_exception_string(throwable);
        LOG(WARNING) << message << "," << details;
        env->ExceptionClear();
        return true;
    }
    return false;
}

std::string JavaRuntime::array_to_string(jobject object) {
    JNIEnv* env = getEnv();
    env->ExceptionClear();
    jobject jstr = env->CallStaticObjectMethod(_arrays_class, _arrays_to_string, object);
    ScopedLocalRef jstr_guard(env, jstr);
    if (_check_exception("Exception happened when call array_to_string")) {
        return "";
    }
    return to_cxx_string(static_cast<jstring>(jstr));
}

std::string JavaRuntime::to_string(jobject obj) {
    JNIEnv* env = getEnv();
    env->ExceptionClear();
    jobject res = env->CallObjectMethod(obj, get_to_string_method(_object_class));
    ScopedLocalRef res_guard(env, res);
    if (_check_exception("Exception happened when call to_string")) {
        return "";
    }
    return to_cxx_string(static_cast<jstring>(res));
}

std::string JavaRuntime::to_cxx_string(jstring str) {
    if (str == nullptr) {
        return "<null>";
    }
    JNIEnv* env = getEnv();
    const char* chars = env->GetStringUTFChars(str, nullptr);
    if (chars == nullptr) {
        env->ExceptionClear();
        return "";
    }
    std::string res = chars;
    env->ReleaseStringUTFChars(str, chars);
    return res;
}

std::string JavaRuntime::dump_exception_string(jthrowable throwable) {
    JNIEnv* env = getEnv();
    jobject stack_traces = env->CallStaticObjectMethod(_exception_util_class, _exception_get_stack_trace,
                                                       static_cast<jobject>(throwable));
    ScopedLocalRef stack_traces_guard(env, stack_traces);
    // Avoid recursive exception reporting through this helper.
    env->ExceptionClear();
    return to_cxx_string(static_cast<jstring>(stack_traces));
}

jmethodID JavaRuntime::get_to_string_method(jclass clazz) {
    return getEnv()->GetMethodID(clazz, "toString", "()Ljava/lang/String;");
}

StatusOr<jstring> JavaRuntime::to_jstring(const std::string& str) {
    auto* env = getEnv();
    auto res = env->NewStringUTF(str.c_str());
    if (res == nullptr) {
        env->ExceptionClear();
        return Status::InternalError(fmt::format("NewStringUTF failed for: {}", str));
    }
    return res;
}

jmethodID JavaRuntime::get_method(jclass clazz, const std::string& method, const std::string& sig) {
    return getEnv()->GetMethodID(clazz, method.c_str(), sig.c_str());
}

jmethodID JavaRuntime::get_static_method(jclass clazz, const std::string& method, const std::string& sig) {
    return getEnv()->GetStaticMethodID(clazz, method.c_str(), sig.c_str());
}

jobjectArray JavaRuntime::create_object_array(int size) {
    auto* env = getEnv();
    auto res = env->NewObjectArray(size, _object_class, nullptr);
    if (_check_exception("create_object_array: NewObjectArray failed")) {
        return nullptr;
    }
    return res;
}

jobjectArray JavaRuntime::create_object_array(jobject fill, int size) {
    auto* env = getEnv();
    auto res = env->NewObjectArray(size, _object_class, fill);
    if (_check_exception("create_object_array: NewObjectArray failed")) {
        return nullptr;
    }
    return res;
}

jobjectArray JavaRuntime::create_object_array(jobject* elements, int size) {
    auto* env = getEnv();
    auto res = env->NewObjectArray(size, _object_class, nullptr);
    if (_check_exception("create_object_array: NewObjectArray failed")) {
        return nullptr;
    }
    for (int i = 0; i < size; ++i) {
        env->SetObjectArrayElement(res, i, elements[i]);
        if (_check_exception("create_object_array: SetObjectArrayElement failed")) {
            env->DeleteLocalRef(res);
            return nullptr;
        }
    }
    return res;
}

jobjectArray JavaRuntime::create_object_2d_array(jobject* elements, int size) {
    auto* env = getEnv();
    auto res = env->NewObjectArray(size, _object_array_class, nullptr);
    if (_check_exception("create_object_2d_array: NewObjectArray failed")) {
        return nullptr;
    }
    for (int i = 0; i < size; ++i) {
        env->SetObjectArrayElement(res, i, elements[i]);
        if (_check_exception("create_object_2d_array: SetObjectArrayElement failed")) {
            env->DeleteLocalRef(res);
            return nullptr;
        }
    }
    return res;
}

std::string JavaRuntime::to_jni_class_name(const std::string& name) {
    std::string res = name;
    for (auto& c : res) {
        if (c == '.') {
            c = '/';
        }
    }
    return res;
}

Status detect_java_runtime() {
    const char* java_home = std::getenv("JAVA_HOME");
    if (java_home == nullptr) {
        return Status::RuntimeError("env 'JAVA_HOME' is not set");
    }
    return check_java_runtime_in_pthread();
}

} // namespace starrocks
