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

#include <utility>

#include "base/statusor.h"

// implemented by libhdfs
// hadoop-hdfs-native-client/src/main/native/libhdfs/jni_helper.c
// Why do we need to use this function?
// 1. a thread can not attach to more than one virtual machine
// 2. libhdfs depends on this function and does some initialization,
// if the JVM has already created it, it won't create it anymore.
// If we skip this function call will cause libhdfs to miss some initialization operations
extern "C" JNIEnv* getJNIEnv(void);

namespace starrocks {

// A global ref of the guard, handle can be shared across threads.
class JavaGlobalRef {
public:
    JavaGlobalRef(jobject handle) : _handle(handle) {}
    ~JavaGlobalRef();
    JavaGlobalRef(const JavaGlobalRef&) = delete;

    JavaGlobalRef(JavaGlobalRef&& other) noexcept {
        _handle = other._handle;
        other._handle = nullptr;
    }

    JavaGlobalRef& operator=(JavaGlobalRef&& other) noexcept {
        JavaGlobalRef tmp(std::move(other));
        std::swap(this->_handle, tmp._handle);
        return *this;
    }

    jobject handle() const { return _handle; }

    jobject& handle() { return _handle; }

    void clear();

private:
    jobject _handle;
};

// A Class object created from the ClassLoader that can be accessed by multiple threads.
class JVMClass {
public:
    JVMClass(jobject clazz) : _clazz(clazz) {}
    JVMClass(const JVMClass&) = delete;

    JVMClass& operator=(const JVMClass&&) = delete;
    JVMClass& operator=(const JVMClass& other) = delete;

    JVMClass(JVMClass&& other) noexcept : _clazz(nullptr) { _clazz = std::move(other._clazz); }

    JVMClass& operator=(JVMClass&& other) noexcept {
        JVMClass tmp(std::move(other));
        std::swap(this->_clazz, tmp._clazz);
        return *this;
    }

    jclass clazz() const { return (jclass)_clazz.handle(); }

    // Create a new instance using the default constructor.
    StatusOr<JavaGlobalRef> newInstance() const;
    StatusOr<jobject> newLocalInstance() const;

private:
    JavaGlobalRef _clazz;
};

} // namespace starrocks
