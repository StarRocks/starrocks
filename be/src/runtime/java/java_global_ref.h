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

} // namespace starrocks
