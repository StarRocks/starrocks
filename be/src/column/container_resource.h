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

#include <memory>
#include <span>

#include "storage/rowset/page_handle_fwd.h"

namespace starrocks {
class faststring;

class ContainerResource {
public:
    ContainerResource() = default;
    ContainerResource(const std::shared_ptr<PageHandle>& handle, const void* data, size_t length)
            : _handle(handle), _data(data), _length(length) {}

    ContainerResource(const ContainerResource& other) {
        this->_data = other._data;
        this->_length = other._length;
        this->_handle = other._handle;
    }

    ContainerResource(ContainerResource&& other) noexcept {
        std::swap(this->_data, other._data);
        std::swap(this->_length, other._length);
        std::swap(this->_handle, other._handle);
    }

    ContainerResource& operator=(ContainerResource&& other) noexcept {
        std::swap(this->_data, other._data);
        std::swap(this->_length, other._length);
        std::swap(this->_handle, other._handle);
        return *this;
    }

    void acquire(const ContainerResource& other) {
        reset();
        _handle = other._handle;
    }

    template <class T>
    std::span<const T> span() const {
        return {reinterpret_cast<const T*>(_data), _length};
    }

    void reset() {
        _handle.reset();
        _data = nullptr;
    }

    bool empty() const { return _data == nullptr; }

    const void* data() const { return _data; }
    size_t length() const { return _length; }

    void set_data(const void* data) { _data = data; }
    void set_length(size_t length) { _length = length; }

    template <class T>
    bool is_aligned() const {
        if ((uintptr_t)_data % alignof(T) == 0) {
            return true;
        }
        return false;
    }

    bool owned() const { return _handle != nullptr; }

private:
    std::shared_ptr<PageHandle> _handle;

    const void* _data{};
    size_t _length{};
};
} // namespace starrocks