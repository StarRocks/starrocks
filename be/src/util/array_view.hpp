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

#include <cstddef>
#include <cstdint>

namespace starrocks {
// TODO: using CXX20 std::span instead of this class
template <typename T>
class array_view {
public:
    array_view(T* ptr, size_t len) noexcept : _ptr(ptr), _len(len) {}
    template <class Container>
    array_view(const Container& container) noexcept : _ptr(container.data()), _len(container.size()) {}

    T const& operator[](int i) const noexcept { return _ptr[i]; }
    size_t size() const noexcept { return _len; }
    bool empty() const noexcept { return size() == 0; }

    auto begin() const noexcept { return _ptr; }
    auto end() const noexcept { return _ptr + _len; }

private:
    const T* _ptr = nullptr;
    size_t _len;
};
} // namespace starrocks
