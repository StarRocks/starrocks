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

#include <cassert>
#include <cstring>
#include <iostream>
#include <string_view>

namespace starrocks {

// A wrapper around c-style string, with some convenient APIs like `size()`, `==`.
//
// The main advantage over std::string is that CString has a smaller value of `sizeof`.
//
// Unlike std::string, NO reserved space will be allocated. This implies that every time
// you assign a new string to CString, the previously allocated heap memory will be freed
// and a new heap memory is allocated.
//
// Time complexity of empty() is O(1).
// Time complexity of size() is O(n), where n is the length of string.
class CString {
    friend void swap(CString& lhs, CString& rhs);

public:
    CString() = default;
    ~CString() { _dealloc_if_needed(); }

    explicit CString(std::string_view v) {
        assert(v.size() == ::strnlen(v.data(), v.size()));
        assign(v.data(), v.size());
    }

    // Copy ctor
    CString(const CString& rhs) { assign(rhs.data(), rhs.size()); }

    // Move ctor
    CString(CString&& rhs) noexcept : _data(rhs._data) { rhs._data = &kStaticStorage; }

    // Copy assignment
    CString& operator=(const CString& rhs) {
        assign(rhs.data(), rhs.size());
        return *this;
    }

    // Move assignment
    CString& operator=(CString&& rhs) noexcept {
        _dealloc_if_needed();
        _data = rhs._data;
        rhs._data = &kStaticStorage;
        return *this;
    }

    // NOTE: it's caller's duty to ensure that the no zero character exist in |s|, otherwize
    // size() may return a value different from |s.size()|.
    CString& assign(std::string_view s) {
        assert(s.size() == ::strnlen(s.data(), s.size()));
        return assign(s.data(), s.size());
    }

    // NOTE: it's caller's duty to ensure that the no zero character exist in data[0...len), otherwize
    // size() may return a value different from |len|.
    CString& assign(const char* data, uint16_t len) {
        assert(len == ::strnlen(data, len));
        _dealloc_if_needed();
        char* p = new char[len + 1];
        memcpy(p, data, len);
        p[len] = '\0';
        _data = p;
        return *this;
    }

    void swap(CString* rhs) { std::swap(_data, rhs->_data); }

    const char* data() const { return _data; }

    size_t size() const { return std::strlen(_data); }

    size_t length() const { return size(); }

    bool empty() const { return _data[0] == '\0'; }

    char operator[](size_t pos) const { return _data[pos]; }

    auto operator<=>(const CString& rhs) const = default;

private:
    constexpr static char kStaticStorage = 0;

    void _dealloc_if_needed() {
        if (_data != &kStaticStorage) {
            delete[] _data;
            _data = &kStaticStorage;
        }
    }

    const char* _data = &kStaticStorage;
};

inline std::ostream& operator<<(std::ostream& os, const CString& s) {
    return os << s.data();
}

inline void swap(CString& lhs, CString& rhs) {
    lhs.swap(&rhs);
}

} // namespace starrocks
