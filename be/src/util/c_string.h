// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

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
    CString() {}
    ~CString() { _dealloc_if_needed(); }

    // Copy ctor
    CString(const CString& rhs) { assign(rhs.data(), rhs.size()); }

    // Move ctor
    CString(CString&& rhs) : _data(rhs._data) { rhs._data = &kStaticStorage; }

    // Copy assignment
    CString& operator=(const CString& rhs) {
        assign(rhs.data(), rhs.size());
        return *this;
    }

    // Move assignment
    CString& operator=(CString&& rhs) {
        _dealloc_if_needed();
        _data = rhs._data;
        rhs._data = &kStaticStorage;
        return *this;
    }

    // NOTE: it's caller's duty to ensure that the no zero character exist in |s|, otherwize
    // size() may return a value different from |s.size()|.
    CString& assign(const std::string_view& s) { return assign(s.data(), s.size()); }

    // NOTE: it's caller's duty to ensure that the no zero character exist in data[0...len), otherwize
    // size() may return a value different from |len|.
    CString& assign(const char* data, uint16_t len) {
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

    bool operator==(const CString& rhs) const { return std::strcmp(_data, rhs._data) == 0; }
    bool operator!=(const CString& rhs) const { return std::strcmp(_data, rhs._data) != 0; }
    bool operator<(const CString& rhs) const { return std::strcmp(_data, rhs._data) < 0; }
    bool operator<=(const CString& rhs) const { return std::strcmp(_data, rhs._data) <= 0; }
    bool operator>(const CString& rhs) const { return std::strcmp(_data, rhs._data) > 0; }
    bool operator>=(const CString& rhs) const { return std::strcmp(_data, rhs._data) >= 0; }

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
