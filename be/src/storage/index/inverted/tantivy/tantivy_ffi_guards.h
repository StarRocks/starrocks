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

#include <tantivy_binding.h>

#include <string>
#include <utility>

#include "common/status.h"

namespace starrocks {

namespace tb = ::starrocks::tantivy_binding;

// Convert a failed RustResult into a StarRocks Status. The caller is
// responsible for freeing the RustResult (typically via TantivyResultGuard).
inline Status tantivy_status_from_error(const tb::RustResult& r) {
    if (r.success) return Status::OK();
    std::string msg = r.error ? r.error : "unknown tantivy error";
    return Status::InternalError("tantivy: " + msg);
}

struct TantivyResultGuard {
    tb::RustResult& result;
    explicit TantivyResultGuard(tb::RustResult& r) : result(r) {}
    ~TantivyResultGuard() { tb::free_rust_result(result); }
    TantivyResultGuard(const TantivyResultGuard&) = delete;
    TantivyResultGuard& operator=(const TantivyResultGuard&) = delete;
};

struct TantivyU32ArrayGuard {
    tb::RustU32Array& arr;
    explicit TantivyU32ArrayGuard(tb::RustU32Array& a) : arr(a) {}
    ~TantivyU32ArrayGuard() { tb::tantivy_free_u32_array(arr); }
    TantivyU32ArrayGuard(const TantivyU32ArrayGuard&) = delete;
    TantivyU32ArrayGuard& operator=(const TantivyU32ArrayGuard&) = delete;
};

class TantivyWriterGuard {
public:
    TantivyWriterGuard() = default;
    explicit TantivyWriterGuard(void* handle) : _handle(handle) {}
    ~TantivyWriterGuard() { reset(); }

    TantivyWriterGuard(TantivyWriterGuard&& o) noexcept : _handle(std::exchange(o._handle, nullptr)) {}
    TantivyWriterGuard& operator=(TantivyWriterGuard&& o) noexcept {
        if (this != &o) {
            reset();
            _handle = std::exchange(o._handle, nullptr);
        }
        return *this;
    }
    TantivyWriterGuard(const TantivyWriterGuard&) = delete;
    TantivyWriterGuard& operator=(const TantivyWriterGuard&) = delete;

    void reset() {
        if (_handle) {
            tb::tantivy_free_index_writer(_handle);
            _handle = nullptr;
        }
    }

    void* release() { return std::exchange(_handle, nullptr); }
    void* get() const { return _handle; }
    explicit operator bool() const { return _handle != nullptr; }

private:
    void* _handle = nullptr;
};

class TantivyReaderGuard {
public:
    TantivyReaderGuard() = default;
    explicit TantivyReaderGuard(void* handle) : _handle(handle) {}
    ~TantivyReaderGuard() { reset(); }

    TantivyReaderGuard(TantivyReaderGuard&& o) noexcept : _handle(std::exchange(o._handle, nullptr)) {}
    TantivyReaderGuard& operator=(TantivyReaderGuard&& o) noexcept {
        if (this != &o) {
            reset();
            _handle = std::exchange(o._handle, nullptr);
        }
        return *this;
    }
    TantivyReaderGuard(const TantivyReaderGuard&) = delete;
    TantivyReaderGuard& operator=(const TantivyReaderGuard&) = delete;

    void reset() {
        if (_handle) {
            tb::tantivy_free_index_reader(_handle);
            _handle = nullptr;
        }
    }

    void* release() { return std::exchange(_handle, nullptr); }
    void* get() const { return _handle; }
    explicit operator bool() const { return _handle != nullptr; }

private:
    void* _handle = nullptr;
};

class TantivyCompoundReaderGuard {
public:
    TantivyCompoundReaderGuard() = default;
    explicit TantivyCompoundReaderGuard(void* handle) : _handle(handle) {}
    ~TantivyCompoundReaderGuard() { reset(); }

    TantivyCompoundReaderGuard(TantivyCompoundReaderGuard&& o) noexcept : _handle(std::exchange(o._handle, nullptr)) {}
    TantivyCompoundReaderGuard& operator=(TantivyCompoundReaderGuard&& o) noexcept {
        if (this != &o) {
            reset();
            _handle = std::exchange(o._handle, nullptr);
        }
        return *this;
    }
    TantivyCompoundReaderGuard(const TantivyCompoundReaderGuard&) = delete;
    TantivyCompoundReaderGuard& operator=(const TantivyCompoundReaderGuard&) = delete;

    void reset() {
        if (_handle) {
            tb::tantivy_free_index_reader(_handle);
            _handle = nullptr;
        }
    }

    void* release() { return std::exchange(_handle, nullptr); }
    void* get() const { return _handle; }
    explicit operator bool() const { return _handle != nullptr; }

private:
    void* _handle = nullptr;
};

} // namespace starrocks
