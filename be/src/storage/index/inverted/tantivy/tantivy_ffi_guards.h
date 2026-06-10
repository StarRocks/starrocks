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

// RAII for the parallel BM25 score array returned by scored query FFI.
struct TantivyF32ArrayGuard {
    tb::RustF32Array& arr;
    explicit TantivyF32ArrayGuard(tb::RustF32Array& a) : arr(a) {}
    ~TantivyF32ArrayGuard() { tb::tantivy_free_f32_array(arr); }
    TantivyF32ArrayGuard(const TantivyF32ArrayGuard&) = delete;
    TantivyF32ArrayGuard& operator=(const TantivyF32ArrayGuard&) = delete;
};

// Owning RAII handle for any FFI pointer that tantivy hands back through
// `*mut c_void`. The free function is captured as a non-type template
// parameter, so the dispatch is resolved at compile time and we don't pay
// for a virtual call in destructors. Three concrete aliases below cover the
// writer, the local reader, and the compound reader — note that the
// reader/compound-reader aliases share `tantivy_free_index_reader` because
// the FFI returns the same underlying `IndexReaderWrapper*` for both load
// paths (see tantivy/AGENTS.md "Unified reader handle").
template <auto FreeFn>
class TantivyFfiHandle {
public:
    TantivyFfiHandle() = default;
    explicit TantivyFfiHandle(void* handle) : _handle(handle) {}
    ~TantivyFfiHandle() { reset(); }

    TantivyFfiHandle(TantivyFfiHandle&& o) noexcept : _handle(std::exchange(o._handle, nullptr)) {}
    TantivyFfiHandle& operator=(TantivyFfiHandle&& o) noexcept {
        if (this != &o) {
            reset();
            _handle = std::exchange(o._handle, nullptr);
        }
        return *this;
    }
    TantivyFfiHandle(const TantivyFfiHandle&) = delete;
    TantivyFfiHandle& operator=(const TantivyFfiHandle&) = delete;

    void reset() {
        if (_handle) {
            FreeFn(_handle);
            _handle = nullptr;
        }
    }

    void* release() { return std::exchange(_handle, nullptr); }
    void* get() const { return _handle; }
    explicit operator bool() const { return _handle != nullptr; }

private:
    void* _handle = nullptr;
};

using TantivyWriterGuard = TantivyFfiHandle<&tb::tantivy_free_index_writer>;
using TantivyReaderGuard = TantivyFfiHandle<&tb::tantivy_free_index_reader>;
using TantivyCompoundReaderGuard = TantivyFfiHandle<&tb::tantivy_free_index_reader>;

} // namespace starrocks
