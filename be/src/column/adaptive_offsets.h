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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>

#include "base/container/raw_container.h"
#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"

namespace starrocks {

// AdaptiveOffsets is a specialized offset container for BinaryColumn-style
// offset arrays. It stores offsets in Small (uint32_t) while every value fits
// and promotes at most once to Large (uint64_t) when a value is known to
// exceed UINT32_MAX.
//
// Intended usage modes
//   1. Generic scalar access (get/set/push_back) for fallback paths that do
//      not know the final offset upper bound in advance.
//   2. Width-resolved fast paths for callers that can decide small-vs-large
//      storage once up front, via ensure_width_for_value() plus either
//      visit_storage() for loops or the typed scalar helpers for scalar code.
//   3. Whole-buffer ownership transfer APIs for boundary/helper paths such as
//      serde, builders, or column conversion.
//
// This is not a full std::vector replacement; it only exposes the operations
// BinaryColumn offset handling actually needs.
class AdaptiveOffsets {
public:
    using Small = Buffer<uint32_t>;
    using Large = Buffer<uint64_t>;

    // ========================= constructors =========================

    AdaptiveOffsets() = default;

    explicit AdaptiveOffsets(size_t n) { _u32.resize(n); }

    AdaptiveOffsets(const AdaptiveOffsets&) = default;
    AdaptiveOffsets& operator=(const AdaptiveOffsets&) = default;
    AdaptiveOffsets(AdaptiveOffsets&&) noexcept = default;
    AdaptiveOffsets& operator=(AdaptiveOffsets&&) noexcept = default;

    // ========================= capacity / state =========================

    bool is_large() const { return _large; }

    size_t size() const {
        if (LIKELY(!_large)) return _u32.size();
        return _u64.size();
    }

    size_t capacity() const {
        if (LIKELY(!_large)) return _u32.capacity();
        return _u64.capacity();
    }

    bool empty() const {
        if (LIKELY(!_large)) return _u32.empty();
        return _u64.empty();
    }

    // Counts both buffers to be safe — shrink_to_fit is non-binding.
    size_t memory_usage() const { return _u32.capacity() * sizeof(uint32_t) + _u64.capacity() * sizeof(uint64_t); }

    // Width of each stored element in bytes (4 or 8).
    size_t element_size() const {
        if (LIKELY(!_large)) return sizeof(uint32_t);
        return sizeof(uint64_t);
    }

    // ========================= width management =========================

    // For builders that know the final offset upper bound up front. This is
    // the primitive behind width-resolved fast paths: decide small-vs-large
    // storage once, then enter either visit_storage() or typed scalar helpers
    // without paying the generic per-element checks.
    ALWAYS_INLINE void ensure_width_for_value(uint64_t v) {
        if (UNLIKELY(!_large && v > std::numeric_limits<uint32_t>::max())) {
            _promote_to_large();
        }
    }

    // ========================= generic scalar access =========================

    ALWAYS_INLINE uint64_t get(size_t i) const {
        if (LIKELY(!_large)) return _u32[i];
        return _u64[i];
    }

    ALWAYS_INLINE void set(size_t i, uint64_t v) {
        if (LIKELY(!_large)) {
            if (LIKELY(v <= std::numeric_limits<uint32_t>::max())) {
                _u32[i] = static_cast<uint32_t>(v);
                return;
            }
            _promote_to_large();
        }
        _u64[i] = v;
    }

    ALWAYS_INLINE uint64_t operator[](size_t i) const { return get(i); }

    ALWAYS_INLINE uint64_t back() const {
        if (LIKELY(!_large)) return _u32.back();
        return _u64.back();
    }

    // ========================= typed scalar fast paths =========================

    // Direct small/large scalar accessors for callers that have already
    // resolved width once and do not want the generic per-element checks in
    // get/set/push_back. Prefer visit_storage() for loops over many elements;
    // these helpers are mainly for scalar code that still benefits from
    // width-specialized access.
    ALWAYS_INLINE uint32_t get_small(size_t i) const {
        DCHECK(!_large);
        return _u32[i];
    }

    ALWAYS_INLINE uint64_t get_large(size_t i) const {
        DCHECK(_large);
        return _u64[i];
    }

    ALWAYS_INLINE uint32_t back_small() const {
        DCHECK(!_large);
        return _u32.back();
    }

    ALWAYS_INLINE uint64_t back_large() const {
        DCHECK(_large);
        return _u64.back();
    }

    ALWAYS_INLINE void set_small(size_t i, uint32_t v) {
        DCHECK(!_large);
        _u32[i] = v;
    }

    ALWAYS_INLINE void set_large(size_t i, uint64_t v) {
        DCHECK(_large);
        _u64[i] = v;
    }

    ALWAYS_INLINE void push_back_small(uint32_t v) {
        DCHECK(!_large);
        _u32.push_back(v);
    }

    ALWAYS_INLINE void push_back_large(uint64_t v) {
        DCHECK(_large);
        _u64.push_back(v);
    }

    ALWAYS_INLINE void emplace_back_small(uint32_t v) { push_back_small(v); }

    ALWAYS_INLINE void emplace_back_large(uint64_t v) { push_back_large(v); }

    // ========================= size modifiers =========================

    void reserve(size_t n) {
        if (LIKELY(!_large)) {
            _u32.reserve(n);
        } else {
            _u64.reserve(n);
        }
    }

    void resize(size_t n) {
        if (LIKELY(!_large)) {
            _u32.resize(n);
        } else {
            _u64.resize(n);
        }
    }

    void resize(size_t n, uint64_t fill) {
        if (LIKELY(!_large && fill <= std::numeric_limits<uint32_t>::max())) {
            _u32.resize(n, static_cast<uint32_t>(fill));
        } else {
            if (!_large) _promote_to_large();
            _u64.resize(n, fill);
        }
    }

    // Intentionally keep small-vs-large state as-is: clearing does not
    // promote/demote storage. This avoids repeated toggling if the container is
    // reused across batches with similar sizes.
    void clear() {
        if (LIKELY(!_large)) {
            _u32.clear();
        } else {
            _u64.clear();
        }
    }

    // Force back to small mode and release large storage. Small-buffer
    // capacity is intentionally retained; this is intended for reset/reuse
    // paths across unrelated workloads.
    void reset() {
        _u32.clear();
        _u64.clear();
        _u64.shrink_to_fit();
        _large = false;
    }

    // ========================= generic scalar append =========================

    ALWAYS_INLINE void push_back(uint64_t v) {
        if (LIKELY(!_large)) {
            if (LIKELY(v <= std::numeric_limits<uint32_t>::max())) {
                _u32.push_back(static_cast<uint32_t>(v));
                return;
            }
            _promote_to_large();
        }
        _u64.push_back(v);
    }

    ALWAYS_INLINE void emplace_back(uint64_t v) { push_back(v); }

    // ========================= bulk allocation helpers =========================

    // Replace the active storage with |n| uninitialized slots. Existing
    // contents are discarded. Callers must fully overwrite [0, n) before any
    // read or promotion path observes these offsets.
    void make_room(size_t n) {
        if (LIKELY(!_large)) return raw::make_room(&_u32, n);
        return raw::make_room(&_u64, n);
    }

    // Hot-path overload: caller guarantees |max_value| covers every value that
    // will be written into the new active storage, so small-vs-large storage
    // can be decided up front and the active storage can grow without a later
    // promotion.
    void make_room(size_t n, uint64_t max_value) {
        ensure_width_for_value(max_value);
        make_room(n);
    }

    // Raw uninitialized resize. Existing prefix is preserved; newly exposed
    // range [old_size, n) must be fully overwritten before any read or
    // promotion path observes it.
    void resize_uninitialized(size_t n) {
        if (LIKELY(!_large)) return raw::stl_vector_resize_uninitialized(&_u32, n);
        return raw::stl_vector_resize_uninitialized(&_u64, n);
    }

    // Hot-path overload matching make_room(size_t, uint64_t).
    void resize_uninitialized(size_t n, uint64_t max_value) {
        ensure_width_for_value(max_value);
        resize_uninitialized(n);
    }

    // ========================= swap =========================

    void swap(AdaptiveOffsets& o) noexcept {
        std::swap(_large, o._large);
        _u32.swap(o._u32);
        _u64.swap(o._u64);
    }

    // ========================= storage visitation =========================

    // For boundary paths or hot loops that need one-time width dispatch and
    // direct typed buffer access. Prefer this over the scalar fast-path
    // helpers when processing many offsets in a loop. Prefer the const
    // overload where possible.
    //
    // Usage:
    //   offsets.visit_storage([&](auto& buf) {
    //       // buf is Buffer<uint32_t>& or Buffer<uint64_t>&
    //       memcpy(dst, buf.data(), buf.size() * sizeof(buf[0]));
    //   });
    template <typename F>
    ALWAYS_INLINE decltype(auto) visit_storage(F&& f) {
        if (LIKELY(!_large)) return f(_u32);
        return f(_u64);
    }

    template <typename F>
    ALWAYS_INLINE decltype(auto) visit_storage(F&& f) const {
        if (LIKELY(!_large)) return f(_u32);
        return f(_u64);
    }

    // ========================= ownership transfer =========================

    // Extract the large buffer, leaving this object empty-and-large. This is a
    // boundary/helper API for whole-buffer transfer, not a hot-path accessor.
    Large take_large_buffer() {
        DCHECK(_large);
        Large result = std::move(_u64);
        _u64.clear();
        return result;
    }

    // Replace current contents with a prebuilt small buffer. This is a
    // boundary/helper API for whole-buffer transfer, not a hot-path accessor.
    void set_small_buffer(Small&& buf) {
        _large = false;
        _u32 = std::move(buf);
        _u64.clear();
        _u64.shrink_to_fit();
    }

    // Replace current contents with a prebuilt large buffer. This is a
    // boundary/helper API for whole-buffer transfer, not a hot-path accessor.
    void set_large_buffer(Large&& buf) {
        _large = true;
        _u64 = std::move(buf);
        _u32.clear();
        _u32.shrink_to_fit();
    }

private:
    // One-time promotion from small to large. O(n) but happens at most once.
    void _promote_to_large();

    bool _large = false;
    Small _u32;
    Large _u64;
};

// Free-standing swap for ADL.
inline void swap(AdaptiveOffsets& a, AdaptiveOffsets& b) noexcept {
    a.swap(b);
}

} // namespace starrocks
