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

#include "column/german_string.h"

#include "gutil/strings/fastmem.h"
#include "util/hash_util.hpp"
#include "util/misc.h"
#include "util/raw_container.h"
#include "util/slice.h"

namespace starrocks {

GermanString::GermanString() {
    auto* p = reinterpret_cast<char*>(this);
    std::fill(p, p + sizeof(GermanString), 0);
}

GermanString::GermanString(const starrocks::GermanString& rhs) {
    strings::memcpy_inlined(this, &rhs, sizeof(GermanString));
}

GermanString::GermanString(const char* str, size_t len, void* ptr) {
    if (len <= INLINE_MAX_LENGTH) {
        auto* p = reinterpret_cast<char*>(this);
        std::fill(p, p + sizeof(GermanString), 0);
        strings::memcpy_inlined(short_rep.str, str, len);
    } else {
        strings::memcpy_inlined(long_rep.prefix, str, PREFIX_LENGTH);
        strings::memcpy_inlined(ptr, str, len);
        long_rep.ptr = reinterpret_cast<uintptr_t>(ptr);
    }
    this->len = len;
}

GermanString::GermanString(const void* str, size_t len) {
    if (len <= INLINE_MAX_LENGTH) {
        auto* p = reinterpret_cast<char*>(this);
        std::fill(p, p + sizeof(GermanString), 0);
        strings::memcpy_inlined(short_rep.str, str, len);
    } else {
        strings::memcpy_inlined(long_rep.prefix, str, PREFIX_LENGTH);
        long_rep.ptr = reinterpret_cast<uintptr_t>(str);
    }
    this->len = len;
}
GermanString& GermanString::operator=(const Slice& slice) {
    *this = GermanString(slice);
    return *this;
}

GermanString::GermanString(const GermanString& rhs, void* ptr) {
    strings::memcpy_inlined(this, &rhs, sizeof(GermanString));
    if (!rhs.is_inline()) {
        // NOLINTNEXTLINE(performance-no-int-to-ptr)
        const auto* rhs_ptr = reinterpret_cast<const char*>(rhs.long_rep.ptr);
        strings::memcpy_inlined(ptr, rhs_ptr, rhs.len);
        long_rep.ptr = reinterpret_cast<uintptr_t>(ptr);
    }
}

GermanString::operator std::string() const {
    if (len <= INLINE_MAX_LENGTH) {
        return std::string(short_rep.str, len);
    } else {
        std::string s;
        raw::make_room(&s, len);
        char* data = s.data();
        // NOLINTNEXTLINE(performance-no-int-to-ptr)
        strings::memcpy_inlined(data, reinterpret_cast<const char*>(long_rep.ptr), len);
        return s;
    }
}

uint32_t GermanString::fnv_hash(uint32_t seed) const {
    if (is_inline()) {
        return HashUtil::fnv_hash(short_rep.str, len, seed);
    } else {
        // // NOLINTNEXTLINE(performance-no-int-to-ptr)
        return HashUtil::fnv_hash(reinterpret_cast<const char*>(long_rep.ptr), len, seed);
    }
}
uint32_t GermanString::crc32_hash(uint32_t seed) const {
    if (is_inline()) {
        return HashUtil::zlib_crc_hash(short_rep.str, len, seed);
    } else {
        // NOLINTNEXTLINE(performance-no-int-to-ptr)
        return HashUtil::zlib_crc_hash(reinterpret_cast<const char*>(long_rep.ptr), len, seed);
    }
}

} // namespace starrocks
