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
size_t GermanStringExternalAllocator::size() const {
    size_t total_size = 0;
    for (const auto& page : medium_string_pages) {
        total_size += page.size();
    }
    for (const auto& str : large_strings) {
        total_size += str.size();
    }
    return total_size;
}

void GermanStringExternalAllocator::clear() {
    large_strings.clear();
    medium_string_pages.clear();
}

char* GermanStringExternalAllocator::allocate(size_t n) {
    if (n <= GermanString::INLINE_MAX_LENGTH) {
        return nullptr;
    }
    n -= GermanString::PREFIX_LENGTH; // reserve 4 bytes for length prefix
    if (n <= MEDIUM_STRING_MAX_SIZE) {
        if (medium_string_pages.empty() || medium_string_pages.back().size() + n > PAGE_SIZE) {
            medium_string_pages.emplace_back();
            medium_string_pages.back().reserve(PAGE_SIZE);
        }
        auto& page = medium_string_pages.back();
        raw::stl_vector_resize_uninitialized(&page, page.size() + n);
        return page.data() + page.size() - n;
    } else {
        large_strings.emplace_back();
        auto& str = large_strings.back();
        raw::make_room(&str, n);
        return large_strings.back().data();
    }
}

GermanString::GermanString() {
    auto* p = reinterpret_cast<char*>(this);
    std::fill(p, p + sizeof(GermanString), 0);
}

GermanString::GermanString(const starrocks::GermanString& rhs) {
    strings::memcpy_inlined(this, &rhs, sizeof(GermanString));
}

GermanString::GermanString(const std::string& str) {
    NOT_SUPPORT();
}

GermanString& GermanString::operator=(const std::string& str) {
    NOT_SUPPORT();
    return *this;
}

GermanString::GermanString(const char* str, size_t len, void* remaining) {
    auto* p = reinterpret_cast<char*>(this);
    std::fill(p, p + sizeof(GermanString), 0);
    this->len = len;
    if (len <= INLINE_MAX_LENGTH) {
        strings::memcpy_inlined(short_rep.str, str, len);
    } else {
        strings::memcpy_inlined(long_rep.prefix, str, PREFIX_LENGTH);
        strings::memcpy_inlined(remaining, str + PREFIX_LENGTH, len - PREFIX_LENGTH);
        long_rep.ptr = reinterpret_cast<uintptr_t>(remaining);
    }
}

GermanString::GermanString(const Slice& slice) {
    NOT_SUPPORT();
}
GermanString& GermanString::operator=(const Slice& slice) {
    NOT_SUPPORT();
    return *this;
}

GermanString::GermanString(const GermanString& rhs, void* remaining) {
    if (rhs.is_inline()) {
        strings::memcpy_inlined(this, &rhs, sizeof(GermanString));
    } else {
        strings::memcpy_inlined(this, &rhs, sizeof(GermanString) - sizeof(uintptr_t));
        const auto* rhs_ptr = reinterpret_cast<const char*>(rhs.long_rep.ptr);
        strings::memcpy_inlined(remaining, rhs_ptr, rhs.len - PREFIX_LENGTH);
        long_rep.ptr = reinterpret_cast<uintptr_t>(remaining);
    }
}

int GermanString::compare(const GermanString& rhs) const {
    if (is_inline() && rhs.is_inline()) {
        return memcompare(short_rep.str, len, rhs.short_rep.str, rhs.len);
    } else if (this->is_inline()) {
        auto min_len = std::min(this->len, PREFIX_LENGTH);
        auto r = memcompare(short_rep.str, min_len, rhs.long_rep.prefix, min_len);
        if (r != 0) {
            return r;
        } else if (min_len <= PREFIX_LENGTH) {
            return -1;
        } else {
            return memcompare(short_rep.str + PREFIX_LENGTH, len - PREFIX_LENGTH,
                              reinterpret_cast<const char*>(rhs.long_rep.ptr), rhs.len - PREFIX_LENGTH);
        }
    } else if (rhs.is_inline()) {
        auto min_len = std::min(rhs.len, PREFIX_LENGTH);
        auto r = memcompare(long_rep.prefix, min_len, rhs.short_rep.str, min_len);
        if (r != 0) {
            return r;
        } else if (min_len <= PREFIX_LENGTH) {
            return 1;
        } else {
            return memcompare(reinterpret_cast<const char*>(long_rep.ptr), len - PREFIX_LENGTH,
                              rhs.short_rep.str + PREFIX_LENGTH, rhs.len - PREFIX_LENGTH);
        }
    } else {
        auto r = memcompare(long_rep.prefix, PREFIX_LENGTH, rhs.long_rep.prefix, PREFIX_LENGTH);
        if (r != 0) {
            return r;
        }
        auto* this_ptr = reinterpret_cast<const char*>(long_rep.ptr);
        auto* rhs_ptr = reinterpret_cast<const char*>(rhs.long_rep.ptr);
        return memcompare(this_ptr, len - PREFIX_LENGTH, rhs_ptr, rhs.len - PREFIX_LENGTH);
    }
}

GermanString::operator std::string() const {
    if (len <= INLINE_MAX_LENGTH) {
        return std::string(short_rep.str, len);
    } else {
        std::string s;
        raw::make_room(&s, len);
        char* data = s.data();
        strings::memcpy_inlined(data, long_rep.prefix, PREFIX_LENGTH);
        strings::memcpy_inlined(data + PREFIX_LENGTH, reinterpret_cast<const char*>(long_rep.ptr), len - PREFIX_LENGTH);
        return s;
    }
}

uint32_t GermanString::fnv_hash(uint32_t seed) const {
    if (is_inline()) {
        return HashUtil::fnv_hash(short_rep.str, len, seed);
    } else {
        uint32_t h = HashUtil::fnv_hash(long_rep.prefix, PREFIX_LENGTH, seed);
        return HashUtil::fnv_hash(reinterpret_cast<const char*>(long_rep.ptr), len - PREFIX_LENGTH, h);
    }
}
uint32_t GermanString::crc32_hash(uint32_t seed) const {
    if (is_inline()) {
        return HashUtil::zlib_crc_hash(short_rep.str, len, seed);
    } else {
        uint32_t h = HashUtil::zlib_crc_hash(long_rep.prefix, PREFIX_LENGTH, seed);
        return HashUtil::zlib_crc_hash(reinterpret_cast<const char*>(long_rep.ptr), len - PREFIX_LENGTH, h);
    }
}

} // namespace starrocks
