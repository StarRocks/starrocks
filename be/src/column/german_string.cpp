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

void GermanStringExternalAllocator::incorporate(std::shared_ptr<GermanStringExternalAllocator>&& other) {
    DCHECK(!other->is_from_binary());
    large_strings.insert(large_strings.end(), std::make_move_iterator(other->large_strings.begin()),
                         std::make_move_iterator(other->large_strings.end()));
    medium_string_pages.insert(medium_string_pages.end(), std::make_move_iterator(other->medium_string_pages.begin()),
                               std::make_move_iterator(other->medium_string_pages.end()));
    other->clear();
}

size_t GermanStringBinaryColumnExternalAllocator::size() const {
    size_t total_size = 0;
    for (auto& bytes : bytes_list) {
        total_size += bytes.size();
    }
    return total_size;
}

void GermanStringBinaryColumnExternalAllocator::clear() {
    NOT_SUPPORT();
}
char* allocate(size_t n) {
    NOT_SUPPORT();
    return nullptr;
}
void GermanStringBinaryColumnExternalAllocator::incorporate(std::shared_ptr<GermanStringExternalAllocator>&& other) {
    DCHECK(other->is_from_binary());
    auto&& binary_other = dynamic_cast<GermanStringBinaryColumnExternalAllocator&&>(*other);
    bytes_list.insert(bytes_list.end(), std::make_move_iterator(binary_other.bytes_list.begin()),
                      std::make_move_iterator(binary_other.bytes_list.end()));
    other->clear();
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

GermanString::GermanString(const char* str, size_t len, void* ptr) {
    auto* p = reinterpret_cast<char*>(this);
    std::fill(p, p + sizeof(GermanString), 0);
    this->len = len;
    if (len <= INLINE_MAX_LENGTH) {
        strings::memcpy_inlined(short_rep.str, str, len);
    } else {
        strings::memcpy_inlined(long_rep.prefix, str, PREFIX_LENGTH);
        strings::memcpy_inlined(ptr, str, len);
        long_rep.ptr = reinterpret_cast<uintptr_t>(ptr);
    }
}

GermanString::GermanString(const void* str, size_t len) {
    auto* p = reinterpret_cast<char*>(this);
    std::fill(p, p + sizeof(GermanString), 0);
    this->len = len;
    if (len <= INLINE_MAX_LENGTH) {
        strings::memcpy_inlined(short_rep.str, str, len);
    } else {
        strings::memcpy_inlined(long_rep.prefix, str, PREFIX_LENGTH);
        long_rep.ptr = reinterpret_cast<uintptr_t>(str);
    }
}
GermanString& GermanString::operator=(const Slice& slice) {
    *this = GermanString(slice);
    return *this;
}

GermanString::GermanString(const GermanString& rhs, void* ptr) {
    strings::memcpy_inlined(this, &rhs, sizeof(GermanString));
    if (!rhs.is_inline()) {
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
        strings::memcpy_inlined(data, reinterpret_cast<const char*>(long_rep.ptr), len);
        return s;
    }
}

uint32_t GermanString::fnv_hash(uint32_t seed) const {
    if (is_inline()) {
        return HashUtil::fnv_hash(short_rep.str, len, seed);
    } else {
        return HashUtil::fnv_hash(reinterpret_cast<const char*>(long_rep.ptr), len, seed);
    }
}
uint32_t GermanString::crc32_hash(uint32_t seed) const {
    if (is_inline()) {
        return HashUtil::zlib_crc_hash(short_rep.str, len, seed);
    } else {
        return HashUtil::zlib_crc_hash(reinterpret_cast<const char*>(long_rep.ptr), len, seed);
    }
}

} // namespace starrocks
