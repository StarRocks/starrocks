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

#include <glog/logging.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "runtime/mem_pool.h"
#include "util/memcmp.h"

namespace starrocks {
class Slice;

// A GermanString is a string that can be stored inline or as a pointer to a larger buffer.

class GermanString {
public:
    static constexpr uint32_t INLINE_MAX_LENGTH = 12;
    static constexpr uint32_t PREFIX_LENGTH = 4;
    GermanString();
    GermanString(const char* str, size_t len, void* remaining);
    GermanString(const GermanString& rhs, void* remaining);
    GermanString(const GermanString& rhs);

    GermanString(const Slice& slice);
    GermanString& operator=(const Slice& slice);

    GermanString(const std::string_view& str, void* remaining) : GermanString(str.data(), str.size(), remaining){};
    GermanString(const std::string& str);
    GermanString& operator=(const std::string& str);

    explicit operator std::string() const;

    void append(size_t pos, const void* str, size_t len) {
        DCHECK(pos + len <= this->len);
        if (this->len <= INLINE_MAX_LENGTH) {
            strings::memcpy_inlined(short_rep.str + pos, str, len);
            return;
        }
        if (pos + len <= PREFIX_LENGTH) {
            strings::memcpy_inlined(long_rep.prefix + pos, str, len);
        } else if (pos < PREFIX_LENGTH) {
            strings::memcpy_inlined(long_rep.prefix + pos, str, PREFIX_LENGTH - pos);
            auto* ptr = reinterpret_cast<char*>(long_rep.ptr);
            strings::memcpy_inlined(ptr, static_cast<const char*>(str) + PREFIX_LENGTH - pos,
                                    len - (PREFIX_LENGTH - pos));
        } else {
            auto* ptr = reinterpret_cast<char*>(long_rep.ptr);
            strings::memcpy_inlined(ptr + pos - PREFIX_LENGTH, str, len);
        }
    }
    void read(size_t pos, void* str, size_t len) const {
        DCHECK(pos + len <= this->len);
        if (this->len <= INLINE_MAX_LENGTH) {
            strings::memcpy_inlined(str, short_rep.str + pos, len);
            return;
        }
        if (pos + len <= PREFIX_LENGTH) {
            strings::memcpy_inlined(str, long_rep.prefix + pos, len);
        } else if (pos < PREFIX_LENGTH) {
            strings::memcpy_inlined(str, long_rep.prefix + pos, PREFIX_LENGTH - pos);
            auto* ptr = reinterpret_cast<const char*>(long_rep.ptr);
            strings::memcpy_inlined(static_cast<char*>(str) + PREFIX_LENGTH - pos, ptr, len - (PREFIX_LENGTH - pos));
        } else {
            auto* ptr = reinterpret_cast<const char*>(long_rep.ptr);
            strings::memcpy_inlined(static_cast<char*>(str), ptr + pos - PREFIX_LENGTH, len);
        }
    }

    void resize(void* remaining) { this->long_rep.ptr = reinterpret_cast<uintptr_t>(remaining); }

    void shrink(size_t len) {
        DCHECK(len <= this->len);
        if (this->len == len) {
            return; // no change
        }
        if (this->len > INLINE_MAX_LENGTH && len > PREFIX_LENGTH && len <= INLINE_MAX_LENGTH) {
            auto* ptr = reinterpret_cast<const char*>(long_rep.ptr);
            long_rep.ptr = 0;
            strings::memcpy_inlined(short_rep.str + PREFIX_LENGTH, ptr, len - PREFIX_LENGTH);
        }
        this->len = len;
    }

    void poison() {
        if (this->is_inline()) {
            return;
        } else {
            auto pos = (this->len - PREFIX_LENGTH - 4) & (~3);
            auto* ptr = reinterpret_cast<char*>(long_rep.ptr);
            uint32_t& poison_value = *reinterpret_cast<uint32_t*>(ptr + pos);
            uint32_t& prefix = *reinterpret_cast<uint32_t*>(long_rep.prefix);
            prefix ^= poison_value; // XOR the prefix with the poison value
        }
    }

    void unpoison() { this->poison(); }

    void expand(size_t len) { this->len += len; }

    bool is_inline() const { return len <= INLINE_MAX_LENGTH; }
    int compare(const GermanString& rhs) const;
    uint32_t fnv_hash(uint32_t seed) const;
    uint32_t crc32_hash(uint32_t seed) const;

    std::string to_string() const { return static_cast<std::string>(*this); }
    inline bool operator==(const GermanString& rhs) const {
        if (this->len != rhs.len) {
            return false;
        }
        if (this->is_inline() || rhs.is_inline()) {
            auto p0 = reinterpret_cast<const char*>(this);
            auto p1 = reinterpret_cast<const char*>(&rhs);
            const auto sz = sizeof(GermanString);
            return memcompare(p0, sz, p1, sz) == 0;
        } else {
            return memcompare(this->long_rep.prefix, PREFIX_LENGTH, rhs.long_rep.prefix, PREFIX_LENGTH) == 0 &&
                   memcompare(reinterpret_cast<const char*>(this->long_rep.ptr), this->len - PREFIX_LENGTH,
                              reinterpret_cast<const char*>(rhs.long_rep.ptr), rhs.len - PREFIX_LENGTH) == 0;
        }
    }

    inline bool operator!=(const GermanString& rhs) const { return !(*this == rhs); }
    inline bool operator<(const GermanString& rhs) const { return compare(rhs) < 0; }
    inline bool operator<=(const GermanString& rhs) const { return compare(rhs) <= 0; }
    inline bool operator>(const GermanString& rhs) const { return compare(rhs) > 0; }
    inline bool operator>=(const GermanString& rhs) const { return compare(rhs) >= 0; }
    inline bool operator<=>(const GermanString& rhs) const { return compare(rhs); }

    friend std::ostream& operator<<(std::ostream& os, const GermanString& gs) {
        os << gs.to_string();
        return os;
    }
    union {
        uint32_t len;
        struct {
            uint32_t len;
            char str[INLINE_MAX_LENGTH];
        } short_rep;
        struct {
            uint32_t len;
            char prefix[PREFIX_LENGTH];
            uintptr_t ptr;
        } long_rep;
    };
};

class GermanStringExternalAllocator {
public:
    static constexpr auto PAGE_SIZE = 4096;
    static constexpr auto MEDIUM_STRING_MAX_SIZE = 512;
    size_t size() const;

    void clear();

    char* allocate(size_t n);

private:
    std::vector<std::vector<char>> medium_string_pages;
    std::vector<std::string> large_strings;
};

class GermanStringMemPoolExternalAllocator {
public:
    GermanStringMemPoolExternalAllocator(MemPool* mem_pool) : _mem_pool(mem_pool) {}
    char* allocate(size_t n) {
        if (n <= GermanString::INLINE_MAX_LENGTH) {
            return nullptr;
        }
        n -= GermanString::PREFIX_LENGTH; // reserve 4 bytes for length prefix
        return reinterpret_cast<char*>(_mem_pool->allocate_aligned(n, 4));
    }

private:
    MemPool* _mem_pool;
};

} // namespace starrocks

namespace std {
static inline std::string to_string(const starrocks::GermanString& gs) {
    return static_cast<std::string>(gs);
}

} // namespace std