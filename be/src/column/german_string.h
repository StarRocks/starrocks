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
#include "runtime/memory/column_allocator.h"
#include "util/memcmp.h"
#include "util/raw_container.h"
#include "util/slice.h"

namespace starrocks {
class Slice;

// A GermanString is a string that can be stored inline or as a pointer to a larger buffer.

class GermanString {
public:
    static constexpr uint32_t INLINE_MAX_LENGTH = 12;
    static constexpr uint32_t PREFIX_LENGTH = 4;
    GermanString();
    GermanString(const char* str, size_t len, void* ptr);
    GermanString(const void* str, size_t len);
    GermanString(const GermanString& rhs, void* ptr);
    GermanString(const GermanString& rhs);

    explicit GermanString(const Slice& slice) : GermanString(slice.data, slice.size){};
    GermanString& operator=(const Slice& slice);

    GermanString(const std::string_view& str, void* ptr) : GermanString(str.data(), str.size(), ptr){};
    GermanString(const std::string& str);
    GermanString& operator=(const std::string& str);

    explicit operator std::string() const;

    void append(size_t pos, const void* str, size_t len) {
        DCHECK(pos + len <= this->len);
        if (this->len <= INLINE_MAX_LENGTH) {
            strings::memcpy_inlined(short_rep.str + pos, str, len);
            return;
        }

        auto* ptr = reinterpret_cast<char*>(this->long_rep.ptr);
        strings::memcpy_inlined(ptr + pos, str, len);
        if (pos < PREFIX_LENGTH && pos + len >= PREFIX_LENGTH) {
            strings::memcpy_inlined(long_rep.prefix, ptr, PREFIX_LENGTH);
        }
    }

    void read(size_t pos, void* str, size_t len) const {
        DCHECK(pos + len <= this->len);
        if (this->len <= INLINE_MAX_LENGTH) {
            strings::memcpy_inlined(str, short_rep.str + pos, len);
            return;
        }
        auto* ptr = reinterpret_cast<char*>(this->long_rep.ptr);
        strings::memcpy_inlined(str, ptr + pos, len);
    }

    void resize(void* ptr) { this->long_rep.ptr = reinterpret_cast<uintptr_t>(ptr); }

    void shrink(size_t len) {
        DCHECK(len <= this->len);
        if (this->len == len) {
            return; // no change
        }
        if (this->len > INLINE_MAX_LENGTH && len <= INLINE_MAX_LENGTH) {
            const auto* ptr = reinterpret_cast<const char*>(this->long_rep.ptr);
            std::fill(short_rep.str, short_rep.str + INLINE_MAX_LENGTH, 0);
            strings::memcpy_inlined(short_rep.str, ptr, len);
        } else if (this->len <= INLINE_MAX_LENGTH) {
            std::fill(short_rep.str + len, short_rep.str + INLINE_MAX_LENGTH, 0);
        }
        this->len = len;
    }

    void poison() {
        if (this->is_inline()) {
            return;
        } else {
            auto pos = (this->len - 4) & (~3);
            auto* ptr = reinterpret_cast<char*>(long_rep.ptr);
            uint32_t& poison_value = *reinterpret_cast<uint32_t*>(ptr + pos);
            uint32_t& prefix = *reinterpret_cast<uint32_t*>(long_rep.prefix);
            prefix ^= poison_value; // XOR the prefix with the poison value
            //strings::memcpy_inlined(ptr, &prefix, sizeof(uint32_t));
        }
    }

    void unpoison() { this->poison(); }

    void expand(size_t len) { this->len += len; }

    bool is_inline() const { return len <= INLINE_MAX_LENGTH; }
    int compare(const GermanString& rhs) const {
        auto byte_swap = [](uint32_t v) -> uint32_t {
            uint32_t t1 = (v >> 16u) | (v << 16u);
            uint32_t t2 = t1 & 0x00ff00ff;
            uint32_t t3 = t1 & 0xff00ff00;
            return (t2 << 8u) | (t3 >> 8u);
        };

        auto lhs_prefix = *reinterpret_cast<const uint32_t*>(this->long_rep.prefix);
        auto rhs_prefix = *reinterpret_cast<const uint32_t*>(rhs.long_rep.prefix);
        lhs_prefix = byte_swap(lhs_prefix);
        rhs_prefix = byte_swap(rhs_prefix);

        auto r = lhs_prefix - rhs_prefix;
        if (r != 0) {
            return r;
        }

        auto min_len = std::min<uint32_t>(this->len, rhs.len);
        const auto* lhs_str =
                this->is_inline() ? this->short_rep.str : reinterpret_cast<const char*>(this->long_rep.ptr);
        const auto* rhs_str = rhs.is_inline() ? rhs.short_rep.str : reinterpret_cast<const char*>(rhs.long_rep.ptr);

        r = memcompare(lhs_str, min_len, rhs_str, min_len);
        return r != 0 ? r : (this->len - rhs.len);
    }
    uint32_t fnv_hash(uint32_t seed) const;
    uint32_t crc32_hash(uint32_t seed) const;

    std::string to_string() const { return static_cast<std::string>(*this); }
    inline bool operator==(const GermanString& rhs) const {
        if (this->len != rhs.len) {
            return false;
        }
        const auto lhs_h0 = *reinterpret_cast<const uint64_t*>(this);
        const auto rhs_h0 = *reinterpret_cast<const uint64_t*>(&rhs);
        if (lhs_h0 != rhs_h0) {
            return false;
        }

        const auto lhs_h1 = *(reinterpret_cast<const uint64_t*>(this) + 1);
        const auto rhs_h1 = *(reinterpret_cast<const uint64_t*>(&rhs) + 1);
        if (lhs_h1 == rhs_h1) {
            return true;
        }

        if (!this->is_inline()) {
            return memcompare(reinterpret_cast<const char*>(this->long_rep.ptr), this->len,
                              reinterpret_cast<const char*>(rhs.long_rep.ptr), rhs.len) == 0;
        }

        return false;
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
    virtual ~GermanStringExternalAllocator() = default;
    size_t size() const;

    void clear();

    char* allocate(size_t n);
    virtual void incorporate(std::shared_ptr<GermanStringExternalAllocator>&& other);
    bool is_from_binary() const { return false; }

private:
    std::vector<std::vector<char>> medium_string_pages;
    std::vector<std::string> large_strings;
};

using Bytes = starrocks::raw::RawVectorPad16<uint8_t, ColumnAllocator<uint8_t>>;
class GermanStringBinaryColumnExternalAllocator : public GermanStringExternalAllocator {
public:
    explicit GermanStringBinaryColumnExternalAllocator(Bytes&& bytes) { bytes_list.emplace_back(std::move(bytes)); }
    virtual ~GermanStringBinaryColumnExternalAllocator() = default;
    size_t size() const;
    void clear();
    char* allocate(size_t n);
    void incorporate(std::shared_ptr<GermanStringExternalAllocator>&& other);
    bool is_from_binary() const { return true; }

private:
    std::vector<Bytes> bytes_list;
};

class GermanStringMemPoolExternalAllocator {
public:
    GermanStringMemPoolExternalAllocator(MemPool* mem_pool) : _mem_pool(mem_pool) {}
    char* allocate(size_t n) {
        if (n <= GermanString::INLINE_MAX_LENGTH) {
            return nullptr;
        }
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