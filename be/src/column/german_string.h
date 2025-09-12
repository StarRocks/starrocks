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
        if (lhs_prefix != rhs_prefix) {
            // if prefix not equal, we can determine the result by prefix
            lhs_prefix = byte_swap(lhs_prefix);
            rhs_prefix = byte_swap(rhs_prefix);
            return (lhs_prefix > rhs_prefix) - (lhs_prefix < rhs_prefix);
        }

        auto min_len = std::min<uint32_t>(this->len, rhs.len);
        auto r = memcompare(this->get_data(), min_len, rhs.get_data(), min_len);
        return r != 0 ? r : ((this->len > rhs.len) - (this->len < rhs.len));
    }
    uint32_t fnv_hash(uint32_t seed) const;
    uint32_t crc32_hash(uint32_t seed) const;

    std::string to_string() const { return static_cast<std::string>(*this); }

    const char* get_data() const {
        if (is_inline()) {
            return short_rep.str;
        } else {
            // NOLINTNEXTLINE(performance-no-int-to-ptr)
            return reinterpret_cast<const char*>(long_rep.ptr);
        }
    }

    inline bool operator==(const GermanString& rhs) const {
        // compare first 8 bytes;
        const auto lhs_h0 = *reinterpret_cast<const uint64_t*>(this);
        const auto rhs_h0 = *reinterpret_cast<const uint64_t*>(&rhs);
        if (lhs_h0 != rhs_h0) {
            return false;
        }

        // compare second 8 bytes;
        const auto lhs_h1 = *(reinterpret_cast<const uint64_t*>(this) + 1);
        const auto rhs_h1 = *(reinterpret_cast<const uint64_t*>(&rhs) + 1);
        if (lhs_h1 == rhs_h1) {
            return true;
        }

        return memcompare(get_data(), this->len, rhs.get_data(), rhs.len) == 0;
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
    alignas(8) union {
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
} // namespace starrocks

namespace std {
static inline std::string to_string(const starrocks::GermanString& gs) {
    return static_cast<std::string>(gs);
}

} // namespace std