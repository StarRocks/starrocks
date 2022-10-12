// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cstdint>
#include <iterator>
#include <ostream>

#include "util/memcmp.h"

namespace starrocks {

#pragma pack(1)
struct int96_t {
    uint64_t lo;
    uint32_t hi;

    bool operator==(const int96_t& other) const {
        return memequal((const char*)this, sizeof(int96_t), (const char*)&other, sizeof(int96_t));
    }

    bool operator!=(const int96_t& other) const { return !(*this == other); }

    bool operator<(const int96_t& other) const {
        if (hi < other.hi) {
            return true;
        }
        if (hi > other.hi) {
            return false;
        }
        return lo < other.lo;
    }

    bool operator>(const int96_t& other) const {
        if (hi > other.hi) {
            return true;
        }
        if (hi < other.hi) {
            return false;
        }
        return lo > other.lo;
    }

    std::string to_string() const;
};
#pragma pack()
static_assert(sizeof(int96_t) == 12, "compiler breaks packing rules");

std::ostream& operator<<(std::ostream& os, const int96_t& val);

} // namespace starrocks
