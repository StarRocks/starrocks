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
