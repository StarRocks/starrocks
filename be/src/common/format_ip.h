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
#include <cstring>

constexpr size_t IPV4_BINARY_LENGTH = 4;
constexpr int IPV4_MAX_OCTET_VALUE = 255; //max value of octet
constexpr size_t IPV4_OCTET_BITS = 8;
constexpr size_t DECIMAL_BASE = 10;

namespace starrocks {

inline bool is_numeric_ascii(char c) {
    // This is faster than
    // return UInt8(UInt8(c) - UInt8('0')) < UInt8(10);
    // on Intel CPUs when compiled by gcc 8.
    return (c >= '0' && c <= '9');
}

inline bool parse_ipv4(const char* pos, size_t str_len, int64_t& dst, int64_t first_octet = -1) {
    if (pos == nullptr || first_octet > IPV4_MAX_OCTET_VALUE) {
        return false;
    }

    int64_t result = 0;
    int offset = (IPV4_BINARY_LENGTH - 1) * IPV4_OCTET_BITS;

    if (first_octet >= 0) {
        result |= first_octet << offset;
        offset -= IPV4_OCTET_BITS;
    }

    if (str_len < 7 || str_len > 15) {
        return false;
    }

    size_t i = 0;
    for (; i < str_len; offset -= IPV4_OCTET_BITS, ++pos, ++i) {
        int64_t value = 0;
        size_t len = 0;

        while (i < str_len && is_numeric_ascii(*pos) && len <= 3) {
            value = value * DECIMAL_BASE + (*pos - '0');
            ++len;
            ++pos;
            ++i;
        }

        if (len == 0 || value > IPV4_MAX_OCTET_VALUE || (offset > 0 && *pos != '.')) {
            return false;
        }

        result |= value << offset;

        if (offset == 0) {
            break;
        }
    }

    while (i < str_len) {
        if (*pos != '\0') {
            return false;
        }
        ++pos;
        ++i;
    }

    dst = result;
    return true;
}

} // namespace starrocks
