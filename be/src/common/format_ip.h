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

#include <cstring>
#include <cstdint>


constexpr size_t IPV4_BINARY_LENGTH = 4;
constexpr int IPV4_MAX_OCTET_VALUE = 255;         //max value of octet
constexpr size_t IPV4_OCTET_BITS = 8;
constexpr size_t DECIMAL_BASE = 10;

namespace starrocks{

enum class IPConvertExceptionMode : uint8_t {
    Throw, Default, Null
};

inline bool is_numeric_ascii(char c) {
    /// This is faster than
    /// return UInt8(UInt8(c) - UInt8('0')) < UInt8(10);
    /// on Intel CPUs when compiled by gcc 8.
    return (c>='0' && c<='9');
}

template<typename T, typename EOFfunction>
requires(std::is_same<typename std::remove_cv<T>::type, char>::value)
inline bool parse_ipv4(T * &src, EOFfunction eof, unsigned char* dst, int64_t first_octet = -1) {
    if (src == nullptr || first_octet > IPV4_MAX_OCTET_VALUE) {
        return false;
    }

    int64_t result = 0;
    int offset = (IPV4_BINARY_LENGTH - 1) * IPV4_OCTET_BITS;
    if (first_octet >= 0) {
        result |= first_octet << offset;
        offset -= IPV4_OCTET_BITS;
    }

    for (; true; offset -= IPV4_OCTET_BITS, ++src) {
        if(eof()){
            return false;
        }

        int64_t value =0;
        size_t len = 0;
        while (is_numeric_ascii(*src) && len <= 3) {
            value = value * DECIMAL_BASE + (*src - '0');
            ++len;
            ++src;
            if (eof()) {
                break;
            }
        }
        if (len == 0 || value > IPV4_MAX_OCTET_VALUE || (offset > 0 && (eof() || *src != '.'))) {
            return false;
        }
        result |= value << offset;
        if (offset == 0) {
            break;
        }
    }
    memcpy(dst, &result, sizeof(result));
    return true;
}

inline const char* parse_ipv4(const char* src, const char* end, unsigned char* dst) {
    if (parse_ipv4(src, [&src,end]() { return src == end; }, dst)) {
        return src;
    }
    return nullptr;
}

//return true if whole buffer was parsed successfully
inline bool parse_ipv4_whole(const char* src, const char* end, unsigned char* dst) {
    return parse_ipv4(src, end, dst) == end;
}

inline const char* parse_ipv4(const char* src, unsigned char* dst) {
    if (parse_ipv4(src, []() { return false; }, dst)) {
        return src;
    }
    return nullptr;
}

inline bool parse_ipv4_whole(const char* src, unsigned char* dst) {
    const char* end = parse_ipv4(src, dst);
    return end != nullptr && *end == '\0';
}
}// namespace starrocks