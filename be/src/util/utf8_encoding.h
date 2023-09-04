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

#include "column/column_hash.h"
#include "util/slice.h"
#include "util/utf8.h"

namespace starrocks {

struct EncodedUtf8Char {
    static constexpr char ABSENT = 0xff;
    static constexpr uint32_t ABSENT_ENCODED = 0xffff'ffffU;

    union U {
        struct {
            char bytes[4];
        } __attribute__((packed));
        uint32_t value;
    } u;
    static_assert(sizeof(u) == sizeof(u.value));

    EncodedUtf8Char() { u.value = ABSENT_ENCODED; }
    explicit EncodedUtf8Char(const char* p, size_t size) {
        u.value = ABSENT_ENCODED;
        size_t copy_size = size < sizeof(u.bytes) ? size : sizeof(u.bytes);
        strings::memcpy_inlined(u.bytes, p, copy_size);
    }
    EncodedUtf8Char(const EncodedUtf8Char& x) { u.value = x.u.value; }
    EncodedUtf8Char(EncodedUtf8Char&& x) noexcept { u.value = x.u.value; }

    EncodedUtf8Char& operator=(const EncodedUtf8Char& rhs) {
        u.value = rhs.u.value;
        return *this;
    }
    bool operator==(const EncodedUtf8Char& rhs) const { return u.value == rhs.u.value; }

    operator Slice() const {
        for (int i = 3; i >= 0; i--) {
            if (u.bytes[i] != ABSENT) {
                return {u.bytes, static_cast<size_t>(i + 1)};
            }
        }
        return {u.bytes, 0};
    }

    bool is_empty() const { return u.value == ABSENT_ENCODED; }
};

class EncodedUtf8CharHash {
public:
    std::size_t operator()(const EncodedUtf8Char& s) const { return std::hash<uint32_t>()(s.u.value); }
};

static inline size_t encode_utf8_chars(const Slice& str, std::vector<EncodedUtf8Char>* encoded_values) {
    for (int i = 0, char_size = 0; i < str.size; i += char_size) {
        char_size = UTF8_BYTE_LENGTH_TABLE[static_cast<unsigned char>(str.data[i])];
        encoded_values->emplace_back(str.data + i, char_size);
    }
    return encoded_values->size();
}

} // namespace starrocks