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
#include <vector>

#include "simdutf.h"
#include "util/slice.h"

namespace starrocks {

// SIZE: 256 * uint8_t
static const uint8_t UTF8_BYTE_LENGTH_TABLE[256] = {
        // start byte of 1-byte utf8 char: 0b0000'0000 ~ 0b0111'1111
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        // continuation byte: 0b1000'0000 ~ 0b1011'1111
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
        // start byte of 2-byte utf8 char: 0b1100'0000 ~ 0b1101'1111
        2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
        // start byte of 3-byte utf8 char: 0b1110'0000 ~ 0b1110'1111
        3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
        // start byte of 4-byte utf8 char: 0b1111'0000 ~ 0b1111'0111
        // invalid utf8 byte: 0b1111'1000~ 0b1111'1111
        4, 4, 4, 4, 4, 4, 4, 4, 1, 1, 1, 1, 1, 1, 1, 1};

/**
 * create index indicate every char start byte, special for chinese use three bytes in utf8
 *
 * E.g:
 * @param str: "hello 你好"
 * @param index: [0(h), 1(e), 2(l), 3(l), 4(o), 5( ), 6(你), 9(好)] -> [0, 1, 2, 3, 4, 5, 6, 9]
 * @return: index.size()
 */
static inline size_t get_utf8_index(const Slice& str, std::vector<size_t>* index) {
    for (int i = 0, char_size = 0; i < str.size; i += char_size) {
        char_size = UTF8_BYTE_LENGTH_TABLE[static_cast<unsigned char>(str.data[i])];
        index->push_back(i);
    }
    return index->size();
}

// called by utf8_substr_from_right to speedup substr computing on small strings.
// utf8_substr_from_left need not invoke this function, it just scan from the leading
// of the string.
// caller guarantee the size of small_index is same to str.size
static inline size_t get_utf8_small_index(const Slice& str, uint8_t* small_index) {
    size_t n = 0;
    for (uint8_t i = 0, char_size = 0; i < str.size; i += char_size) {
        char_size = UTF8_BYTE_LENGTH_TABLE[static_cast<unsigned char>(str.data[i])];
        small_index[n++] = i;
    }
    return n;
}

static inline Slice truncate_utf8(const Slice& str, const size_t max_size) {
    std::vector<size_t> index{};
    const size_t utf8_length = get_utf8_index(str, &index);
    size_t actual_size = 0;
    if (utf8_length > max_size) {
        // do truncate
        actual_size = index[max_size];
    } else {
        // don't need to truncate
        actual_size = str.size;
    }
    return {str.data, actual_size};
}

// table-driven is faster than computing as follow:
/*
 *      uint8_t b = ~static_cast<uint8_t>(*p);
 *      if (b >> 7 == 1) {
 *         char_size = 1;
 *      } else {
 *         char_size = __builtin_clz(b) - 24;
 *      }
 */
template <bool use_skipped_chars>
static inline const char* skip_leading_utf8(const char* p, const char* end, size_t n,
                                            [[maybe_unused]] size_t* skipped_chars) {
    int char_size = 0;
    size_t i = 0;
    for (; i < n && p < end; ++i, p += char_size) {
        char_size = UTF8_BYTE_LENGTH_TABLE[static_cast<uint8_t>(*p)];
    }
    if constexpr (use_skipped_chars) {
        *skipped_chars = i;
    }
    return p;
}

static inline const char* skip_leading_utf8(const char* p, const char* end, size_t n) {
    return skip_leading_utf8<false>(p, end, n, nullptr);
}
// scan from trailing to leading in order to locate n-th utf char
static inline const char* skip_trailing_utf8(const char* p, const char* begin, size_t n) {
    constexpr auto threshold = static_cast<int8_t>(0xBF);
    for (auto i = 0; i < n && p >= begin; ++i) {
        --p;
        while (p >= begin && static_cast<int8_t>(*p) <= threshold) --p;
    }
    return p;
}

static inline int utf8_len(const char* begin, const char* end) {
    return simdutf::count_utf8(begin, end - begin);
}

// Check if the string contains a utf-8 character
static inline bool utf8_contains(const std::string& str, const std::vector<size_t>& utf8_index, Slice utf8_char) {
    for (int i = 0; i < utf8_index.size(); i++) {
        size_t char_idx = utf8_index[i];
        // TODO: optimize the utf8-index, add a length guard at the tail
        size_t char_len = i < utf8_index.size() - 1 ? utf8_index[i + 1] - char_idx : str.length() - char_idx;
        if (memcmp(str.data() + char_idx, utf8_char.data, char_len) == 0) {
            return true;
        }
    }
    return false;
}

// Find the start of utf8 character
// NOTE: it must be a valid utf-8 string
static inline Slice utf8_char_start(const char* end) {
    const char* p = end;
    size_t count = 1;
    for (; (*p & 0xC0) == 0x80; p--, count++) {
    }
    return {p, count};
}

static inline bool validate_ascii_fast(const char* src, size_t len) {
    return simdutf::validate_ascii(src, len);
}

} // namespace starrocks
