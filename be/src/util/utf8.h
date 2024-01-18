// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <cstring>
#include <vector>

#include "util/slice.h"

namespace starrocks::vectorized {

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

// utf8_length
// this SIMD optimization bases upon the brilliant implementation of ClickHouse
// (https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/UTF8Helpers.cpp)
// utf8-encoding:
// - 1-byte: 0xxx_xxxx;
// - 2-byte: 110x_xxxx 10xx_xxxx;
// - 3-byte: 1110_xxxx 10xx_xxxx 10xx_xxxx 10xx_xxxx;
// - 4-byte: 1111_0xxx 10xx_xxxx 10xx_xxxx 10xx_xxxx 10xx_xxxx.
// Counting utf8 chars in a byte string is equivalent to counting first byte of utf chars, that
// is to say, counting bytes which do not match 10xx_xxxx pattern.
// All 0xxx_xxxx, 110x_xxxx, 1110_xxxx and 1111_0xxx are greater than 1011_1111 when use int8_t arithmetic,
// so just count bytes greater than 1011_1111 in a byte string as the result of utf8_length.
inline static int utf8_len(const char* begin, const char* end) {
    int len = 0;
    const char* p = begin;
#if defined(__AVX2__)
    size_t str_size = end - begin;
    constexpr auto bytes_avx2 = sizeof(__m256i);
    // Bytes processed by SIMD operations must be mutiples of bytes_avx2, so:
    // src_end_avx2 = p + str_size / bytes_avx2 * bytes_avx2;
    //
    // use bitwise operations to speed up alignment evaluation when bytes_avx2
    // is power of 2.
    //
    // _mm256_loadu_si256 need not to load bytes from addresses aligning at bytes_avx2
    // boundary.
    const auto src_end_avx2 = p + (str_size & ~(bytes_avx2 - 1));
    const auto threshold = _mm256_set1_epi8(0xBF);
    for (; p < src_end_avx2; p += bytes_avx2)
        len += __builtin_popcount(_mm256_movemask_epi8(
                _mm256_cmpgt_epi8(_mm256_loadu_si256(reinterpret_cast<const __m256i*>(p)), threshold)));
#elif defined(__SSE2__)
    size_t str_size = end - begin;
    constexpr auto bytes_sse2 = sizeof(__m128i);
    const auto src_end_sse2 = p + (str_size & ~(bytes_sse2 - 1));
    const auto threshold = _mm_set1_epi8(0xBF);
    for (; p < src_end_sse2; p += bytes_sse2) {
        len += __builtin_popcount(
                _mm_movemask_epi8(_mm_cmpgt_epi8(_mm_loadu_si128(reinterpret_cast<const __m128i*>(p)), threshold)));
    }
#endif
    // process remaining bytes the number of which not exceed bytes_avx2 at the
    // tail of string, one by one.
    for (; p < end; ++p) {
        len += static_cast<int8_t>(*p) > static_cast<int8_t>(0xBF);
    }
    return len;
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

// Modify from https://github.com/lemire/fastvalidate-utf-8/blob/master/include/simdasciicheck.h
static inline bool validate_ascii_fast(const char* src, size_t len) {
#ifdef __AVX2__
    size_t i = 0;
    __m256i has_error = _mm256_setzero_si256();
    if (len >= 32) {
        for (; i <= len - 32; i += 32) {
            __m256i current_bytes = _mm256_loadu_si256((const __m256i*)(src + i));
            has_error = _mm256_or_si256(has_error, current_bytes);
        }
    }
    int error_mask = _mm256_movemask_epi8(has_error);

    char tail_has_error = 0;
    for (; i < len; i++) {
        tail_has_error |= src[i];
    }
    error_mask |= (tail_has_error & 0x80);

    return !error_mask;
#elif defined(__SSE2__)
    size_t i = 0;
    __m128i has_error = _mm_setzero_si128();
    if (len >= 16) {
        for (; i <= len - 16; i += 16) {
            __m128i current_bytes = _mm_loadu_si128((const __m128i*)(src + i));
            has_error = _mm_or_si128(has_error, current_bytes);
        }
    }
    int error_mask = _mm_movemask_epi8(has_error);

    char tail_has_error = 0;
    for (; i < len; i++) {
        tail_has_error |= src[i];
    }
    error_mask |= (tail_has_error & 0x80);

    return !error_mask;
#else
    char tail_has_error = 0;
    for (size_t i = 0; i < len; i++) {
        tail_has_error |= src[i];
    }
    return !(tail_has_error & 0x80);
#endif
}

} // namespace starrocks::vectorized
