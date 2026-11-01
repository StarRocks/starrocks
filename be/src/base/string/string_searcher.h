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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>

#if defined(__x86_64__)
#include <immintrin.h>
#elif defined(__ARM_NEON)
#include <arm_neon.h>
#endif

namespace starrocks {

class StringSearcher {
private:
    /// string to be searched for
    const uint8_t* const needle;
    size_t needle_size;
    /// first, last, and discriminating interior anchor in `needle`
    uint8_t first{};
    uint8_t last{};
    uint8_t mid{};
    uint16_t mid_off{};
    uint16_t alt_off{};

    inline bool _verify_interior(const uint8_t* pos) const {
        if (needle_size <= 2) {
            return true;
        }
        if (pos[mid_off] != mid || pos[alt_off] != needle[alt_off]) {
            return false;
        }
        return std::memcmp(pos + 1, needle + 1, needle_size - 2) == 0;
    }

#if defined(__x86_64__)
    __attribute__((target("sse4.1"))) const uint8_t* _search_sse41_from(const uint8_t* p,
                                                                        const uint8_t* haystack_end) const {
        const size_t last_off = needle_size - 1;
        const __m128i v_first = _mm_set1_epi8(static_cast<char>(first));
        const __m128i v_last = _mm_set1_epi8(static_cast<char>(last));
        const __m128i v_mid = _mm_set1_epi8(static_cast<char>(mid));

        while (static_cast<size_t>(haystack_end - p) >= last_off + 16) {
            const __m128i f0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p));
            const __m128i l0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p + last_off));
            __m128i eq0 = _mm_and_si128(_mm_cmpeq_epi8(f0, v_first), _mm_cmpeq_epi8(l0, v_last));
            uint32_t mask = static_cast<uint32_t>(_mm_movemask_epi8(eq0));
            if (mask != 0 && needle_size > 2) {
                const __m128i m0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p + mid_off));
                eq0 = _mm_and_si128(eq0, _mm_cmpeq_epi8(m0, v_mid));
                mask = static_cast<uint32_t>(_mm_movemask_epi8(eq0));
            }
            while (mask != 0) {
                const uint32_t bit = static_cast<uint32_t>(__builtin_ctz(mask));
                if (_verify_interior(p + bit)) {
                    return p + bit;
                }
                mask &= (mask - 1);
            }
            p += 16;
        }

        if (static_cast<size_t>(haystack_end - p) >= needle_size) {
            const uint8_t* tail_p = haystack_end - (last_off + 16);
            const size_t already_checked = static_cast<size_t>(p - tail_p);
            const __m128i f0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(tail_p));
            const __m128i l0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(tail_p + last_off));
            __m128i eq0 = _mm_and_si128(_mm_cmpeq_epi8(f0, v_first), _mm_cmpeq_epi8(l0, v_last));
            uint32_t mask = static_cast<uint32_t>(_mm_movemask_epi8(eq0)) & (~0U << already_checked);
            if (mask != 0 && needle_size > 2) {
                const __m128i m0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(tail_p + mid_off));
                eq0 = _mm_and_si128(eq0, _mm_cmpeq_epi8(m0, v_mid));
                mask = static_cast<uint32_t>(_mm_movemask_epi8(eq0)) & (~0U << already_checked);
            }
            while (mask != 0) {
                const uint32_t bit = static_cast<uint32_t>(__builtin_ctz(mask));
                if (_verify_interior(tail_p + bit)) {
                    return tail_p + bit;
                }
                mask &= (mask - 1);
            }
        }

        return haystack_end;
    }
#endif

public:
    template <typename CharT, typename = std::enable_if_t<sizeof(CharT) == 1>>
    StringSearcher(const CharT* needle_, const size_t needle_size_)
            : needle{reinterpret_cast<const uint8_t*>(needle_)}, needle_size(needle_size_) {
        if (0 == needle_size) {
            return;
        }

        first = *needle;
        last = needle[needle_size - 1];
        if (needle_size > 2) {
            static constexpr size_t kMaxAnchorScan = 64;
            const size_t max_interior = std::min<size_t>(needle_size - 2, kMaxAnchorScan);
            const size_t center = (1 + max_interior) / 2;
            size_t chosen_mid = center;
            for (size_t delta = 0; delta <= max_interior; ++delta) {
                const size_t right = center + delta;
                if (right <= max_interior && needle[right] != first && needle[right] != last) {
                    chosen_mid = right;
                    break;
                }
                if (delta <= center - 1) {
                    const size_t left = center - delta;
                    if (left >= 1 && needle[left] != first && needle[left] != last) {
                        chosen_mid = left;
                        break;
                    }
                }
            }
            mid_off = static_cast<uint16_t>(chosen_mid);
            mid = needle[mid_off];

            size_t chosen_alt = (chosen_mid > 1) ? 1 : max_interior;
            for (size_t i = max_interior; i >= 1; --i) {
                if (i != chosen_mid && needle[i] != first && needle[i] != last && needle[i] != mid) {
                    chosen_alt = i;
                    break;
                }
            }
            alt_off = static_cast<uint16_t>(chosen_alt);
        }
    }

    template <typename CharT, typename = std::enable_if_t<sizeof(CharT) == 1>>
    inline bool compare(const CharT* haystack, const CharT* haystack_end, const CharT* pos) const {
        if (needle_size == 0) {
            return true;
        }
        if (haystack == nullptr || haystack_end == nullptr || pos == nullptr || pos < haystack || pos > haystack_end ||
            static_cast<size_t>(haystack_end - pos) < needle_size) {
            return false;
        }
        const auto* p = reinterpret_cast<const uint8_t*>(pos);
        if (p[0] != first || p[needle_size - 1] != last) {
            return false;
        }
        return _verify_interior(p);
    }

    template <typename CharT, typename = std::enable_if_t<sizeof(CharT) == 1>>
    inline bool compare_lt_sse4_1_width(const CharT* haystack, const CharT* haystack_end, const CharT* pos) const {
        return compare(haystack, haystack_end, pos);
    }

    template <typename CharT, typename = std::enable_if_t<sizeof(CharT) == 1>>
    inline bool compare_ge_sse4_1_width(const CharT* haystack, const CharT* haystack_end, const CharT* pos) const {
        return compare(haystack, haystack_end, pos);
    }

    const uint8_t* search_scalar(const uint8_t* haystack, const uint8_t* haystack_end) const {
        if (needle_size == 0) {
            return haystack;
        }
        if (haystack == nullptr || haystack_end == nullptr || haystack >= haystack_end ||
            static_cast<size_t>(haystack_end - haystack) < needle_size) {
            return haystack_end;
        }
        if (needle_size == 1) {
            const void* res = std::memchr(haystack, first, static_cast<size_t>(haystack_end - haystack));
            return res != nullptr ? static_cast<const uint8_t*>(res) : haystack_end;
        }

        const size_t last_off = needle_size - 1;
        const uint8_t* p = haystack;
        while (static_cast<size_t>(haystack_end - p) >= needle_size) {
            if (p[0] == first && p[last_off] == last && _verify_interior(p)) {
                return p;
            }
            ++p;
        }
        return haystack_end;
    }

#if defined(__x86_64__)
    __attribute__((target("sse4.1"))) const uint8_t* search_sse41(const uint8_t* haystack,
                                                                  const uint8_t* haystack_end) const {
        if (needle_size <= 1 || haystack == nullptr || haystack_end == nullptr || haystack >= haystack_end ||
            static_cast<size_t>(haystack_end - haystack) < (needle_size - 1) + 16) {
            return search_scalar(haystack, haystack_end);
        }
        return _search_sse41_from(haystack, haystack_end);
    }

    __attribute__((target("avx2"))) const uint8_t* search_avx2(const uint8_t* haystack,
                                                               const uint8_t* haystack_end) const {
        if (needle_size <= 1 || haystack == nullptr || haystack_end == nullptr || haystack >= haystack_end ||
            static_cast<size_t>(haystack_end - haystack) < (needle_size - 1) + 16) {
            return search_scalar(haystack, haystack_end);
        }

        const size_t last_off = needle_size - 1;
        const __m256i v_first256 = _mm256_set1_epi8(static_cast<char>(first));
        const __m256i v_last256 = _mm256_set1_epi8(static_cast<char>(last));
        const __m256i v_mid256 = _mm256_set1_epi8(static_cast<char>(mid));
        const uint8_t* p = haystack;

        while (static_cast<size_t>(haystack_end - p) >= last_off + 32) {
            const __m256i f = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
            const __m256i l = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + last_off));
            __m256i eq = _mm256_and_si256(_mm256_cmpeq_epi8(f, v_first256), _mm256_cmpeq_epi8(l, v_last256));
            uint32_t mask = static_cast<uint32_t>(_mm256_movemask_epi8(eq));
            if (mask != 0 && needle_size > 2) {
                const __m256i m = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + mid_off));
                eq = _mm256_and_si256(eq, _mm256_cmpeq_epi8(m, v_mid256));
                mask = static_cast<uint32_t>(_mm256_movemask_epi8(eq));
            }
            while (mask != 0) {
                const uint32_t bit = static_cast<uint32_t>(__builtin_ctz(mask));
                if (_verify_interior(p + bit)) {
                    return p + bit;
                }
                mask &= (mask - 1);
            }
            p += 32;
        }

        return _search_sse41_from(p, haystack_end);
    }
#elif defined(__ARM_NEON)
    const uint8_t* search_neon(const uint8_t* haystack, const uint8_t* haystack_end) const {
        if (needle_size <= 1 || haystack == nullptr || haystack_end == nullptr || haystack >= haystack_end ||
            static_cast<size_t>(haystack_end - haystack) < (needle_size - 1) + 16) {
            return search_scalar(haystack, haystack_end);
        }

        const size_t last_off = needle_size - 1;
        const uint8x16_t v_first = vdupq_n_u8(first);
        const uint8x16_t v_last = vdupq_n_u8(last);
        const uint8x16_t v_mid = vdupq_n_u8(mid);
        const uint8_t* p = haystack;

        while (static_cast<size_t>(haystack_end - p) >= last_off + 32) {
            const uint8x16_t f0 = vld1q_u8(p);
            const uint8x16_t l0 = vld1q_u8(p + last_off);
            const uint8x16_t f1 = vld1q_u8(p + 16);
            const uint8x16_t l1 = vld1q_u8(p + 16 + last_off);

            uint8x16_t eq0 = vandq_u8(vceqq_u8(f0, v_first), vceqq_u8(l0, v_last));
            uint8x16_t eq1 = vandq_u8(vceqq_u8(f1, v_first), vceqq_u8(l1, v_last));

            if (vmaxvq_u8(vorrq_u8(eq0, eq1)) != 0) {
                if (needle_size > 2) {
                    const uint8x16_t m0 = vld1q_u8(p + mid_off);
                    const uint8x16_t m1 = vld1q_u8(p + 16 + mid_off);
                    eq0 = vandq_u8(eq0, vceqq_u8(m0, v_mid));
                    eq1 = vandq_u8(eq1, vceqq_u8(m1, v_mid));
                }
                uint64_t mask0 = vget_lane_u64(vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(eq0), 4)), 0);
                while (mask0 != 0) {
                    const uint32_t bit = static_cast<uint32_t>(__builtin_ctzll(mask0) >> 2);
                    if (_verify_interior(p + bit)) {
                        return p + bit;
                    }
                    mask0 &= ~(0xFULL << (bit << 2));
                }
                uint64_t mask1 = vget_lane_u64(vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(eq1), 4)), 0);
                while (mask1 != 0) {
                    const uint32_t bit = static_cast<uint32_t>(__builtin_ctzll(mask1) >> 2);
                    if (_verify_interior(p + 16 + bit)) {
                        return p + 16 + bit;
                    }
                    mask1 &= ~(0xFULL << (bit << 2));
                }
            }
            p += 32;
        }

        while (static_cast<size_t>(haystack_end - p) >= last_off + 16) {
            const uint8x16_t f0 = vld1q_u8(p);
            const uint8x16_t l0 = vld1q_u8(p + last_off);
            uint8x16_t eq0 = vandq_u8(vceqq_u8(f0, v_first), vceqq_u8(l0, v_last));
            if (vmaxvq_u8(eq0) != 0) {
                if (needle_size > 2) {
                    const uint8x16_t m0 = vld1q_u8(p + mid_off);
                    eq0 = vandq_u8(eq0, vceqq_u8(m0, v_mid));
                }
                uint64_t mask0 = vget_lane_u64(vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(eq0), 4)), 0);
                while (mask0 != 0) {
                    const uint32_t bit = static_cast<uint32_t>(__builtin_ctzll(mask0) >> 2);
                    if (_verify_interior(p + bit)) {
                        return p + bit;
                    }
                    mask0 &= ~(0xFULL << (bit << 2));
                }
            }
            p += 16;
        }

        if (static_cast<size_t>(haystack_end - p) >= needle_size) {
            const uint8_t* tail_p = haystack_end - (last_off + 16);
            const size_t already_checked = static_cast<size_t>(p - tail_p);
            const uint8x16_t f0 = vld1q_u8(tail_p);
            const uint8x16_t l0 = vld1q_u8(tail_p + last_off);
            uint8x16_t eq0 = vandq_u8(vceqq_u8(f0, v_first), vceqq_u8(l0, v_last));
            if (vmaxvq_u8(eq0) != 0) {
                if (needle_size > 2) {
                    const uint8x16_t m0 = vld1q_u8(tail_p + mid_off);
                    eq0 = vandq_u8(eq0, vceqq_u8(m0, v_mid));
                }
                uint64_t mask64 = vget_lane_u64(vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(eq0), 4)), 0);
                mask64 &= (~0ULL << (already_checked << 2));
                while (mask64 != 0) {
                    const uint32_t bit = static_cast<uint32_t>(__builtin_ctzll(mask64) >> 2);
                    if (_verify_interior(tail_p + bit)) {
                        return tail_p + bit;
                    }
                    mask64 &= ~(0xFULL << (bit << 2));
                }
            }
        }

        return haystack_end;
    }
#endif

    template <typename CharT, typename = std::enable_if_t<sizeof(CharT) == 1>>
    const CharT* search(const CharT* haystack, const CharT* const haystack_end) const {
        const auto* h = reinterpret_cast<const uint8_t*>(haystack);
        const auto* h_end = reinterpret_cast<const uint8_t*>(haystack_end);
#if defined(__AVX2__)
        return reinterpret_cast<const CharT*>(search_avx2(h, h_end));
#elif defined(__SSE4_1__)
        return reinterpret_cast<const CharT*>(search_sse41(h, h_end));
#elif defined(__ARM_NEON)
        return reinterpret_cast<const CharT*>(search_neon(h, h_end));
#else
        return reinterpret_cast<const CharT*>(search_scalar(h, h_end));
#endif
    }

    template <typename CharT, typename = std::enable_if_t<sizeof(CharT) == 1>>
    const CharT* search(const CharT* haystack, const size_t haystack_size) const {
        return search(haystack, haystack + haystack_size);
    }
};

/** Uses functions from libc.
  * It makes sense to use only with short haystacks when cheap initialization is required.
  */

struct LibcASCIICaseSensitiveStringSearcher {
    const char* const needle;
    const size_t needle_size;

    template <typename CharT, typename = std::enable_if_t<sizeof(CharT) == 1>>
    LibcASCIICaseSensitiveStringSearcher(const CharT* const needle_, const size_t needle_size_)
            : needle(reinterpret_cast<const char*>(needle_)), needle_size(needle_size_) {}

    template <typename CharT, typename = std::enable_if_t<sizeof(CharT) == 1>>
    const CharT* search(const CharT* haystack, size_t haystack_len) const {
        return reinterpret_cast<const CharT*>(memmem(haystack, haystack_len, needle, needle_size));
    }
};

} // namespace starrocks
