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

struct StringSearcherBase {
    static constexpr size_t SSE2_WIDTH = 16;
};

class StringSearcher : private StringSearcherBase {
public:
    enum class Kernel : uint8_t {
        kScalar = 0,
        kSse41 = 1,
        kAvx2 = 2,
        kNeon = 3,
    };

private:
    /// string to be searched for
    const uint8_t* const needle;
    [[maybe_unused]] const uint8_t* const needle_end;
    size_t needle_size;
    /// first and last character in `needle`
    uint8_t first{};
    uint8_t last{};
    Kernel kernel{Kernel::kScalar};

    inline bool _verify_interior(const uint8_t* pos) const {
        if (needle_size <= 2) {
            return true;
        }
        return std::memcmp(pos + 1, needle + 1, needle_size - 2) == 0;
    }

public:
    template <typename CharT, typename = std::enable_if_t<sizeof(CharT) == 1>>
    StringSearcher(const CharT* needle_, const size_t needle_size_)
            : needle{reinterpret_cast<const uint8_t*>(needle_)},
              needle_end{needle + needle_size_},
              needle_size(needle_size_) {
        if (0 == needle_size) {
            return;
        }

        first = *needle;
        last = needle[needle_size - 1];

#if defined(__x86_64__)
        if (__builtin_cpu_supports("avx2")) {
            kernel = Kernel::kAvx2;
        } else if (__builtin_cpu_supports("sse4.1")) {
            kernel = Kernel::kSse41;
        }
#elif defined(__ARM_NEON)
        kernel = Kernel::kNeon;
#endif
    }

    template <typename CharT, typename = std::enable_if_t<sizeof(CharT) == 1>>
    inline bool compare(const CharT* haystack, const CharT* haystack_end, const CharT* pos) const {
        if (needle_size == 0) {
            return true;
        }
        if (pos < haystack || static_cast<size_t>(haystack_end - pos) < needle_size) {
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
        if (haystack >= haystack_end || static_cast<size_t>(haystack_end - haystack) < needle_size) {
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
        if (needle_size == 0) {
            return haystack;
        }
        if (haystack >= haystack_end) {
            return haystack_end;
        }
        const size_t haystack_size = static_cast<size_t>(haystack_end - haystack);
        if (haystack_size < needle_size) {
            return haystack_end;
        }
        if (needle_size == 1) {
            const void* res = std::memchr(haystack, first, haystack_size);
            return res != nullptr ? static_cast<const uint8_t*>(res) : haystack_end;
        }

        const size_t last_off = needle_size - 1;
        if (haystack_size < last_off + 16) {
            return search_scalar(haystack, haystack_end);
        }

        const __m128i v_first = _mm_set1_epi8(static_cast<char>(first));
        const __m128i v_last = _mm_set1_epi8(static_cast<char>(last));
        const uint8_t* p = haystack;

        while (static_cast<size_t>(haystack_end - p) >= last_off + 32) {
            const __m128i f0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p));
            const __m128i l0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p + last_off));
            const __m128i f1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p + 16));
            const __m128i l1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p + 16 + last_off));

            const __m128i eq0 = _mm_and_si128(_mm_cmpeq_epi8(f0, v_first), _mm_cmpeq_epi8(l0, v_last));
            const __m128i eq1 = _mm_and_si128(_mm_cmpeq_epi8(f1, v_first), _mm_cmpeq_epi8(l1, v_last));

            uint32_t mask = static_cast<uint32_t>(_mm_movemask_epi8(eq0)) |
                            (static_cast<uint32_t>(_mm_movemask_epi8(eq1)) << 16);
            while (mask != 0) {
                const uint32_t bit = static_cast<uint32_t>(__builtin_ctz(mask));
                if (_verify_interior(p + bit)) {
                    return p + bit;
                }
                mask &= (mask - 1);
            }
            p += 32;
        }

        while (static_cast<size_t>(haystack_end - p) >= last_off + 16) {
            const __m128i f0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p));
            const __m128i l0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p + last_off));
            const __m128i eq0 = _mm_and_si128(_mm_cmpeq_epi8(f0, v_first), _mm_cmpeq_epi8(l0, v_last));
            uint32_t mask = static_cast<uint32_t>(_mm_movemask_epi8(eq0));
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
            const __m128i eq0 = _mm_and_si128(_mm_cmpeq_epi8(f0, v_first), _mm_cmpeq_epi8(l0, v_last));
            uint32_t mask = static_cast<uint32_t>(_mm_movemask_epi8(eq0)) & (~0U << already_checked);
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

    __attribute__((target("avx2"))) const uint8_t* search_avx2(const uint8_t* haystack,
                                                               const uint8_t* haystack_end) const {
        if (needle_size == 0) {
            return haystack;
        }
        if (haystack >= haystack_end) {
            return haystack_end;
        }
        const size_t haystack_size = static_cast<size_t>(haystack_end - haystack);
        if (haystack_size < needle_size) {
            return haystack_end;
        }
        if (needle_size == 1) {
            const void* res = std::memchr(haystack, first, haystack_size);
            return res != nullptr ? static_cast<const uint8_t*>(res) : haystack_end;
        }

        const size_t last_off = needle_size - 1;
        if (haystack_size < last_off + 16) {
            return search_scalar(haystack, haystack_end);
        }

        const __m256i v_first256 = _mm256_set1_epi8(static_cast<char>(first));
        const __m256i v_last256 = _mm256_set1_epi8(static_cast<char>(last));
        const uint8_t* p = haystack;

        while (static_cast<size_t>(haystack_end - p) >= last_off + 32) {
            const __m256i f = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
            const __m256i l = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + last_off));
            const __m256i eq = _mm256_and_si256(_mm256_cmpeq_epi8(f, v_first256), _mm256_cmpeq_epi8(l, v_last256));
            uint32_t mask = static_cast<uint32_t>(_mm256_movemask_epi8(eq));
            while (mask != 0) {
                const uint32_t bit = static_cast<uint32_t>(__builtin_ctz(mask));
                if (_verify_interior(p + bit)) {
                    return p + bit;
                }
                mask &= (mask - 1);
            }
            p += 32;
        }

        const __m128i v_first128 = _mm256_castsi256_si128(v_first256);
        const __m128i v_last128 = _mm256_castsi256_si128(v_last256);

        while (static_cast<size_t>(haystack_end - p) >= last_off + 16) {
            const __m128i f0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p));
            const __m128i l0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p + last_off));
            const __m128i eq0 = _mm_and_si128(_mm_cmpeq_epi8(f0, v_first128), _mm_cmpeq_epi8(l0, v_last128));
            uint32_t mask = static_cast<uint32_t>(_mm_movemask_epi8(eq0));
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
            const __m128i eq0 = _mm_and_si128(_mm_cmpeq_epi8(f0, v_first128), _mm_cmpeq_epi8(l0, v_last128));
            uint32_t mask = static_cast<uint32_t>(_mm_movemask_epi8(eq0)) & (~0U << already_checked);
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
#elif defined(__ARM_NEON)
    const uint8_t* search_neon(const uint8_t* haystack, const uint8_t* haystack_end) const {
        if (needle_size == 0) {
            return haystack;
        }
        if (haystack >= haystack_end) {
            return haystack_end;
        }
        const size_t haystack_size = static_cast<size_t>(haystack_end - haystack);
        if (haystack_size < needle_size) {
            return haystack_end;
        }
        if (needle_size == 1) {
            const void* res = std::memchr(haystack, first, haystack_size);
            return res != nullptr ? static_cast<const uint8_t*>(res) : haystack_end;
        }

        const size_t last_off = needle_size - 1;
        if (haystack_size < last_off + 16) {
            return search_scalar(haystack, haystack_end);
        }

        const uint8x16_t v_first = vdupq_n_u8(first);
        const uint8x16_t v_last = vdupq_n_u8(last);
        const uint8_t* p = haystack;

        while (static_cast<size_t>(haystack_end - p) >= last_off + 32) {
            const uint8x16_t f0 = vld1q_u8(p);
            const uint8x16_t l0 = vld1q_u8(p + last_off);
            const uint8x16_t f1 = vld1q_u8(p + 16);
            const uint8x16_t l1 = vld1q_u8(p + 16 + last_off);

            const uint8x16_t eq0 = vandq_u8(vceqq_u8(f0, v_first), vceqq_u8(l0, v_last));
            const uint8x16_t eq1 = vandq_u8(vceqq_u8(f1, v_first), vceqq_u8(l1, v_last));

            if (vmaxvq_u8(vorrq_u8(eq0, eq1)) != 0) {
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
            const uint8x16_t eq0 = vandq_u8(vceqq_u8(f0, v_first), vceqq_u8(l0, v_last));
            if (vmaxvq_u8(eq0) != 0) {
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
            const uint8x16_t eq0 = vandq_u8(vceqq_u8(f0, v_first), vceqq_u8(l0, v_last));
            if (vmaxvq_u8(eq0) != 0) {
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
        if (needle_size == 0) {
            return haystack;
        }
        if (haystack >= haystack_end) {
            return haystack_end;
        }
        const size_t haystack_size = static_cast<size_t>(haystack_end - haystack);
        if (haystack_size < needle_size) {
            return haystack_end;
        }
        const auto* h = reinterpret_cast<const uint8_t*>(haystack);
        const auto* h_end = reinterpret_cast<const uint8_t*>(haystack_end);
        if (needle_size == 1) {
            const void* res = std::memchr(h, first, haystack_size);
            return res != nullptr ? reinterpret_cast<const CharT*>(res) : haystack_end;
        }

        switch (kernel) {
#if defined(__x86_64__)
        case Kernel::kAvx2:
            return reinterpret_cast<const CharT*>(search_avx2(h, h_end));
        case Kernel::kSse41:
            return reinterpret_cast<const CharT*>(search_sse41(h, h_end));
#elif defined(__ARM_NEON)
        case Kernel::kNeon:
            return reinterpret_cast<const CharT*>(search_neon(h, h_end));
#endif
        case Kernel::kScalar:
        default:
            return reinterpret_cast<const CharT*>(search_scalar(h, h_end));
        }
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
