#pragma once

#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>

#ifdef __SSE4_1__
#include <smmintrin.h>
#elif defined(__aarch64__)
#include "avx2ki.h"
#endif

namespace starrocks {

/** Variants for searching a substring in a string.
  * In most cases, performance is less than Volnitsky (see Volnitsky.h).
  */
struct StringSearcherBase {
#if defined(__SSE4_1__) || defined(__aarch64__)
    static constexpr size_t SSE2_WIDTH = sizeof(__m128i);

    const int page_size = getpagesize();

    bool pageSafe(const void* const ptr) const {
        return ((page_size - 1) & reinterpret_cast<std::uintptr_t>(ptr)) <= page_size - SSE2_WIDTH;
    }
#else
    static constexpr size_t SSE2_WIDTH = sizeof(__int128_t);
#endif
};

class StringSearcher : private StringSearcherBase {
private:
    /// string to be searched for
    const uint8_t* const needle;
    const uint8_t* const needle_end;
    size_t needle_size;
    /// first character in `needle`
    uint8_t first{};

#if defined(__SSE4_1__) || defined(__aarch64__)
    /// vector filled `first` for determining leftmost position of the first symbol
    __m128i pattern;
    /// vector of first 16 characters of `needle`
    __m128i cache = _mm_setzero_si128();
    int cachemask{};
#endif

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

#if defined(__SSE4_1__) || defined(__aarch64__)
        pattern = _mm_set1_epi8(first);

        const uint8_t* needle_pos = needle;

        for (int i = 0; i < SSE2_WIDTH; ++i) {
            cache = _mm_srli_si128(cache, 1);

            if (needle_pos != needle_end) {
                cache = _mm_insert_epi8(cache, *needle_pos, SSE2_WIDTH - 1);
                cachemask |= 1 << i;
                ++needle_pos;
            }
        }
#endif
    }

    template <typename CharT, typename = std::enable_if_t<sizeof(CharT) == 1>>
    inline bool compare(const CharT* haystack, const CharT* haystack_end, const CharT* pos) const {
        if (needle_size < SSE2_WIDTH) {
            return compare_lt_sse4_1_width(haystack, haystack_end, pos);
        } else {
            return compare_ge_sse4_1_width(haystack, haystack_end, pos);
        }
    }

    template <typename CharT, typename = std::enable_if_t<sizeof(CharT) == 1>>
    inline bool compare_lt_sse4_1_width(const CharT* haystack, const CharT* haystack_end, const CharT* pos) const {
#if defined(__SSE4_1__) || defined(__aarch64__)
        if (pageSafe(pos)) {
            const auto v_haystack = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pos));
            const auto v_against_cache = _mm_cmpeq_epi8(v_haystack, cache);
            const auto mask = _mm_movemask_epi8(v_against_cache);

            if (0xffff == cachemask) {
                if (mask == cachemask) {
                    pos += SSE2_WIDTH;
                    const uint8_t* needle_pos = needle + SSE2_WIDTH;

                    while (needle_pos < needle_end && *pos == *needle_pos) {
                        ++pos, ++needle_pos;
                    }

                    if (needle_pos == needle_end) {
                        return true;
                    }
                }
            } else if ((mask & cachemask) == cachemask) {
                return true;
            }

            return false;
        }
#endif

        if (*pos == first) {
            ++pos;
            const uint8_t* needle_pos = needle + 1;

            while (needle_pos < needle_end && *pos == *needle_pos) {
                ++pos, ++needle_pos;
            }

            if (needle_pos == needle_end) {
                return true;
            }
        }

        return false;
    }

    template <typename CharT, typename = std::enable_if_t<sizeof(CharT) == 1>>
    inline bool compare_ge_sse4_1_width(const CharT* haystack, const CharT* haystack_end, const CharT* pos) const {
        const uint8_t* needle_pos = needle;
#if defined(__SSE4_1__) || defined(__aarch64__)
        size_t quotient = needle_size / SSE2_WIDTH;
        for (int i = 0; i < quotient; ++i) {
            const auto v_haystack = _mm_loadu_si128(reinterpret_cast<const __m128i*>(pos));
            const auto v_needle = _mm_loadu_si128(reinterpret_cast<const __m128i*>(needle_pos));
            const auto v_against_cache = _mm_cmpeq_epi8(v_haystack, v_needle);
            const auto mask = _mm_movemask_epi8(v_against_cache);
            if (0xffff != mask) {
                return false;
            } else {
                pos += SSE2_WIDTH;
                needle_pos += SSE2_WIDTH;
                continue;
            }
        }
#endif
        while (needle_pos < needle_end && *pos == *needle_pos) {
            ++pos, ++needle_pos;
        }
        if (needle_pos == needle_end) {
            return true;
        }
        return false;
    }

    template <typename CharT, typename = std::enable_if_t<sizeof(CharT) == 1>>
    const CharT* search(const CharT* haystack, const CharT* const haystack_end) const {
        if (needle == needle_end) return haystack;

        while (haystack < haystack_end) {
#if defined(__SSE4_1__) || defined(__aarch64__)
            if (haystack + SSE2_WIDTH <= haystack_end && pageSafe(haystack)) {
                /// find first character
                const auto v_haystack = _mm_loadu_si128(reinterpret_cast<const __m128i*>(haystack));
                const auto v_against_pattern = _mm_cmpeq_epi8(v_haystack, pattern);

                const auto mask = _mm_movemask_epi8(v_against_pattern);

                /// first character not present in 16 octets starting at `haystack`
                if (mask == 0) {
                    haystack += SSE2_WIDTH;
                    continue;
                }

                const size_t offset = __builtin_ctz(mask);
                haystack += offset;

                if (haystack + SSE2_WIDTH <= haystack_end && pageSafe(haystack)) {
                    /// check for first 16 octets
                    const auto v_haystack_offset = _mm_loadu_si128(reinterpret_cast<const __m128i*>(haystack));
                    const auto v_against_cache = _mm_cmpeq_epi8(v_haystack_offset, cache);
                    const auto mask_offset = _mm_movemask_epi8(v_against_cache);

                    if (0xffff == cachemask) {
                        if (mask_offset == cachemask) {
                            const uint8_t* haystack_pos = haystack + SSE2_WIDTH;
                            const uint8_t* needle_pos = needle + SSE2_WIDTH;

                            while (haystack_pos < haystack_end && needle_pos < needle_end &&
                                   *haystack_pos == *needle_pos) {
                                ++haystack_pos, ++needle_pos;
                            }

                            if (needle_pos == needle_end) {
                                return haystack;
                            }
                        }
                    } else if ((mask_offset & cachemask) == cachemask) {
                        return haystack;
                    }

                    ++haystack;
                    continue;
                }
            }
#endif
            if (haystack == haystack_end) {
                return haystack_end;
            }

            if (*haystack == first) {
                const uint8_t* haystack_pos = haystack + 1;
                const uint8_t* needle_pos = needle + 1;

                while (haystack_pos < haystack_end && needle_pos < needle_end && *haystack_pos == *needle_pos) {
                    ++haystack_pos, ++needle_pos;
                }

                if (needle_pos == needle_end) {
                    return haystack;
                }
            }
            ++haystack;
        }

        return haystack_end;
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
