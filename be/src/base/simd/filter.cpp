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

#include "base/simd/filter.h"

#include <cstring>

#include "base/simd/multi_version.h"

#if defined(__x86_64__)
#include <immintrin.h>
#endif

#if defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>

#include "base/simd/simd.h"
#endif

namespace SIMD::Filter {
namespace {

// AVX-512 vpcompressq (8-byte lanes) is a fixed four-group cost per batch and,
// on down-clocking Intel parts, loses to a plain scalar copy once only a few
// lanes survive. Below this many set bits in a batch the 64-bit path falls back
// to scalar. (The 32-bit path packs 16 lanes/group, so its fixed cost is lower
// and it always takes the vectorised path.)
constexpr int kCompressMinBits = 6;

// Trivially-copyable stand-in for element widths without a native integer type
// (3/12/16/32-byte: uint24 / int96 / decimal12 / int128 / DecimalV2 / int256).
// Copying one compiles to a fixed-size move, not a runtime-sized memcpy.
template <size_t N>
struct Bytes {
    char data[N];
};

// Scalar reference. `Elem` carries the width as a compile-time constant so the
// per-element copy lowers to a fixed-size move.
template <typename Elem>
size_t scan_scalar(Elem* dst, const Elem* src, const uint8_t* selector, size_t from, size_t to) {
    size_t result = from;
    for (size_t i = from; i < to; ++i) {
        if (selector[i]) dst[result++] = src[i];
    }
    return result;
}

// Last-resort runtime-width scalar copy for any width not enumerated below (no
// real column type hits this).
size_t scan_scalar_bytes(void* dst, const void* src, size_t esz, const uint8_t* selector, size_t from, size_t to) {
    auto* d = reinterpret_cast<uint8_t*>(dst);
    const auto* s = reinterpret_cast<const uint8_t*>(src);
    size_t result = from;
    for (size_t i = from; i < to; ++i) {
        if (selector[i]) {
            if (result != i) std::memcpy(d + result * esz, s + i * esz, esz);
            ++result;
        }
    }
    return result;
}

#define FILTER_CALL(SCAN, T) SCAN(reinterpret_cast<T*>(dst), reinterpret_cast<const T*>(src), selector, from, to)

// Map a runtime element width to a compile-time-width call of `SCAN<Elem>`.
// Every real column width is enumerated so only exotic widths fall through to
// the runtime-sized copy.
#define FILTER_WIDTH_DISPATCH(SCAN)                                           \
    switch (element_size) {                                                   \
    case 1:                                                                   \
        return FILTER_CALL(SCAN, uint8_t);                                    \
    case 2:                                                                   \
        return FILTER_CALL(SCAN, uint16_t);                                   \
    case 3:                                                                   \
        return FILTER_CALL(SCAN, Bytes<3>);                                   \
    case 4:                                                                   \
        return FILTER_CALL(SCAN, uint32_t);                                   \
    case 8:                                                                   \
        return FILTER_CALL(SCAN, uint64_t);                                   \
    case 12:                                                                  \
        return FILTER_CALL(SCAN, Bytes<12>);                                  \
    case 16:                                                                  \
        return FILTER_CALL(SCAN, Bytes<16>);                                  \
    case 32:                                                                  \
        return FILTER_CALL(SCAN, Bytes<32>);                                  \
    default:                                                                  \
        return scan_scalar_bytes(dst, src, element_size, selector, from, to); \
    }

#if defined(__x86_64__)

constexpr size_t kBatchNums = 256 / 8; // selector bytes scanned per 256-bit AVX2 load

// AVX2 batch scan (compile-time element width). Whole all-dropped batches are
// skipped and whole all-kept batches move with one memmove; only mixed batches
// fall to a per-element copy. Fast path for EVERY width, independent of the
// AVX-512 compress below.
template <typename Elem>
__attribute__((target("avx2"))) size_t scan_avx2(Elem* dst, const Elem* src, const uint8_t* selector, size_t from,
                                                 size_t to) {
    size_t start = from;
    size_t result = from;
    const __m256i all0 = _mm256_setzero_si256();
    while (start + kBatchNums <= to) {
        __m256i sel = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(selector + start));
        uint32_t mask = _mm256_movemask_epi8(_mm256_cmpgt_epi8(sel, all0));
        if (mask == 0) {
            // whole batch dropped
        } else if (mask == 0xffffffff) {
            memmove(dst + result, src + start, kBatchNums * sizeof(Elem));
            result += kBatchNums;
        } else {
            for (uint32_t m = mask; m != 0; m &= m - 1) {
                dst[result++] = src[start + __builtin_ctz(m)];
            }
        }
        start += kBatchNums;
    }
    for (; start < to; ++start) {
        if (selector[start]) dst[result++] = src[start];
    }
    return result;
}

// AVX-512 vpcompressd for 4-byte lanes (same scan, compress in the mixed branch).
__attribute__((target("avx512f,avx512vl,avx512bw"))) size_t compress_w4(uint32_t* dst, const uint32_t* src,
                                                                        const uint8_t* selector, size_t from,
                                                                        size_t to) {
    size_t start = from;
    size_t result = from;
    const __m256i all0 = _mm256_setzero_si256();
    while (start + kBatchNums <= to) {
        __m256i sel = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(selector + start));
        uint32_t mask = _mm256_movemask_epi8(_mm256_cmpgt_epi8(sel, all0));
        if (mask == 0) {
            // whole batch dropped
        } else if (mask == 0xffffffff) {
            memmove(dst + result, src + start, kBatchNums * sizeof(uint32_t));
            result += kBatchNums;
        } else {
            // 32 lanes = two groups of 16.
            _mm512_storeu_si512(dst + result, _mm512_maskz_compress_epi32(static_cast<__mmask16>(mask),
                                                                          _mm512_loadu_si512(src + start)));
            result += __builtin_popcount(mask & 0xffffu);
            _mm512_storeu_si512(dst + result, _mm512_maskz_compress_epi32(static_cast<__mmask16>(mask >> 16),
                                                                          _mm512_loadu_si512(src + start + 16)));
            result += __builtin_popcount(mask >> 16);
        }
        start += kBatchNums;
    }
    for (; start < to; ++start) {
        if (selector[start]) dst[result++] = src[start];
    }
    return result;
}

// AVX-512 vpcompressq for 8-byte lanes, with a popcount guard for sparse batches.
__attribute__((target("avx512f,avx512vl,avx512bw"))) size_t compress_w8(uint64_t* dst, const uint64_t* src,
                                                                        const uint8_t* selector, size_t from,
                                                                        size_t to) {
    size_t start = from;
    size_t result = from;
    const __m256i all0 = _mm256_setzero_si256();
    while (start + kBatchNums <= to) {
        __m256i sel = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(selector + start));
        uint32_t mask = _mm256_movemask_epi8(_mm256_cmpgt_epi8(sel, all0));
        if (mask == 0) {
            // whole batch dropped
        } else if (mask == 0xffffffff) {
            memmove(dst + result, src + start, kBatchNums * sizeof(uint64_t));
            result += kBatchNums;
        } else if (__builtin_popcount(mask) < kCompressMinBits) {
            for (uint32_t m = mask; m != 0; m &= m - 1) {
                dst[result++] = src[start + __builtin_ctz(m)];
            }
        } else {
            // 32 lanes = four groups of 8.
            for (int g = 0; g < 4; ++g) {
                uint32_t group = mask >> (g * 8);
                _mm512_storeu_si512(dst + result, _mm512_maskz_compress_epi64(static_cast<__mmask8>(group),
                                                                              _mm512_loadu_si512(src + start + g * 8)));
                result += __builtin_popcount(group & 0xffu);
            }
        }
        start += kBatchNums;
    }
    for (; start < to; ++start) {
        if (selector[start]) dst[result++] = src[start];
    }
    return result;
}

// AVX2 scan for every width; shared by the AVX2 clone and (for non-4/8 widths)
// the AVX-512 clone.
__attribute__((target("avx2"))) size_t scan_avx2_any(void* dst, const void* src, size_t element_size,
                                                     const uint8_t* selector, size_t from, size_t to) {
    FILTER_WIDTH_DISPATCH(scan_avx2)
}

#endif // __x86_64__

#if defined(__ARM_NEON) && defined(__aarch64__)

constexpr size_t kBatchNums = 128 / 8; // selector bytes per 128-bit NEON load

// NEON batch scan (compile-time element width); FMV is x86-only, dispatch here.
template <typename Elem>
size_t scan_neon(Elem* dst, const Elem* src, const uint8_t* selector, size_t from, size_t to) {
    size_t start = from;
    size_t result = from;
    const uint8_t* sel = selector + from;
    while (start + kBatchNums < to) {
        const uint8x16_t vsel = vld1q_u8(sel);
        // nibble_mask[i] != 0 ? 0xF : 0x0
        uint64_t nibble_mask = SIMD::get_nibble_mask(vtstq_u8(vsel, vsel));
        if (nibble_mask == 0) {
            // whole batch dropped
        } else if (nibble_mask == 0xffff'ffff'ffff'ffffull) {
            memmove(dst + result, src + start, kBatchNums * sizeof(Elem));
            result += kBatchNums;
        } else {
            // Keep only the high bit of each nibble: 0b1111 -> 0b1000.
            nibble_mask &= 0x8888'8888'8888'8888ull;
            for (; nibble_mask > 0; nibble_mask &= nibble_mask - 1) {
                dst[result++] = src[start + (__builtin_ctzll(nibble_mask) >> 2)];
            }
        }
        start += kBatchNums;
        sel += kBatchNums;
    }
    for (; start < to; ++start) {
        if (selector[start]) dst[result++] = src[start];
    }
    return result;
}

#endif // __ARM_NEON

// Scalar dispatch shared by the default clone (x86) and non-SIMD targets.
// [[maybe_unused]]: on aarch64 the NEON path is used instead.
[[maybe_unused]] size_t scan_scalar_any(void* dst, const void* src, size_t element_size, const uint8_t* selector,
                                        size_t from, size_t to) {
    FILTER_WIDTH_DISPATCH(scan_scalar)
}

#if defined(__x86_64__)

// One IFUNC family. Each clone maps the width to a compile-time-width code path,
// so the hot per-element copies never use a runtime-sized memcpy.
MFV_DEFAULT(size_t filter_impl(void* dst, const void* src, size_t element_size, const uint8_t* selector, size_t from,
                               size_t to) { return scan_scalar_any(dst, src, element_size, selector, from, to); })
MFV_AVX2(size_t filter_impl(void* dst, const void* src, size_t element_size, const uint8_t* selector, size_t from,
                            size_t to) { return scan_avx2_any(dst, src, element_size, selector, from, to); })
MFV_AVX512VLBW(size_t filter_impl(void* dst, const void* src, size_t element_size, const uint8_t* selector, size_t from,
                                  size_t to) {
    if (element_size == 4) return FILTER_CALL(compress_w4, uint32_t);
    if (element_size == 8) return FILTER_CALL(compress_w8, uint64_t);
    return scan_avx2_any(dst, src, element_size, selector, from, to);
})

#endif // __x86_64__

} // namespace

size_t detail::filter_range(void* dst, const void* src, size_t element_size, const uint8_t* selector, size_t from,
                            size_t to) {
#if defined(__x86_64__)
    return filter_impl(dst, src, element_size, selector, from, to);
#elif defined(__ARM_NEON) && defined(__aarch64__)
    FILTER_WIDTH_DISPATCH(scan_neon)
#else
    return scan_scalar_any(dst, src, element_size, selector, from, to);
#endif
}

#undef FILTER_WIDTH_DISPATCH
#undef FILTER_CALL

} // namespace SIMD::Filter
