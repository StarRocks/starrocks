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
#if defined(__x86_64__)
constexpr int kCompressMinBits = 6;
#endif

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

constexpr auto make_permute_table_8x32() {
    std::array<std::array<int32_t, 8>, 256> table{};
    for (int mask = 0; mask < 256; ++mask) {
        int out_idx = 0;
        for (int bit = 0; bit < 8; ++bit) {
            if (mask & (1 << bit)) {
                table[mask][out_idx++] = bit;
            }
        }
        for (int i = out_idx; i < 8; ++i) {
            table[mask][i] = 0;
        }
    }
    return table;
}

alignas(64) inline constexpr auto kPermuteTable8x32 = make_permute_table_8x32();

constexpr auto make_permute_table_4x64() {
    std::array<std::array<int32_t, 8>, 16> table{};
    for (int mask = 0; mask < 16; ++mask) {
        int out_lane = 0;
        for (int lane = 0; lane < 4; ++lane) {
            if (mask & (1 << lane)) {
                table[mask][out_lane * 2] = lane * 2;
                table[mask][out_lane * 2 + 1] = lane * 2 + 1;
                out_lane++;
            }
        }
        for (int i = out_lane * 2; i < 8; ++i) {
            table[mask][i] = 0;
        }
    }
    return table;
}

alignas(64) inline constexpr auto kPermuteTable4x64 = make_permute_table_4x64();

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
        // Keep every non-zero selector byte: cmpeq-zero + invert, so that bytes
        // with the high bit set (e.g. 0xff) are kept, not dropped as a signed
        // cmpgt(sel, 0) would.
        uint32_t mask = ~_mm256_movemask_epi8(_mm256_cmpeq_epi8(sel, all0));
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

// AVX2 vectorized compaction for 4-byte lanes using _mm256_permutevar8x32_epi32
__attribute__((target("avx2"))) size_t compress_avx2_w4(uint32_t* dst, const uint32_t* src, const uint8_t* selector,
                                                        size_t from, size_t to) {
    size_t start = from;
    size_t result = from;
    const __m256i all0 = _mm256_setzero_si256();
    while (start + kBatchNums <= to) {
        __m256i sel = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(selector + start));
        uint32_t mask = ~_mm256_movemask_epi8(_mm256_cmpeq_epi8(sel, all0));
        if (mask == 0) {
            // whole batch dropped
        } else if (mask == 0xffffffff) {
            memmove(dst + result, src + start, kBatchNums * sizeof(uint32_t));
            result += kBatchNums;
        } else {
            for (int g = 0; g < 4; ++g) {
                uint32_t sub_mask = (mask >> (g * 8)) & 0xffu;
                if (sub_mask == 0) {
                    continue;
                } else if (sub_mask == 0xffu) {
                    __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + start + g * 8));
                    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + result), v);
                    result += 8;
                } else {
                    __m256i v_src = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + start + g * 8));
                    __m256i v_perm =
                            _mm256_load_si256(reinterpret_cast<const __m256i*>(kPermuteTable8x32[sub_mask].data()));
                    __m256i v_compact = _mm256_permutevar8x32_epi32(v_src, v_perm);
                    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + result), v_compact);
                    result += _mm_popcnt_u32(sub_mask);
                }
            }
        }
        start += kBatchNums;
    }
    for (; start < to; ++start) {
        if (selector[start]) dst[result++] = src[start];
    }
    return result;
}

// AVX2 vectorized compaction for 8-byte lanes using _mm256_permutevar8x32_epi32
__attribute__((target("avx2"))) size_t compress_avx2_w8(uint64_t* dst, const uint64_t* src, const uint8_t* selector,
                                                        size_t from, size_t to) {
    size_t start = from;
    size_t result = from;
    const __m256i all0 = _mm256_setzero_si256();
    while (start + kBatchNums <= to) {
        __m256i sel = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(selector + start));
        uint32_t mask = ~_mm256_movemask_epi8(_mm256_cmpeq_epi8(sel, all0));
        if (mask == 0) {
            // whole batch dropped
        } else if (mask == 0xffffffff) {
            memmove(dst + result, src + start, kBatchNums * sizeof(uint64_t));
            result += kBatchNums;
        } else {
            for (int g = 0; g < 8; ++g) {
                uint32_t sub_mask = (mask >> (g * 4)) & 0x0fu;
                if (sub_mask == 0) {
                    continue;
                } else if (sub_mask == 0x0fu) {
                    __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + start + g * 4));
                    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + result), v);
                    result += 4;
                } else {
                    __m256i v_src = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + start + g * 4));
                    __m256i v_perm =
                            _mm256_load_si256(reinterpret_cast<const __m256i*>(kPermuteTable4x64[sub_mask].data()));
                    __m256i v_compact = _mm256_permutevar8x32_epi32(v_src, v_perm);
                    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + result), v_compact);
                    result += _mm_popcnt_u32(sub_mask);
                }
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
        // Keep every non-zero selector byte: cmpeq-zero + invert, so that bytes
        // with the high bit set (e.g. 0xff) are kept, not dropped as a signed
        // cmpgt(sel, 0) would.
        uint32_t mask = ~_mm256_movemask_epi8(_mm256_cmpeq_epi8(sel, all0));
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
        // Keep every non-zero selector byte: cmpeq-zero + invert, so that bytes
        // with the high bit set (e.g. 0xff) are kept, not dropped as a signed
        // cmpgt(sel, 0) would.
        uint32_t mask = ~_mm256_movemask_epi8(_mm256_cmpeq_epi8(sel, all0));
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

#if defined(__has_include)
#if __has_include(<sys/auxv.h>) && __has_include(<asm/hwcap.h>)
#include <asm/hwcap.h>
#include <sys/auxv.h>
#define STARROCKS_HAS_ARM_HWCAP 1
#endif
#endif

#if defined(__has_include)
#if __has_include(<arm_sve.h>)
#define STARROCKS_HAS_ARM_SVE 1
#include <arm_sve.h>
#endif
#endif

inline bool cpu_has_sve2() {
#if defined(STARROCKS_HAS_ARM_HWCAP) && defined(HWCAP2_SVE2)
    static const bool has_sve2 = (getauxval(AT_HWCAP2) & HWCAP2_SVE2) != 0;
    return has_sve2;
#else
    return false;
#endif
}

constexpr size_t kBatchNums = 128 / 8; // selector bytes per 128-bit NEON load

constexpr auto make_neon_shuffle_table_4x32() {
    std::array<std::array<uint8_t, 16>, 16> table{};
    for (int mask = 0; mask < 16; ++mask) {
        int out_lane = 0;
        for (int lane = 0; lane < 4; ++lane) {
            if (mask & (1 << lane)) {
                for (int b = 0; b < 4; ++b) {
                    table[mask][out_lane * 4 + b] = static_cast<uint8_t>(lane * 4 + b);
                }
                out_lane++;
            }
        }
        for (int i = out_lane * 4; i < 16; ++i) {
            table[mask][i] = 0xFF;
        }
    }
    return table;
}

alignas(64) inline constexpr auto kNeonShuffleTable4x32 = make_neon_shuffle_table_4x32();

// NEON batch scan (compile-time element width); FMV is x86-only, dispatch here.
template <typename Elem>
size_t scan_neon(Elem* dst, const Elem* src, const uint8_t* selector, size_t from, size_t to) {
    size_t start = from;
    size_t result = from;
    const uint8_t* sel = selector + from;
    while (start + kBatchNums <= to) {
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

// Vectorized NEON compaction for 4-byte lanes using vqtbl1q_u8 table permute
size_t compress_neon_w4(uint32_t* dst, const uint32_t* src, const uint8_t* selector, size_t from, size_t to) {
    size_t start = from;
    size_t result = from;
    while (start + kBatchNums <= to) {
        const uint8x16_t vsel = vld1q_u8(selector + start);
        uint64_t nibble_mask = SIMD::get_nibble_mask(vtstq_u8(vsel, vsel));
        if (nibble_mask == 0) {
            // all dropped
        } else if (nibble_mask == 0xffff'ffff'ffff'ffffull) {
            memmove(dst + result, src + start, kBatchNums * sizeof(uint32_t));
            result += kBatchNums;
        } else {
            for (int g = 0; g < 4; ++g) {
                uint32_t chunk_nibbles = static_cast<uint32_t>((nibble_mask >> (g * 16)) & 0xffffu);
                if (chunk_nibbles == 0) {
                    continue;
                } else if (chunk_nibbles == 0x8888u) {
                    uint32x4_t v = vld1q_u32(src + start + g * 4);
                    vst1q_u32(dst + result, v);
                    result += 4;
                } else {
                    uint32_t mask = ((chunk_nibbles & 0x8u) >> 3) | ((chunk_nibbles & 0x80u) >> 6) |
                                    ((chunk_nibbles & 0x800u) >> 9) | ((chunk_nibbles & 0x8000u) >> 12);
                    uint8x16_t v_src = vld1q_u8(reinterpret_cast<const uint8_t*>(src + start + g * 4));
                    uint8x16_t v_perm = vld1q_u8(kNeonShuffleTable4x32[mask].data());
                    uint8x16_t v_compact = vqtbl1q_u8(v_src, v_perm);
                    vst1q_u8(reinterpret_cast<uint8_t*>(dst + result), v_compact);
                    result += __builtin_popcount(mask);
                }
            }
        }
        start += kBatchNums;
    }
    for (; start < to; ++start) {
        if (selector[start]) dst[result++] = src[start];
    }
    return result;
}

#if defined(STARROCKS_HAS_ARM_SVE)
__attribute__((target("+sve2"))) size_t compress_sve2_w4(uint32_t* dst, const uint32_t* src, const uint8_t* selector,
                                                         size_t from, size_t to) {
    size_t start = from;
    size_t result = from;
    while (start + kBatchNums <= to) {
        const uint8x16_t vsel = vld1q_u8(selector + start);
        uint64_t nibble_mask = SIMD::get_nibble_mask(vtstq_u8(vsel, vsel));
        if (nibble_mask == 0) {
            // all dropped
        } else if (nibble_mask == 0xffff'ffff'ffff'ffffull) {
            memmove(dst + result, src + start, kBatchNums * sizeof(uint32_t));
            result += kBatchNums;
        } else {
            for (size_t g = 0; g < kBatchNums; g += svcntw()) {
                svbool_t pg = svwhilelt_b32_u64(g, kBatchNums);
                svuint32_t sel_u32 = svld1ub_u32(pg, selector + start + g);
                svbool_t mask_pg = svcmpne_n_u32(pg, sel_u32, 0);
                uint64_t count = svcntp_b32(pg, mask_pg);
                if (count == 0) {
                    continue;
                }
                uint64_t active_lanes = svcntp_b32(pg, pg);
                if (count == active_lanes) {
                    svuint32_t v_src = svld1_u32(pg, src + start + g);
                    svst1_u32(pg, dst + result, v_src);
                    result += active_lanes;
                } else {
                    svuint32_t v_src = svld1_u32(pg, src + start + g);
                    svuint32_t compacted = svcompact_u32(mask_pg, v_src);
                    svbool_t store_pg = svwhilelt_b32_u64(0, count);
                    svst1_u32(store_pg, dst + result, compacted);
                    result += count;
                }
            }
        }
        start += kBatchNums;
    }
    for (; start < to; ++start) {
        if (selector[start]) dst[result++] = src[start];
    }
    return result;
}
#endif // STARROCKS_HAS_ARM_SVE

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
                            size_t to) {
    if (element_size == 4) return FILTER_CALL(compress_avx2_w4, uint32_t);
    if (element_size == 8) return FILTER_CALL(compress_avx2_w8, uint64_t);
    return scan_avx2_any(dst, src, element_size, selector, from, to);
})
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
#if defined(STARROCKS_HAS_ARM_SVE)
    if (cpu_has_sve2()) {
        if (element_size == 4) return FILTER_CALL(compress_sve2_w4, uint32_t);
    }
#endif
    if (element_size == 4) return FILTER_CALL(compress_neon_w4, uint32_t);
    FILTER_WIDTH_DISPATCH(scan_neon)
#else
    return scan_scalar_any(dst, src, element_size, selector, from, to);
#endif
}

#undef FILTER_WIDTH_DISPATCH
#undef FILTER_CALL

} // namespace SIMD::Filter
