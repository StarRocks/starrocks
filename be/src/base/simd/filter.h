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

namespace SIMD::Filter {

namespace detail {
// Type-erased implementation; defined in filter.cpp. Compaction is pure data
// movement, so only the element width matters -- this keeps the whole SIMD
// implementation in the .cpp and free of higher-level type dependencies.
size_t filter_range(void* dst, const void* src, size_t element_size, const uint8_t* selector, size_t from, size_t to);

alignas(16) inline constexpr uint8_t kPermuteLut4B[16][16] = {
        // 0000: none
        {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
        // 0001: lane 0
        {0, 1, 2, 3, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
        // 0010: lane 1
        {4, 5, 6, 7, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
        // 0011: lanes 0, 1
        {0, 1, 2, 3, 4, 5, 6, 7, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
        // 0100: lane 2
        {8, 9, 10, 11, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
        // 0101: lanes 0, 2
        {0, 1, 2, 3, 8, 9, 10, 11, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
        // 0110: lanes 1, 2
        {4, 5, 6, 7, 8, 9, 10, 11, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
        // 0111: lanes 0, 1, 2
        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 0xFF, 0xFF, 0xFF, 0xFF},
        // 1000: lane 3
        {12, 13, 14, 15, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
        // 1001: lanes 0, 3
        {0, 1, 2, 3, 12, 13, 14, 15, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
        // 1010: lanes 1, 3
        {4, 5, 6, 7, 12, 13, 14, 15, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
        // 1011: lanes 0, 1, 3
        {0, 1, 2, 3, 4, 5, 6, 7, 12, 13, 14, 15, 0xFF, 0xFF, 0xFF, 0xFF},
        // 1100: lanes 2, 3
        {8, 9, 10, 11, 12, 13, 14, 15, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
        // 1101: lanes 0, 2, 3
        {0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15, 0xFF, 0xFF, 0xFF, 0xFF},
        // 1110: lanes 1, 2, 3
        {4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0xFF, 0xFF, 0xFF, 0xFF},
        // 1111: lanes 0, 1, 2, 3 (all)
        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}};

inline constexpr uint8_t kPopcountLut4B[16] = {0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4};

alignas(16) inline constexpr uint8_t kPermuteLut8B[4][16] = {
        // 00: none
        {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
        // 01: lane 0
        {0, 1, 2, 3, 4, 5, 6, 7, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
        // 10: lane 1
        {8, 9, 10, 11, 12, 13, 14, 15, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
        // 11: lanes 0, 1
        {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}};

inline constexpr uint8_t kPopcountLut8B[4] = {0, 1, 1, 2};

// Extract 4-bit mask for sub-batch k in {0, 1, 2, 3} (4 lanes of 4B each) from 64-bit nibble_mask
constexpr uint32_t extract_neon_mask4(uint64_t nibble_mask, int k) noexcept {
    uint32_t c = static_cast<uint32_t>(nibble_mask >> (k * 16 + 3)) & 0x1111u;
    return ((c * 0x1248u) >> 12) & 0xFu;
}

// Extract 2-bit mask for sub-batch k in {0..7} (2 lanes of 8B each) from 64-bit nibble_mask
constexpr uint32_t extract_neon_mask2(uint64_t nibble_mask, int k) noexcept {
    uint32_t pair_bits = static_cast<uint32_t>(nibble_mask >> (k * 8 + 3)) & 0x11u;
    return (pair_bits | (pair_bits >> 3)) & 0x3u;
}

#if defined(__ARM_NEON) && defined(__aarch64__)
size_t compress_neon_w4(uint32_t* dst, const uint32_t* src, const uint8_t* selector, size_t from, size_t to);
size_t compress_neon_w8(uint64_t* dst, const uint64_t* src, const uint8_t* selector, size_t from, size_t to);
#endif

} // namespace detail

// Stream compaction: copy the elements of `src` whose selector byte is non-zero,
// over the index range [from, to), to the front of `dst` starting at index
// `from`, and return the new end offset. Elements before `from` are left
// untouched, and `dst` may alias `src` (in-place). `dst` must have capacity for
// at least `to` elements; vectorised stores may overwrite dead slots in
// `[returned_size, to)` before the caller resizes `dst`. 4- and 8-byte elements
// take a vectorised path (AVX-512 vpcompress / AVX2 on x86, NEON on aarch64);
// other sizes use a scalar byte copy.
template <typename T>
inline size_t filter_range(T* dst, const T* src, const uint8_t* selector, size_t from, size_t to) {
    return detail::filter_range(dst, src, sizeof(T), selector, from, to);
}

} // namespace SIMD::Filter
