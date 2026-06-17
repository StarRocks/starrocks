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

#if defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

#include "base/bit/bit_packing_default.h"
#include "base/logging.h"

namespace starrocks::util::bitpacking_neon {

#if defined(__ARM_NEON) && defined(__aarch64__)

// Forward-declare the primary template before the uint8_t/uint16_t/uint32_t
// explicit specializations below — C++ requires the primary to be visible first.
// Definition is at the bottom of the NEON block.
template <typename T>
inline void unpack(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
                   T* __restrict__ out);

// ============================================================================
// NEON Bit Unpacking Implementation
// Uses shift-mask approach since ARM doesn't have pdep instruction
// Processes 8 values at a time for uint32_t output
// ============================================================================

// Masks for extracting values of different bit widths
static constexpr uint32_t kMask[] = {
        0x00000000, 0x00000001, 0x00000003, 0x00000007, // 0-3 bits
        0x0000000f, 0x0000001f, 0x0000003f, 0x0000007f, // 4-7 bits
        0x000000ff, 0x000001ff, 0x000003ff, 0x000007ff, // 8-11 bits
        0x00000fff, 0x00001fff, 0x00003fff, 0x00007fff, // 12-15 bits
        0x0000ffff, 0x0001ffff, 0x0003ffff, 0x0007ffff, // 16-19 bits
        0x000fffff, 0x001fffff, 0x003fffff, 0x007fffff, // 20-23 bits
        0x00ffffff, 0x01ffffff, 0x03ffffff, 0x07ffffff, // 24-27 bits
        0x0fffffff, 0x1fffffff, 0x3fffffff, 0x7fffffff, // 28-31 bits
        0xffffffff                                      // 32 bits
};

// Unpack 8 values with bit_width in [1, 8] range to uint32_t
// Input: bit_width bytes containing 8 packed values
static inline void unpack1to8_uint32(const uint8_t* __restrict__ in, uint32_t* __restrict__ out, int64_t num_values,
                                     int bit_width) {
    const uint32_t mask = kMask[bit_width];
    auto writeEnd = out + num_values;

    while (out + 8 <= writeEnd) {
        // Load up to 8 bytes (enough for 8 values at max 8 bits each)
        uint64_t packed = 0;
        memcpy(&packed, in, bit_width); // Load exactly bit_width bytes for 8 values

        // Extract 8 values using shifts and masks
        uint32_t v0 = (packed >> (0 * bit_width)) & mask;
        uint32_t v1 = (packed >> (1 * bit_width)) & mask;
        uint32_t v2 = (packed >> (2 * bit_width)) & mask;
        uint32_t v3 = (packed >> (3 * bit_width)) & mask;
        uint32_t v4 = (packed >> (4 * bit_width)) & mask;
        uint32_t v5 = (packed >> (5 * bit_width)) & mask;
        uint32_t v6 = (packed >> (6 * bit_width)) & mask;
        uint32_t v7 = (packed >> (7 * bit_width)) & mask;

        // Store using NEON for efficiency
        uint32x4_t vec_lo = {v0, v1, v2, v3};
        uint32x4_t vec_hi = {v4, v5, v6, v7};
        vst1q_u32(out, vec_lo);
        vst1q_u32(out + 4, vec_hi);

        in += bit_width;
        out += 8;
    }
}

// Unpack 8 values with bit_width in [9, 16] range to uint32_t
static inline void unpack9to16_uint32(const uint8_t* __restrict__ in, uint32_t* __restrict__ out, int64_t num_values,
                                      int bit_width) {
    const uint32_t mask = kMask[bit_width];
    auto writeEnd = out + num_values;

    // For 9-16 bit widths, we need exactly bit_width bytes for 8 values
    while (out + 8 <= writeEnd) {
        // Safe load: only read exactly bit_width bytes (8 values * bit_width bits / 8)
        uint64_t lo = 0, hi = 0;
        memcpy(&lo, in, std::min(8, bit_width));
        if (bit_width > 8) {
            memcpy(&hi, in + 8, bit_width - 8);
        }

        // Extract values using bit positions
        int bit_pos = 0;
        uint32_t values[8];
        for (int i = 0; i < 8; ++i) {
            int byte_idx = bit_pos / 8;
            int bit_offset = bit_pos % 8;

            uint64_t src = (byte_idx < 8) ? lo : hi;
            int local_byte = byte_idx % 8;
            uint64_t shifted = src >> (local_byte * 8 + bit_offset);

            // Handle crossing 64-bit boundary
            if (byte_idx < 8 && local_byte * 8 + bit_offset + bit_width > 64) {
                int bits_from_hi = (local_byte * 8 + bit_offset + bit_width) - 64;
                shifted |= (hi << (bit_width - bits_from_hi));
            }

            values[i] = shifted & mask;
            bit_pos += bit_width;
        }

        // Store using NEON
        uint32x4_t vec_lo = vld1q_u32(values);
        uint32x4_t vec_hi = vld1q_u32(values + 4);
        vst1q_u32(out, vec_lo);
        vst1q_u32(out + 4, vec_hi);

        in += bit_width;
        out += 8;
    }
}

// Unpack 8 values with bit_width in [17, 24] range to uint32_t
static inline void unpack17to24_uint32(const uint8_t* __restrict__ in, uint32_t* __restrict__ out, int64_t num_values,
                                       int bit_width) {
    const uint32_t mask = kMask[bit_width];
    auto writeEnd = out + num_values;

    while (out + 8 <= writeEnd) {
        // Safe load: read all bit_width bytes at once into buffer
        uint8_t buffer[32] = {0}; // Max 24 bytes needed, pad for safety
        memcpy(buffer, in, bit_width);

        uint32_t values[8];
        int bit_pos = 0;

        for (int i = 0; i < 8; ++i) {
            int byte_idx = bit_pos / 8;
            int bit_offset = bit_pos % 8;

            // Safe read from buffer (no overflow possible)
            uint32_t word;
            memcpy(&word, buffer + byte_idx, sizeof(uint32_t));
            values[i] = (word >> bit_offset) & mask;

            bit_pos += bit_width;
        }

        // Store using NEON
        uint32x4_t vec_lo = vld1q_u32(values);
        uint32x4_t vec_hi = vld1q_u32(values + 4);
        vst1q_u32(out, vec_lo);
        vst1q_u32(out + 4, vec_hi);

        in += bit_width;
        out += 8;
    }
}

// Unpack 8 values with bit_width in [25, 32] range to uint32_t
static inline void unpack25to32_uint32(const uint8_t* __restrict__ in, uint32_t* __restrict__ out, int64_t num_values,
                                       int bit_width) {
    const uint32_t mask = kMask[bit_width];
    auto writeEnd = out + num_values;

    while (out + 8 <= writeEnd) {
        // Safe load: read all bit_width bytes at once into buffer
        uint8_t buffer[40] = {0}; // Max 32 bytes needed + padding for safe reads
        memcpy(buffer, in, bit_width);

        uint32_t values[8];
        int bit_pos = 0;

        for (int i = 0; i < 8; ++i) {
            int byte_idx = bit_pos / 8;
            int bit_offset = bit_pos % 8;

            // Safe read from buffer
            uint64_t qword;
            memcpy(&qword, buffer + byte_idx, 5);
            values[i] = (qword >> bit_offset) & mask;

            bit_pos += bit_width;
        }

        // Store using NEON
        uint32x4_t vec_lo = vld1q_u32(values);
        uint32x4_t vec_hi = vld1q_u32(values + 4);
        vst1q_u32(out, vec_lo);
        vst1q_u32(out + 4, vec_hi);

        in += bit_width;
        out += 8;
    }
}

// Main unpack function for uint32_t
template <>
inline void unpack<uint32_t>(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
                             uint32_t* __restrict__ out) {
    DCHECK(bit_width >= 1 && bit_width <= 32);
    DCHECK(in_bytes * 8 >= bit_width * num_values);
    DCHECK(num_values % 8 == 0);

    switch (bit_width) {
    case 1:
    case 2:
    case 3:
    case 4:
    case 5:
    case 6:
    case 7:
    case 8:
        unpack1to8_uint32(in, out, num_values, bit_width);
        break;
    case 9:
    case 10:
    case 11:
    case 12:
    case 13:
    case 14:
    case 15:
    case 16:
        unpack9to16_uint32(in, out, num_values, bit_width);
        break;
    case 17:
    case 18:
    case 19:
    case 20:
    case 21:
    case 22:
    case 23:
    case 24:
        unpack17to24_uint32(in, out, num_values, bit_width);
        break;
    case 25:
    case 26:
    case 27:
    case 28:
    case 29:
    case 30:
    case 31:
    case 32:
        unpack25to32_uint32(in, out, num_values, bit_width);
        break;
    default:
        DCHECK(false) << "invalid bit_width";
    }
}

// Unpack for uint16_t - similar structure
static inline void unpack1to8_uint16(const uint8_t* __restrict__ in, uint16_t* __restrict__ out, int64_t num_values,
                                     int bit_width) {
    const uint16_t mask = static_cast<uint16_t>(kMask[bit_width]);
    auto writeEnd = out + num_values;

    while (out + 8 <= writeEnd) {
        uint64_t packed = 0;
        memcpy(&packed, in, bit_width);

        uint16_t v0 = (packed >> (0 * bit_width)) & mask;
        uint16_t v1 = (packed >> (1 * bit_width)) & mask;
        uint16_t v2 = (packed >> (2 * bit_width)) & mask;
        uint16_t v3 = (packed >> (3 * bit_width)) & mask;
        uint16_t v4 = (packed >> (4 * bit_width)) & mask;
        uint16_t v5 = (packed >> (5 * bit_width)) & mask;
        uint16_t v6 = (packed >> (6 * bit_width)) & mask;
        uint16_t v7 = (packed >> (7 * bit_width)) & mask;

        uint16x8_t vec = {v0, v1, v2, v3, v4, v5, v6, v7};
        vst1q_u16(out, vec);

        in += bit_width;
        out += 8;
    }
}

static inline void unpack9to16_uint16(const uint8_t* __restrict__ in, uint16_t* __restrict__ out, int64_t num_values,
                                      int bit_width) {
    const uint16_t mask = static_cast<uint16_t>(kMask[bit_width]);
    auto writeEnd = out + num_values;

    while (out + 8 <= writeEnd) {
        // Safe load: copy all bit_width bytes into buffer to avoid overread
        uint8_t buffer[20] = {0}; // Max 16 bytes needed + padding for safe 4-byte reads
        memcpy(buffer, in, bit_width);

        uint16_t values[8];
        int bit_pos = 0;

        for (int i = 0; i < 8; ++i) {
            int byte_idx = bit_pos / 8;
            int bit_offset = bit_pos % 8;

            // Safe read from buffer (no overflow possible)
            uint32_t word;
            memcpy(&word, buffer + byte_idx, sizeof(uint32_t));
            values[i] = (word >> bit_offset) & mask;

            bit_pos += bit_width;
        }

        uint16x8_t vec = vld1q_u16(values);
        vst1q_u16(out, vec);

        in += bit_width;
        out += 8;
    }
}

template <>
inline void unpack<uint16_t>(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
                             uint16_t* __restrict__ out) {
    DCHECK(bit_width >= 1 && bit_width <= 16);
    DCHECK(in_bytes * 8 >= bit_width * num_values);
    DCHECK(num_values % 8 == 0);

    if (bit_width <= 8) {
        unpack1to8_uint16(in, out, num_values, bit_width);
    } else {
        unpack9to16_uint16(in, out, num_values, bit_width);
    }
}

// Unpack for uint8_t
template <>
inline void unpack<uint8_t>(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
                            uint8_t* __restrict__ out) {
    DCHECK(bit_width >= 1 && bit_width <= 8);
    DCHECK(in_bytes * 8 >= bit_width * num_values);
    DCHECK(num_values % 8 == 0);

    const uint8_t mask = static_cast<uint8_t>(kMask[bit_width]);
    auto writeEnd = out + num_values;

    while (out + 8 <= writeEnd) {
        uint64_t packed = 0;
        memcpy(&packed, in, bit_width);

        uint8_t v0 = (packed >> (0 * bit_width)) & mask;
        uint8_t v1 = (packed >> (1 * bit_width)) & mask;
        uint8_t v2 = (packed >> (2 * bit_width)) & mask;
        uint8_t v3 = (packed >> (3 * bit_width)) & mask;
        uint8_t v4 = (packed >> (4 * bit_width)) & mask;
        uint8_t v5 = (packed >> (5 * bit_width)) & mask;
        uint8_t v6 = (packed >> (6 * bit_width)) & mask;
        uint8_t v7 = (packed >> (7 * bit_width)) & mask;

        uint8x8_t vec = {v0, v1, v2, v3, v4, v5, v6, v7};
        vst1_u8(out, vec);

        in += bit_width;
        out += 8;
    }
}

// Generic template fallback
template <typename T>
inline void unpack(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
                   T* __restrict__ out) {
    // Fallback to default for unsupported types
    auto res = starrocks::util::bitpacking_default::UnpackValues<T>(bit_width, in, in_bytes, num_values, out);
    (void)res;
}

#else // !NEON

// Fallback to default when NEON is not available
template <typename T>
inline void unpack(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
                   T* __restrict__ out) {
    auto res = starrocks::util::bitpacking_default::UnpackValues<T>(bit_width, in, in_bytes, num_values, out);
    (void)res;
}

#endif // __ARM_NEON && __aarch64__

// Main entry point - matches AVX2 interface
template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValues(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes,
                                                int64_t num_values, OutType* __restrict__ out) {
    // Handle bit_width=0 (valid for constant columns) - delegate to default to avoid divide-by-zero
    if (bit_width == 0) {
        return starrocks::util::bitpacking_default::UnpackValues(bit_width, in, in_bytes, num_values, out);
    }

    const int64_t values_to_read =
            starrocks::util::bitpacking_default::NumValuesToUnpack(bit_width, in_bytes, num_values);
    constexpr int BATCH_SIZE = 8;

    const int64_t batches_to_read =
            (in_bytes > (bit_width + 7) / 8 * 8)
                    ? std::min((in_bytes - (bit_width + 7) / 8 * 8) / bit_width + 1, values_to_read / BATCH_SIZE)
                    : 0;

    if (batches_to_read > 0) {
        unpack(bit_width, in, in_bytes, batches_to_read * BATCH_SIZE, out);
        in_bytes -= batches_to_read * bit_width;
        in += batches_to_read * bit_width;
        out += batches_to_read * BATCH_SIZE;
    }

    const int64_t remainder_values = values_to_read - batches_to_read * BATCH_SIZE;
    if (remainder_values > 0) {
        in = starrocks::util::bitpacking_default::UnpackValues(bit_width, in, in_bytes, remainder_values, out).first;
    }
    return std::make_pair(in, values_to_read);
}

} // namespace starrocks::util::bitpacking_neon
