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

#if defined(__AVX2__) && defined(__BMI2__)
#include <emmintrin.h>
#include <immintrin.h>
#endif

#include "common/logging.h"
#include "util/bit_packing.h"

namespace starrocks::util {

// Pdep instruction masks for uint8_t.
static constexpr const uint64_t kPdepMask8[] = {0x0000000000000000, 0x0101010101010101, 0x0303030303030303,
                                                0x0707070707070707, 0x0f0f0f0f0f0f0f0f, 0x1f1f1f1f1f1f1f1f,
                                                0x3f3f3f3f3f3f3f3f, 0x7f7f7f7f7f7f7f7f, 0xffffffffffffffff};

// Pdep instruction masks for uint16_t.
static constexpr const uint64_t kPdepMask16[] = {
        0x0000000000000000, 0x0001000100010001, 0x0003000300030003, 0x0007000700070007, 0x000f000f000f000f,
        0x001f001f001f001f, 0x003f003f003f003f, 0x007f007f007f007f, 0x00ff00ff00ff00ff, 0x01ff01ff01ff01ff,
        0x03ff03ff03ff03ff, 0x07ff07ff07ff07ff, 0x0fff0fff0fff0fff, 0x1fff1fff1fff1fff, 0x3fff3fff3fff3fff,
        0x7fff7fff7fff7fff, 0xffffffffffffffff};

// Pdep instruction masks for uint32_t.
static constexpr const uint64_t kPdepMask32[] = {
        0x0000000000000000, 0x0000000100000001, 0x0000000300000003, 0x0000000700000007, 0x0000000f0000000f,
        0x0000001f0000001f, 0x0000003f0000003f, 0x0000007f0000007f, 0x000000ff000000ff, 0x000001ff000001ff,
        0x000003ff000003ff, 0x000007ff000007ff, 0x00000fff00000fff, 0x00001fff00001fff, 0x00003fff00003fff,
        0x00007fff00007fff, 0x0000ffff0000ffff, 0x0001ffff0001ffff, 0x0003ffff0003ffff, 0x0007ffff0007ffff,
        0x000fffff000fffff, 0x001fffff001fffff, 0x003fffff003fffff, 0x007fffff007fffff, 0x00ffffff00ffffff,
        0x01ffffff01ffffff, 0x03ffffff03ffffff, 0x07ffffff07ffffff, 0x0fffffff0fffffff, 0x1fffffff1fffffff,
        0x3fffffff3fffffff, 0x7fffffff7fffffff, 0xffffffffffffffff};

// Unpack num_values number of input values from in. The results
// will be written to output. The caller needs to make sure the
// in_bytes contains at least num_values number of packed values.
// The in and out pointers need to be updated to the next to read
// position after this call.
template <typename T>
static inline void unpackNaive(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
                               T* __restrict__ out);

template <typename T>
static inline void unpackNaive(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
                               T* __restrict__ out) {
    DCHECK(bit_width >= 1 && bit_width <= sizeof(T) * 8);
    DCHECK(in_bytes * 8 >= bit_width * num_values);

    auto res = BitPacking::UnpackValues<T>(bit_width, in, in_bytes, num_values, out);
    in = res.first;
}

// Unpack num_values number of input values from in. The results
// will be written to output. num_values must be a multiple of 8. The
// caller needs to make sure the in_bytes contains at least num_values
// number of packed values. The in and out pointers need to be updated
// to the next to read position after this call.
template <typename T>
inline void unpack(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
                   T* __restrict__ out) {
    unpackNaive<T>(bit_width, in, in_bytes, num_values, out);
}

template <>
inline void unpack<uint8_t>(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
                            uint8_t* __restrict__ out);

template <>
inline void unpack<uint16_t>(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
                             uint16_t* __restrict__ out);

template <>
inline void unpack<uint32_t>(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
                             uint32_t* __restrict__ out);

#if defined(__AVX2__) && defined(__BMI2__)
// num_values number of uint16_t values with bit_width in
//  [1, 7] range.
static inline void unpack1to7(const uint8_t* __restrict__ in, uint16_t* __restrict__ out, int64_t num_values,
                              int bit_width) {
    uint64_t pdepMask = kPdepMask8[bit_width];
    auto writeEndOffset = out + num_values;

    while (out + 8 <= writeEndOffset) {
        uint64_t val = *reinterpret_cast<const uint64_t*>(in);
        uint64_t intermediateVal = _pdep_u64(val, pdepMask);

        __m128i result = _mm_cvtepu8_epi16(_mm_loadl_epi64((reinterpret_cast<const __m128i*>(&intermediateVal))));
        _mm_storeu_si128(reinterpret_cast<__m128i*>(out), result);

        in += bit_width;
        out += 8;
    }
}

static inline void unpack8(const uint8_t* __restrict__ in, uint16_t* __restrict__ out, int64_t num_values) {
    auto writeEndOffset = out + num_values;

    while (out + 8 <= writeEndOffset) {
        uint64_t value = *(reinterpret_cast<const uint64_t*>(in));

        __m128i result = _mm_cvtepu8_epi16(_mm_loadl_epi64((const __m128i*)&value));
        _mm_storeu_si128(reinterpret_cast<__m128i*>(out), result);

        in += 8;
        out += 8;
    }
}

// Unpack num_values number of uint16_t values with bit_width in [9, 15] range.
static inline void unpack9to15(const uint8_t* __restrict__ in, uint16_t* __restrict__ out, int64_t num_values,
                               int bit_width) {
    uint64_t pdepMask = kPdepMask16[bit_width];

    uint8_t shift1 = bit_width * 4;
    uint8_t shift2 = 64 - shift1;

    auto writeEndOffset = out + num_values;

    // Process bit_width bytes (8 values) a time.
    while (out + 8 <= writeEndOffset) {
        uint64_t value_0 = *(reinterpret_cast<const uint64_t*>(in));
        uint64_t value_1 = *(reinterpret_cast<const uint64_t*>(in + 8));

        *reinterpret_cast<uint64_t*>(out) = _pdep_u64(value_0, pdepMask);
        *reinterpret_cast<uint64_t*>(out + 4) = _pdep_u64((value_0 >> shift1) | (value_1 << shift2), pdepMask);

        in += bit_width;
        out += 8;
    }
}

// Unpack num_values number of uint16_t values with bit_width = 16
static inline void unpack16(const uint8_t* __restrict__ in, uint16_t* __restrict__ out, int64_t num_values) {
    uint64_t numBytes = num_values * 2;
    std::memcpy(out, in, numBytes);

    in += numBytes;
    out += num_values;
}

// Unpack num_values number of uint32_t values with bit_width in [1, 7] range.
static inline void unpack1to7(const uint8_t* __restrict__ in, uint32_t* __restrict__ out, int64_t num_values,
                              int bit_width) {
    uint64_t pdepMask = kPdepMask8[bit_width];

    auto writeEndOffset = out + num_values;

    // Process bit_width bytes (8 values) a time.
    while (out + 8 <= writeEndOffset) {
        uint64_t val = *reinterpret_cast<const uint64_t*>(in);

        uint64_t intermediateVal = _pdep_u64(val, pdepMask);
        __m256i result = _mm256_cvtepu8_epi32(_mm_loadl_epi64(reinterpret_cast<const __m128i*>(&intermediateVal)));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(out), result);

        in += bit_width;
        out += 8;
    }
}

// Unpack num_values number of uint32_t values with bit_width = 8.
static inline void unpack8(const uint8_t* __restrict__ in, uint32_t* __restrict__ out, int64_t num_values) {
    auto writeEndOffset = out + num_values;

    // Process 8 bytes (8 values) a time.
    while (out + 8 <= writeEndOffset) {
        uint64_t value = *(reinterpret_cast<const uint64_t*>(in));
        __m128i packed = _mm_set_epi64x(0, value);
        __m256i result = _mm256_cvtepu8_epi32(packed);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(out), result);

        in += 8;
        out += 8;
    }
}

// Unpack num_values number of uint32_t values with bit_width in [9, 15] range.
static inline void unpack9to15(const uint8_t* __restrict__ in, uint32_t* __restrict__ out, int64_t num_values,
                               int bit_width) {
    uint64_t pdepMask = kPdepMask16[bit_width];

    uint8_t shift1 = bit_width * 4;
    uint8_t shift2 = 64 - shift1;

    auto writeEndOffset = out + num_values;

    // Process bit_width bytes (8 values) a time.
    while (out + 8 <= writeEndOffset) {
        uint64_t value_0 = *(reinterpret_cast<const uint64_t*>(in));
        uint64_t value_1 = *(reinterpret_cast<const uint64_t*>(in + 8));

        uint64_t intermediateValue_0 = _pdep_u64(value_0, pdepMask);
        uint64_t intermediateValue_1 = _pdep_u64((value_0 >> shift1) | (value_1 << shift2), pdepMask);

        __m128i packed = _mm_set_epi64x(intermediateValue_1, intermediateValue_0);
        __m256i result = _mm256_cvtepu16_epi32(packed);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(out), result);

        in += bit_width;
        out += 8;
    }
}

// Unpack num_values number of uint32_t values with bit_width = 16.
static inline void unpack16(const uint8_t* __restrict__ in, uint32_t* __restrict__ out, int64_t num_values) {
    auto writeEndOffset = out + num_values;

    // Process 16 bytes (8 values) a time.
    while (out + 8 <= writeEndOffset) {
        __m256i result = _mm256_cvtepu16_epi32(_mm_loadu_si128(reinterpret_cast<const __m128i*>(in)));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(out), result);

        in += 16;
        out += 8;
    }
}

// Unpack num_values number of uint32_t values with bit_width in [17, 21] range.
static inline void unpack17to21(const uint8_t* __restrict__ in, uint32_t* __restrict__ out, int64_t num_values,
                                int bit_width) {
    uint64_t pdepMask = kPdepMask32[bit_width];

    uint8_t rightShift1 = bit_width * 2;
    uint8_t leftShift1 = 64 - rightShift1;
    uint8_t rightShift2 = bit_width * 4 - 64;
    uint8_t rightShift3 = bit_width * 6 - 64;
    uint8_t leftShift3 = 128 - bit_width * 6;

    auto writeEndOffset = out + num_values;

    // Process bit_width bytes (8 values) a time.
    while (out + 8 <= writeEndOffset) {
        uint64_t value_0 = *(reinterpret_cast<const uint64_t*>(in));
        uint64_t value_1 = *(reinterpret_cast<const uint64_t*>(in + 8));
        uint64_t value_2 = *(reinterpret_cast<const uint64_t*>(in + 16));

        *reinterpret_cast<uint64_t*>(out) = _pdep_u64(value_0, pdepMask);
        *(reinterpret_cast<uint64_t*>(out) + 1) =
                _pdep_u64((value_0 >> rightShift1) | (value_1 << leftShift1), pdepMask);
        *(reinterpret_cast<uint64_t*>(out) + 2) = _pdep_u64(value_1 >> rightShift2, pdepMask);
        *(reinterpret_cast<uint64_t*>(out) + 3) =
                _pdep_u64((value_1 >> rightShift3) | (value_2 << leftShift3), pdepMask);

        in += bit_width;
        out += 8;
    }
}

// Unpack num_values number of uint32_t values with bit_width in [22, 31] range.
static inline void unpack22to31(const uint8_t* __restrict__ in, uint32_t* __restrict__ out, int64_t num_values,
                                int bit_width) {
    uint64_t pdepMask = kPdepMask32[bit_width];

    uint8_t rightShift1 = bit_width * 2;
    uint8_t leftShift1 = 64 - rightShift1;
    uint8_t rightShift2 = bit_width * 4 - 64;
    uint8_t leftShift2 = 2 * 64 - bit_width * 4;
    uint8_t rightShift3 = bit_width * 6 - 2 * 64;
    uint8_t leftShift3 = 3 * 64 - bit_width * 6;

    auto writeEndOffset = out + num_values;

    // Process bit_width bytes (8 values) a time.
    while (out + 8 <= writeEndOffset) {
        uint64_t value_0 = *(reinterpret_cast<const uint64_t*>(in));
        uint64_t value_1 = *(reinterpret_cast<const uint64_t*>(in + 8));
        uint64_t value_2 = *(reinterpret_cast<const uint64_t*>(in + 16));
        uint64_t value_3 = *(reinterpret_cast<const uint64_t*>(in + 24));

        *reinterpret_cast<uint64_t*>(out) = _pdep_u64(value_0, pdepMask);
        *(reinterpret_cast<uint64_t*>(out) + 1) =
                _pdep_u64((value_1 << leftShift1) | (value_0 >> rightShift1), pdepMask);
        *(reinterpret_cast<uint64_t*>(out) + 2) =
                _pdep_u64((value_2 << leftShift2) | (value_1 >> rightShift2), pdepMask);
        *(reinterpret_cast<uint64_t*>(out) + 3) =
                _pdep_u64((value_3 << leftShift3) | (value_2 >> rightShift3), pdepMask);

        in += bit_width;
        out += 8;
    }
}

// Unpack num_values number of uint32_t values with bit_width = 32
static inline void unpack32(const uint8_t* __restrict__ in, uint32_t* __restrict__ out, int64_t num_values) {
    uint64_t numBytes = num_values * 4;
    std::memcpy(out, in, numBytes);

    in += numBytes;
    out += num_values;
}

#endif

template <>
inline void unpack<uint8_t>(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
                            uint8_t* __restrict__ out) {
    DCHECK(bit_width >= 1 && bit_width <= 8);
    DCHECK(in_bytes * 8 >= bit_width * num_values);
    DCHECK(num_values % 8 == 0);

#if defined(__AVX2__) && defined(__BMI2__)

    uint64_t mask = kPdepMask8[bit_width];
    auto writeEndOffset = out + num_values;

    // Process bit_width bytes (8 values) a time. Note that for bit_width 8, the
    // performance of direct memcpy is about the same as this solution.
    while (out + 8 <= writeEndOffset) {
        // Using memcpy() here may result in non-optimized loops by clang.
        uint64_t val = *reinterpret_cast<const uint64_t*>(in);
        *(reinterpret_cast<uint64_t*>(out)) = _pdep_u64(val, mask);
        in += bit_width;
        out += 8;
    }

#else

    unpackNaive<uint8_t>(bit_width, in, in_bytes, num_values, out);

#endif
}

template <>
inline void unpack<uint16_t>(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
                             uint16_t* __restrict__ out) {
    DCHECK(bit_width >= 1 && bit_width <= 16);
    DCHECK(in_bytes * 8 >= bit_width * num_values);
    DCHECK(num_values % 8 == 0);

#if defined(__AVX2__) && defined(__BMI2__)

    switch (bit_width) {
    case 1:
    case 2:
    case 3:
    case 4:
    case 5:
    case 6:
    case 7:
        unpack1to7(in, out, num_values, bit_width);
        break;
    case 8:
        unpack8(in, out, num_values);
        break;
    case 9:
    case 11:
    case 13:
    case 15:
    case 10:
    case 12:
    case 14:
        unpack9to15(in, out, num_values, bit_width);
        break;
    case 16:
        unpack16(in, out, num_values);
        break;
    default:
        DCHECK(false) << "invalid bit_width";
    }

#else

    unpackNaive<uint16_t>(bit_width, in, in_bytes, out, num_values);

#endif
}

template <>
inline void unpack<uint32_t>(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
                             uint32_t* __restrict__ out) {
    DCHECK(bit_width >= 1 && bit_width <= 32);
    DCHECK(in_bytes * 8 >= bit_width * num_values);
    DCHECK(num_values % 8 == 0);

#if defined(__AVX2__) && defined(__BMI2__)

    switch (bit_width) {
    case 1:
    case 2:
    case 3:
    case 4:
    case 5:
    case 6:
    case 7:
        unpack1to7(in, out, num_values, bit_width);
        break;
    case 8:
        unpack8(in, out, num_values);
        break;
    case 9:
    case 10:
    case 11:
    case 12:
    case 13:
    case 14:
    case 15:
        unpack9to15(in, out, num_values, bit_width);
        break;
    case 16:
        unpack16(in, out, num_values);
        break;
    case 17:
    case 18:
    case 19:
    case 20:
    case 21:
        unpack17to21(in, out, num_values, bit_width);
        break;
    case 22:
    case 23:
    case 24:
    case 25:
    case 26:
    case 27:
    case 28:
    case 29:
    case 30:
    case 31:
        unpack22to31(in, out, num_values, bit_width);
        break;
    case 32:
        unpack32(in, out, num_values);
        break;
    default:
        DCHECK("invalid bit_width");
    }

#else

    unpackNaive<uint32_t>(bit_width, in, in_bytes, num_values, out);

#endif
}

} // namespace starrocks::util
