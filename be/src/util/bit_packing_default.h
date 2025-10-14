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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/bit_packing.inline.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// the implement of BitPacking is from impala

#pragma once

#include <boost/preprocessor/repetition/repeat_from_to.hpp>
#include <cstdint>
#include <utility>

#include "util/bit_util.h"

namespace starrocks::util::bitpacking_default {

// Utilities for manipulating bit-packed values. Bit-packing is a technique for
// compressing integer values that do not use the full range of the integer type.
// E.g. an array of uint32_t values with range [0, 31] only uses the lower 5 bits
// of every uint32_t value, or an array of 0/1 booleans only uses the lowest bit
// of each integer.
//
// Bit-packing always has a "bit width" parameter that determines the range of
// representable unsigned values: [0, 2^bit_width - 1]. The packed representation
// is logically the concatenatation of the lower bits of the input values (in
// little-endian order). E.g. the values 1, 2, 3, 4 packed with bit width 4 results
// in the two output bytes: [ 0 0 1 0 | 0 0 0 1 ] [ 0 1 0 0 | 0 0 1 1 ]
//                               2         1           4         3
//
// Packed values can be split across words, e.g. packing 1, 17 with bit_width 5 results
// in the two output bytes: [ 0 0 1 | 0 0 0 0 1 ] [ x x x x x x | 1 0 ]
//            lower bits of 17--^         1         next value     ^--upper bits of 17
//
// Bit widths from 0 to 64 are supported (0 bit width means that every value is 0).
// The batched unpacking functions operate on batches of 32 values. This batch size
// is convenient because for every supported bit width, the end of a 32 value batch
// falls on a byte boundary. It is also large enough to amortise loop overheads.

static constexpr int MAX_BITWIDTH = sizeof(uint64_t) * 8;

/// Compute the number of values with the given bit width that can be unpacked from
/// an input buffer of 'in_bytes' into an output buffer with space for 'num_values'.

static inline int64_t NumValuesToUnpack(int bit_width, int64_t in_bytes, int64_t num_values) {
    // Check if we have enough input bytes to decode 'num_values'.
    if (bit_width == 0 || BitUtil::RoundUpNumBytes(num_values * bit_width) <= in_bytes) {
        // Limited by output space.
        return num_values;
    }

    // Limited by the number of input bytes. Compute the number of values that can be
    // unpacked from the input.
    return (in_bytes * CHAR_BIT) / bit_width;
}

// Loop body of unrolled loop that unpacks the value. BIT_WIDTH is the bit width of
// the packed values. 'in_buf' is the start of the input buffer and 'out_vals' is the
// start of the output values array. This function unpacks the VALUE_IDX'th packed value
// from 'in_buf'.
//
// This implements essentially the same algorithm as the (Apache-licensed) code in
// bpacking.c at https://github.com/lemire/FrameOfReference/, but is much more compact
// because it uses templates rather than source-level unrolling of all combinations.
//
// After the template parameters are expanded and constants are propagated, all branches
// and offset/shift calculations should be optimized out, leaving only shifts by constants
// and bitmasks by constants. Calls to this must be stamped out manually or with
// BOOST_PP_REPEAT_FROM_TO: experimentation revealed that the GCC 4.9.2 optimiser was
// not able to fully propagate constants and remove branches when this was called from
// inside a for loop with constant bounds with VALUE_IDX changed to a function argument.
//
// We compute how many 32 bit words we have to read, which is either 1, 2 or 3. If it is
// at least 2, the first two 32 bit words are read as one 64 bit word. Even if only one
// word needs to be read, we try to read 64 bits if it does not lead to buffer overflow
// because benchmarks show that it has a positive effect on performance.
//
// If 'FULL_BATCH' is true, this function call is part of unpacking 32 values, otherwise
// up to 31 values. This is needed to optimise the length of the reads (32 or 64 bits) and
// avoid buffer overflow (if we are unpacking 32 values, we can safely assume an input
// buffer of length 32 * BIT_WIDTH).
template <int BIT_WIDTH, int VALUE_IDX, bool FULL_BATCH>
inline uint64_t ALWAYS_INLINE UnpackValue(const uint8_t* __restrict__ in_buf) {
    if (BIT_WIDTH == 0) return 0;

    constexpr int FIRST_BIT_IDX = VALUE_IDX * BIT_WIDTH;
    constexpr int FIRST_WORD_IDX = FIRST_BIT_IDX / 32;
    constexpr int LAST_BIT_IDX = FIRST_BIT_IDX + BIT_WIDTH;
    constexpr int LAST_WORD_IDX = BitUtil::round_up_numi32(LAST_BIT_IDX);
    constexpr int WORDS_TO_READ = LAST_WORD_IDX - FIRST_WORD_IDX;

    constexpr int FIRST_BIT_OFFSET = FIRST_BIT_IDX - FIRST_WORD_IDX * 32;
    constexpr uint64_t mask = []() {
        if constexpr (BIT_WIDTH == 64) {
            return ~0UL;
        } else {
            return (1UL << BIT_WIDTH) - 1;
        }
    }();
    const auto* const in = reinterpret_cast<const uint32_t*>(in_buf);

    // Avoid reading past the end of the buffer. We can safely read 64 bits if we know that
    // this is a full batch read (so the input buffer is 32 * BIT_WIDTH long) and there is
    // enough space in the buffer from the current reading point.
    // We try to read 64 bits even when it is not necessary because the benchmarks show it
    // is faster.
    constexpr bool CAN_SAFELY_READ_64_BITS = FULL_BATCH && FIRST_BIT_IDX - FIRST_BIT_OFFSET + 64 <= BIT_WIDTH * 32;

    // We do not try to read 64 bits when the bit width is a power of two (unless it is
    // necessary) because performance benchmarks show that it is better this way. This seems
    // to be due to compiler optimisation issues, so we can revisit it when we update the
    // compiler version.
    constexpr bool READ_32_BITS = WORDS_TO_READ == 1 && (!CAN_SAFELY_READ_64_BITS || BitUtil::IsPowerOf2(BIT_WIDTH));

    if (READ_32_BITS) {
        uint32_t word = in[FIRST_WORD_IDX];
        word >>= FIRST_BIT_OFFSET < 32 ? FIRST_BIT_OFFSET : 0;
        return word & mask;
    }

    uint64_t word = *reinterpret_cast<const uint64_t*>(in + FIRST_WORD_IDX);
    word >>= FIRST_BIT_OFFSET;

    if (WORDS_TO_READ > 2) {
        constexpr int USEFUL_BITS = FIRST_BIT_OFFSET == 0 ? 0 : 64 - FIRST_BIT_OFFSET;
        uint64_t extra_word = in[FIRST_WORD_IDX + 2];
        word |= extra_word << USEFUL_BITS;
    }

    return word & mask;
}

/// Same as Unpack32Values() but templated by BIT_WIDTH.

template <typename OutType, int BIT_WIDTH>
const uint8_t* Unpack32Values(const uint8_t* __restrict__ in, int64_t in_bytes, OutType* __restrict__ out) {
    constexpr int BYTES_TO_READ = BitUtil::RoundUpNumBytes(32 * BIT_WIDTH);

    // Call UnpackValue for 0 <= i < 32.
#pragma push_macro("UNPACK_VALUE_CALL")
#define UNPACK_VALUE_CALL(ignore1, i, ignore2) out[i] = static_cast<OutType>(UnpackValue<BIT_WIDTH, i, true>(in));

    BOOST_PP_REPEAT_FROM_TO(0, 32, UNPACK_VALUE_CALL, ignore);
    return in + BYTES_TO_READ;
#pragma pop_macro("UNPACK_VALUE_CALL")
}

/// Unpacks 'num_values' values with the given BIT_WIDTH from 'in' to 'out'.
/// 'num_values' must be at most 31. 'in' must point to 'in_bytes' of addressable
/// memory, and 'in_bytes' must be at least ceil(num_values * bit_width / 8).
/// 'out' must have space for 'num_values' OutType values.
/// 0 <= 'bit_width' <= 64 and 'bit_width' <= # of bits in OutType.

template <typename OutType, int BIT_WIDTH>
const uint8_t* UnpackUpTo31Values(const uint8_t* __restrict__ in, int64_t in_bytes, int num_values,
                                  OutType* __restrict__ out) {
    constexpr int MAX_BATCH_SIZE = 31;
    const int BYTES_TO_READ = BitUtil::RoundUpNumBytes(num_values * BIT_WIDTH);

    // Make sure the buffer is at least 1 byte.
    constexpr int TMP_BUFFER_SIZE = BIT_WIDTH ? (BIT_WIDTH * (MAX_BATCH_SIZE + 1)) / CHAR_BIT : 1;
    uint8_t tmp_buffer[TMP_BUFFER_SIZE];

    const uint8_t* in_buffer = in;
    // Copy into padded temporary buffer to avoid reading past the end of 'in' if the
    // last 32-bit load would go past the end of the buffer.
    if (BitUtil::round_up(BYTES_TO_READ, sizeof(uint32_t)) > in_bytes) {
        memcpy(tmp_buffer, in, BYTES_TO_READ);
        in_buffer = tmp_buffer;
    }

#pragma push_macro("UNPACK_VALUES_CASE")
#define UNPACK_VALUES_CASE(ignore1, i, ignore2) \
    case 31 - i:                                \
        out[30 - i] = static_cast<OutType>(UnpackValue<BIT_WIDTH, 30 - i, false>(in_buffer));

    // Use switch with fall-through cases to minimise branching.
    switch (num_values) {
        // Expand cases from 31 down to 1.
        BOOST_PP_REPEAT_FROM_TO(0, 31, UNPACK_VALUES_CASE, ignore);
    case 0:
        break;
    default:
        DCHECK(false);
    }
    return in + BYTES_TO_READ;
#pragma pop_macro("UNPACK_VALUES_CASE")
}

template <typename OutType, int BIT_WIDTH>
std::pair<const uint8_t*, int64_t> UnpackValues(const uint8_t* __restrict__ in, int64_t in_bytes, int64_t num_values,
                                                OutType* __restrict__ out) {
    constexpr int BATCH_SIZE = 32;
    const int64_t values_to_read = NumValuesToUnpack(BIT_WIDTH, in_bytes, num_values);
    const int64_t batches_to_read = values_to_read / BATCH_SIZE;
    const int64_t remainder_values = values_to_read % BATCH_SIZE;
    const uint8_t* in_pos = in;
    OutType* out_pos = out;

    // First unpack as many full batches as possible.
    for (int64_t i = 0; i < batches_to_read; ++i) {
        in_pos = Unpack32Values<OutType, BIT_WIDTH>(in_pos, in_bytes, out_pos);
        out_pos += BATCH_SIZE;
        in_bytes -= (BATCH_SIZE * BIT_WIDTH) / CHAR_BIT;
    }

    // Then unpack the final partial batch.
    if (remainder_values > 0) {
        in_pos = UnpackUpTo31Values<OutType, BIT_WIDTH>(in_pos, in_bytes, remainder_values, out_pos);
    }
    return std::make_pair(in_pos, values_to_read);
}

// Unpack bit-packed values with 'bit_width' from 'in' to 'out'. Keeps unpacking until
// either all 'in_bytes' are read or 'num_values' values are unpacked. 'out' must have
// enough space for 'num_values'. 0 <= 'bit_width' <= 64 and 'bit_width' <= # of bits
// in OutType. 'in' must point to 'in_bytes' of addressable memory.
//
// Returns a pointer to the byte after the last byte of 'in' that was read and also the
// number of values that were read. If the caller wants to continue reading packed
// values after the last one returned, it must ensure that the next value to unpack
// starts at a byte boundary. This is true if 'num_values' is a multiple of 32, or
// more generally if (bit_width * num_values) % 8 == 0.

template <typename OutType>
std::pair<const uint8_t*, int64_t> UnpackValues(int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes,
                                                int64_t num_values, OutType* __restrict__ out) {
#pragma push_macro("UNPACK_VALUES_CASE")
#define UNPACK_VALUES_CASE(ignore1, i, ignore2) \
    case i:                                     \
        return UnpackValues<OutType, i>(in, in_bytes, num_values, out);

    switch (bit_width) {
        // Expand cases from 0 to 64.
        BOOST_PP_REPEAT_FROM_TO(0, 65, UNPACK_VALUES_CASE, ignore);
    default:
        DCHECK(false);
        return std::make_pair(nullptr, -1);
    }
#pragma pop_macro("UNPACK_VALUES_CASE")
}

} // namespace starrocks::util::bitpacking_default
