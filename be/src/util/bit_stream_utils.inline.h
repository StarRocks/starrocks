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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/bit_stream_utils.inline.h

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
#pragma once

#include <algorithm>

#include "glog/logging.h"
#include "util/alignment.h"
#include "util/bit_packing.inline.h"
#include "util/bit_stream_utils.h"

using starrocks::BitUtil;

namespace starrocks {

inline void BitWriter::PutValue(uint64_t v, int num_bits) {
    DCHECK_LE(num_bits, 64);
    // Truncate the higher-order bits. This is necessary to
    // support signed values.
    v &= ~0ULL >> (64 - num_bits);

    buffered_values_ |= v << bit_offset_;
    bit_offset_ += num_bits;

    if (PREDICT_FALSE(bit_offset_ >= 64)) {
        // Flush buffered_values_ and write out bits of v that did not fit
        buffer_->reserve(ALIGN_UP(byte_offset_ + 8, 8));
        buffer_->resize(byte_offset_ + 8);
        DCHECK_LE(byte_offset_ + 8, buffer_->capacity());
        memcpy(buffer_->data() + byte_offset_, &buffered_values_, 8);
        buffered_values_ = 0;
        byte_offset_ += 8;
        bit_offset_ -= 64;
        buffered_values_ = BitUtil::ShiftRightZeroOnOverflow(v, (num_bits - bit_offset_));
    }
    DCHECK_LT(bit_offset_, 64);
}

inline void BitWriter::Flush(bool align) {
    int num_bytes = BitUtil::Ceil(bit_offset_, 8);
    buffer_->reserve(ALIGN_UP(byte_offset_ + num_bytes, 8));
    buffer_->resize(byte_offset_ + num_bytes);
    DCHECK_LE(byte_offset_ + num_bytes, buffer_->capacity());
    memcpy(buffer_->data() + byte_offset_, &buffered_values_, num_bytes);

    if (align) {
        buffered_values_ = 0;
        byte_offset_ += num_bytes;
        bit_offset_ = 0;
    }
}

inline uint8_t* BitWriter::GetNextBytePtr(int num_bytes) {
    Flush(/* align */ true);
    buffer_->reserve(ALIGN_UP(byte_offset_ + num_bytes, 8));
    buffer_->resize(byte_offset_ + num_bytes);
    uint8_t* ptr = buffer_->data() + byte_offset_;
    byte_offset_ += num_bytes;
    DCHECK_LE(byte_offset_, buffer_->capacity());
    return ptr;
}

template <typename T>
inline void BitWriter::PutAligned(T val, int num_bytes) {
    DCHECK_LE(num_bytes, sizeof(T));
    uint8_t* ptr = GetNextBytePtr(num_bytes);
    memcpy(ptr, &val, num_bytes);
}

inline void BitWriter::PutVlqInt(uint32_t v) {
    [[maybe_unused]] int num_bytes = 0;
    while ((v & 0xFFFFFF80) != 0L) {
        PutAligned<uint8_t>((v & 0x7F) | 0x80, 1);
        v >>= 7;
        DCHECK_LE(++num_bytes, MAX_VLQ_BYTE_LEN);
    }
    PutAligned<uint8_t>(v & 0x7F, 1);
}

inline BitReader::BitReader(const uint8_t* buffer, int buffer_len) : buffer_(buffer), max_bytes_(buffer_len) {
    int num_bytes = std::min(8, max_bytes_);
    memcpy(&buffered_values_, buffer_ + byte_offset_, num_bytes);
}

inline void BitReader::BufferValues() {
    int bytes_remaining = max_bytes_ - byte_offset_;
    if (PREDICT_TRUE(bytes_remaining >= 8)) {
        memcpy(&buffered_values_, buffer_ + byte_offset_, 8);
    } else {
        memcpy(&buffered_values_, buffer_ + byte_offset_, bytes_remaining);
    }
}

template <typename T>
inline bool BitReader::GetValue(int num_bits, T* v) {
    DCHECK_LE(num_bits, 64);
    DCHECK_LE(num_bits, sizeof(T) * 8);

    if (PREDICT_FALSE(byte_offset_ * 8 + bit_offset_ + num_bits > max_bytes_ * 8)) return false;

    *v = BitUtil::TrailingBits(buffered_values_, bit_offset_ + num_bits) >> bit_offset_;

    bit_offset_ += num_bits;
    if (bit_offset_ >= 64) {
        byte_offset_ += 8;
        bit_offset_ -= 64;
        BufferValues();
        // Read bits of v that crossed into new buffered_values_
        *v |= BitUtil::ShiftLeftZeroOnOverflow(BitUtil::TrailingBits(buffered_values_, bit_offset_),
                                               (num_bits - bit_offset_));
    }
    DCHECK_LE(bit_offset_, 64);
    return true;
}

inline void BitReader::Rewind(int num_bits) {
    bit_offset_ -= num_bits;
    if (bit_offset_ >= 0) {
        return;
    }
    while (bit_offset_ < 0) {
        int seek_back = std::min(byte_offset_, 8);
        byte_offset_ -= seek_back;
        bit_offset_ += seek_back * 8;
    }
    // This should only be executed *if* rewinding by 'num_bits'
    // make the existing buffered_values_ invalid
    DCHECK_GE(byte_offset_, 0); // Check for underflow
    memcpy(&buffered_values_, buffer_ + byte_offset_, 8);
}

inline void BitReader::SeekToBit(uint stream_position) {
    DCHECK_LE(stream_position, max_bytes_ * 8);

    int delta = static_cast<int>(stream_position) - position();
    if (delta == 0) {
        return;
    } else if (delta < 0) {
        Rewind(position() - stream_position);
    } else {
        bit_offset_ += delta;
        while (bit_offset_ >= 64) {
            byte_offset_ += 8;
            bit_offset_ -= 64;
            if (bit_offset_ < 64) {
                // This should only be executed if seeking to
                // 'stream_position' makes the existing buffered_values_
                // invalid.
                BufferValues();
            }
        }
    }
}

template <typename T>
inline bool BitReader::GetAligned(int num_bytes, T* v) {
    DCHECK_LE(num_bytes, sizeof(T));
    int bytes_read = BitUtil::Ceil(bit_offset_, 8);
    if (PREDICT_FALSE(byte_offset_ + bytes_read + num_bytes > max_bytes_)) return false;

    // Advance byte_offset to next unread byte and read num_bytes
    byte_offset_ += bytes_read;
    memcpy(v, buffer_ + byte_offset_, num_bytes);
    byte_offset_ += num_bytes;

    // Reset buffered_values_
    bit_offset_ = 0;
    int bytes_remaining = max_bytes_ - byte_offset_;
    if (PREDICT_TRUE(bytes_remaining >= 8)) {
        memcpy(&buffered_values_, buffer_ + byte_offset_, 8);
    } else {
        memcpy(&buffered_values_, buffer_ + byte_offset_, bytes_remaining);
    }
    return true;
}

inline bool BitReader::GetVlqInt(uint32_t* v) {
    *v = 0;
    int shift = 0;
    [[maybe_unused]] int num_bytes = 0;
    uint8_t byte = 0;
    do {
        if (!GetAligned<uint8_t>(1, &byte)) return false;
        *v |= (byte & 0x7F) << shift;
        shift += 7;
        DCHECK_LE(++num_bytes, MAX_VLQ_BYTE_LEN);
    } while ((byte & 0x80) != 0);
    return true;
}

template <typename UINT_T>
inline bool BatchedBitReader::get_lleb_128(UINT_T* v) {
    static_assert(std::is_integral<UINT_T>::value, "Integral type required.");
    static_assert(std::is_unsigned<UINT_T>::value, "Unsigned type required.");
    static_assert(!std::is_same<UINT_T, bool>::value, "Bools are not supported.");

    *v = 0;
    int shift = 0;
    uint8_t byte = 0;
    do {
        if (UNLIKELY(shift >= _max_vlq_byte_len<UINT_T>() * 7)) return false;
        if (!get_bytes(1, &byte)) return false;

        /// We need to convert 'byte' to UINT_T so that the result of the bitwise and
        /// operation is at least as long an integer as '*v', otherwise the shift may be too
        /// big and lead to undefined behaviour.
        const UINT_T byte_as_UINT_T = byte;
        *v |= (byte_as_UINT_T & 0x7Fu) << shift;
        shift += 7;
    } while ((byte & 0x80u) != 0);
    return true;
}

template <typename T>
inline bool BatchedBitReader::get_bytes(int num_bytes, T* v) {
    if (UNLIKELY(_buffer_pos + num_bytes > _buffer_end)) {
        return false;
    }
    *v = 0; // Ensure unset bytes are initialized to zero.
    memcpy(v, _buffer_pos, num_bytes);
    _buffer_pos += num_bytes;
    return true;
}

template <typename T>
inline int BatchedBitReader::unpack_batch(int bit_width, int num_values, T* v) {
    int64_t num_read;
    std::tie(_buffer_pos, num_read) = BitPacking::UnpackValues(bit_width, _buffer_pos, _bytes_left(), num_values, v);
    return static_cast<int>(num_read);
}

} // namespace starrocks
