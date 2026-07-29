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

// Shared low-level helpers for the spill codec implementations: bounds checking,
// zigzag, and the FOR-style bit-packed block used by the int and string codecs.
//
// The bit-packed block is deliberately built on the word-buffered BitWriter (a few ops
// per value) instead of base/bit's ForEncoder, whose bit_pack inner loop runs PER BIT
// (~400 ops/value at 64-bit width) -- fine for storage page builds, disqualifying on
// the spill hot path.

#include <cstring>
#include <limits>

#include "base/bit/bit_stream_utils.h"
#include "base/bit/bit_stream_utils.inline.h"
#include "base/bit/frame_of_reference_coding.h" // bits<T>()
#include "base/string/faststring.h"
#include "common/statusor.h"
#include "gutil/port.h"

namespace starrocks::spill {

inline Status codec_check_remaining(const uint8_t* buf, const uint8_t* end, size_t need) {
    // Compare on the non-negative gap (end >= buf is an invariant) rather than `buf + need`:
    // forming a pointer past the end of the allocation is UB, and a huge `need` could even wrap.
    if (UNLIKELY(need > static_cast<size_t>(end - buf))) {
        return Status::Corruption("spill codec payload truncated");
    }
    return Status::OK();
}

// Upper sanity bound on a stream-provided element/row count before it drives an allocation.
// RLE/dictionary/bit-packed encodings can legitimately represent many rows in very few bytes,
// so a count cannot be bounded by the remaining buffer size; this fixed ceiling exists solely
// to reject a corrupt/garbage count (e.g. a bit-flipped u32 read as billions) before it triggers
// a multi-GB resize. A real spilled chunk holds at most a few thousand rows -- orders of
// magnitude below this -- so a valid stream is never rejected.
inline constexpr uint64_t kMaxDecodeCount = 1ull << 28; // 268M
inline Status codec_check_count(uint64_t count) {
    if (UNLIKELY(count > kMaxDecodeCount)) {
        return Status::Corruption("spill codec decode count implausibly large (corrupt payload)");
    }
    return Status::OK();
}

template <typename UT>
UT zigzag_encode(UT v) {
    using ST = std::make_signed_t<UT>;
    auto s = static_cast<ST>(v);
    return (static_cast<UT>(s) << 1) ^ static_cast<UT>(s >> (sizeof(ST) * 8 - 1));
}

template <typename UT>
UT zigzag_decode(UT v) {
    return (v >> 1) ^ (~(v & 1) + 1);
}

// FOR-style block: [u8 bw][UT min][u32 packed_bytes][n*bw bits]
template <typename UT>
uint8_t* bitpack_block_encode(const UT* v, size_t n, uint8_t* buf) {
    UT min_v = std::numeric_limits<UT>::max();
    UT max_v = 0;
    for (size_t i = 0; i < n; ++i) {
        min_v = std::min(min_v, v[i]);
        max_v = std::max(max_v, v[i]);
    }
    if (n == 0) {
        min_v = 0;
        max_v = 0;
    }
    const int bw = bits<UT>(max_v - min_v);
    buf[0] = static_cast<uint8_t>(bw);
    memcpy(buf + 1, &min_v, sizeof(UT));
    buf += 1 + sizeof(UT);

    thread_local faststring packed;
    BitWriter writer(&packed); // ctor clears the buffer
    if (bw > 0) {
        packed.reserve(n * sizeof(UT) + 16);
        for (size_t i = 0; i < n; ++i) {
            writer.PutValue(static_cast<uint64_t>(v[i] - min_v), bw);
        }
        writer.Flush();
    }
    const auto packed_bytes = static_cast<uint32_t>(writer.bytes_written());
    UNALIGNED_STORE32(buf, packed_bytes);
    memcpy(buf + 4, packed.data(), packed_bytes);
    return buf + 4 + packed_bytes;
}

template <typename UT>
StatusOr<const uint8_t*> bitpack_block_decode(const uint8_t* buf, const uint8_t* end, UT* out, size_t n) {
    RETURN_IF_ERROR(codec_check_remaining(buf, end, 1 + sizeof(UT) + 4));
    const int bw = buf[0];
    UT min_v;
    memcpy(&min_v, buf + 1, sizeof(UT));
    buf += 1 + sizeof(UT);
    uint32_t packed_bytes = UNALIGNED_LOAD32(buf);
    buf += 4;
    RETURN_IF_ERROR(codec_check_remaining(buf, end, packed_bytes));
    if (bw == 0) {
        for (size_t i = 0; i < n; ++i) out[i] = min_v;
    } else {
        BitReader reader(buf, packed_bytes);
        if (!reader.GetBatch(bw, out, static_cast<int>(n))) {
            return Status::Corruption("spill bitpack block unpack failed");
        }
        for (size_t i = 0; i < n; ++i) out[i] += min_v;
    }
    return buf + packed_bytes;
}

// worst-case size of a bitpack block over n values of type UT
template <typename UT>
int64_t bitpack_block_max_size(size_t n) {
    return 1 + sizeof(UT) + 4 + static_cast<int64_t>(n) * sizeof(UT) + 16;
}

} // namespace starrocks::spill
