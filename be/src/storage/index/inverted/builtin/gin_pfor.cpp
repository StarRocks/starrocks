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

#include "storage/index/inverted/builtin/gin_pfor.h"

#include <glog/logging.h>

#include "base/string/faststring.h"

namespace starrocks::gin_pfor {

namespace {

// Maximum number of values a single encode()/decode() call may handle: the exception position and
// the exception count are both u8 in the frozen format, so a stream cannot represent more than this
// many values. The GIN block size (128) is well under this bound.
constexpr size_t kMaxValues = 255;

// Mask of the low `b` bits (0xFFFFFFFF for b == 32). The bit-packing loops OR whole values into a
// shift accumulator, so the high bits above `b` must be cleared first or they would bleed into the
// next value's slot. b is always in [0, 32] here.
inline uint32_t low_mask(int b) {
    return b >= 32 ? 0xFFFFFFFFu : ((1u << b) - 1u);
}

// Bit-length of v: number of bits needed to represent it (0 for v == 0).
inline int bit_length(uint32_t v) {
    return v == 0 ? 0 : (32 - __builtin_clz(v));
}

// Append `v` to `out` as LEB128 (identical bytes to the codebase's encode_varint32, kept inline so
// the codec depends only on faststring and stays a self-contained frozen format).
inline void put_varint(faststring* out, uint32_t v) {
    while (v >= 0x80) {
        out->push_back(static_cast<char>(v | 0x80));
        v >>= 7;
    }
    out->push_back(static_cast<char>(v));
}

// Read one LEB128 value from [p, limit). Returns the pointer past it, or nullptr if truncated or if
// the encoding does not fit in a uint32 (malformed input): either too many bytes, or a 5th byte
// whose high bits would overflow bit 31.
inline const uint8_t* get_varint(const uint8_t* p, const uint8_t* limit, uint32_t* out) {
    uint32_t result = 0;
    int shift = 0;
    while (p < limit) {
        uint8_t byte = *p++;
        // The 5th byte (shift == 28) of a uint32 LEB128 may only set the low 4 bits; a value above
        // 0x0F would push bits past bit 31. Reject such an oversized/non-canonical encoding from
        // corrupt persisted bytes instead of silently truncating it to a wrong value (the shift
        // below drops the overflowed bits). Canonical UINT32_MAX is FF FF FF FF 0F.
        if (shift == 28 && (byte & 0x7Fu) > 0x0Fu) return nullptr;
        result |= static_cast<uint32_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) {
            *out = result;
            return p;
        }
        shift += 7;
        if (shift > 28) return nullptr; // a uint32 LEB128 is at most 5 bytes
    }
    return nullptr;
}

// Result of the writer-side width search: the chosen low-bit width plus the number of values that
// overflow it and become exceptions. The count falls out of the same histogram, so the caller needs
// no second pass over the values to recompute it.
struct ChosenWidth {
    int bit_width;
    uint8_t num_exceptions;
};

// Pick the low-bit width that minimizes the encoded size, using the bit-length histogram method
// (same family as Lucene PForUtil's freqs[] and Doris/TurboPFor's _p4bits): the encoded size depends
// only on the distribution of value bit-lengths, not on the values themselves. So summarize that
// distribution once in O(n), then evaluate every candidate width against the histogram instead of
// rescanning all values per width. Produces the exact same width (hence the same bytes) as a brute
// per-width rescan, in O(n + W^2) instead of O(n*W), W = 33.
//
// This is a writer-side heuristic only; it is NOT part of the on-disk format and can be retuned
// freely without breaking old segments.
ChosenWidth choose_bit_width(const uint32_t* vals, size_t n) {
    // hist[k] = number of values whose bit-length is exactly k (k in 0..32).
    int hist[33] = {0};
    int maxbits = 0;
    for (size_t i = 0; i < n; ++i) {
        int bl = bit_length(vals[i]);
        ++hist[bl];
        if (bl > maxbits) maxbits = bl;
    }

    int best_b = 0;
    size_t best_exc = 0;
    size_t best_cost = static_cast<size_t>(-1);
    for (int b = 0; b <= maxbits; ++b) {
        // A value with bit-length k > b overflows b low bits and becomes an exception costing
        // 1 (pos) + varint_len(high) bytes; high has (k - b) bits so varint_len = ceil((k-b)/7).
        size_t exc_bytes = 0;
        size_t exc_count = 0;
        for (int k = b + 1; k <= maxbits; ++k) {
            exc_bytes += static_cast<size_t>(hist[k]) * (1 + ((k - b) + 6) / 7);
            exc_count += static_cast<size_t>(hist[k]);
        }
        size_t stream_bytes = (n * static_cast<size_t>(b) + 7) / 8;
        size_t cost = 2 /*header*/ + exc_bytes + stream_bytes;
        if (cost < best_cost) {
            best_cost = cost;
            best_b = b;
            best_exc = exc_count;
        }
    }
    // num_exceptions <= n <= kMaxValues (255), so it always fits in u8.
    return {best_b, static_cast<uint8_t>(best_exc)};
}

} // namespace

void encode(const uint32_t* vals, size_t n, faststring* out) {
    DCHECK_LE(n, kMaxValues); // pos/num_exceptions are u8; the caller always passes n <= block size (128)

    const ChosenWidth chosen = choose_bit_width(vals, n);
    const int b = chosen.bit_width;
    const uint8_t num_exc = chosen.num_exceptions;

    out->push_back(static_cast<char>(b));
    out->push_back(static_cast<char>(num_exc));

    // exception table: {pos:u8, high:varint} in increasing position order
    for (size_t i = 0; i < n; ++i) {
        uint32_t high = (b >= 32) ? 0u : (vals[i] >> b);
        if (high != 0) {
            out->push_back(static_cast<char>(i));
            put_varint(out, high);
        }
    }

    // low stream: n values, b bits each, LSB-first. Pack through a 64-bit shift accumulator and emit
    // whole bytes, rather than setting one bit at a time. `acc_bits` is kept < 8 before each value
    // and b <= 32, so `acc` never holds more than 39 live bits; every output byte is written exactly
    // once, so no pre-zeroing is needed. This yields the identical byte stream, ~10x faster.
    const size_t stream_bytes = (n * static_cast<size_t>(b) + 7) / 8;
    if (stream_bytes > 0) {
        const size_t start = out->size();
        out->resize(start + stream_bytes);
        uint8_t* p = out->data() + start;
        const uint32_t mask = low_mask(b);
        uint64_t acc = 0;
        int acc_bits = 0;
        for (size_t i = 0; i < n; ++i) {
            acc |= static_cast<uint64_t>(vals[i] & mask) << acc_bits;
            acc_bits += b;
            while (acc_bits >= 8) {
                *p++ = static_cast<uint8_t>(acc);
                acc >>= 8;
                acc_bits -= 8;
            }
        }
        if (acc_bits > 0) *p++ = static_cast<uint8_t>(acc); // trailing partial byte
    }
}

size_t decode(const uint8_t* data, size_t len, size_t n, uint32_t* out) {
    if (len < 2) return 0;        // need at least the 2-byte header
    if (n > kMaxValues) return 0; // more values than the u8 exception pos/count header can represent
    const uint8_t* p = data;
    const uint8_t* const limit = data + len;

    const int b = *p++;
    const uint8_t num_exc = *p++;
    if (b > 32) return 0; // malformed bit width
    // b == 32 means every value already fits in 32 low bits, so a well-formed block has no
    // exceptions. Reject b == 32 with exceptions from corrupt input so the patch step below never
    // evaluates (high << 32), a shift by the full type width (undefined behavior).
    if (b == 32 && num_exc > 0) return 0;

    uint8_t exc_pos[256];
    uint32_t exc_high[256];
    for (int e = 0; e < num_exc; ++e) {
        if (p >= limit) return 0;
        exc_pos[e] = *p++;
        // Reject an out-of-range exception position from corrupt persisted bytes before it is used
        // to index out[0..n) in the patch step below (otherwise an out-of-bounds write).
        if (exc_pos[e] >= n) return 0;
        const uint8_t* np = get_varint(p, limit, &exc_high[e]);
        if (np == nullptr) return 0;
        p = np;
        // Reject an exception high that does not fit in the (32 - b) high bits from corrupt persisted
        // bytes. b is always < 32 here (b == 32 with exceptions was rejected above), so otherwise the
        // patch step (high << b) below would silently drop the overflowed bits and decode a wrong
        // value. (b == 0 gives mask 0xFFFFFFFF, so a legitimately encoded high never trips this.)
        if (exc_high[e] > (0xFFFFFFFFu >> b)) return 0;
    }

    const size_t stream_bytes = (n * static_cast<size_t>(b) + 7) / 8;
    if (static_cast<size_t>(limit - p) < stream_bytes) return 0;
    const uint8_t* s = p;
    // Mirror of the packing loop: refill a 64-bit accumulator a whole byte at a time and emit the low
    // b bits per value. Reads exactly stream_bytes bytes (bounds-checked above). b == 0 yields
    // mask == 0, so every value reads as 0 without consuming any stream bytes.
    const uint32_t mask = low_mask(b);
    uint64_t acc = 0;
    int acc_bits = 0;
    for (size_t i = 0; i < n; ++i) {
        while (acc_bits < b) {
            acc |= static_cast<uint64_t>(*s++) << acc_bits;
            acc_bits += 8;
        }
        out[i] = static_cast<uint32_t>(acc & mask);
        acc >>= b;
        acc_bits -= b;
    }
    p += stream_bytes;

    // apply patches; the checks above guarantee b < 32 whenever num_exc > 0 and every exc_high fits
    // in the (32 - b) high bits (so (high << b) is well-defined and does not overflow), and every
    // exc_pos < n (so the index stays in bounds).
    for (int e = 0; e < num_exc; ++e) {
        out[exc_pos[e]] |= (exc_high[e] << b);
    }

    return static_cast<size_t>(p - data);
}

} // namespace starrocks::gin_pfor
