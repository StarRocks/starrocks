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

#include <cstring>

#include "base/string/faststring.h"

namespace starrocks::gin_pfor {

namespace {

// Maximum number of values a single encode()/decode() call may handle: the exception position and
// the exception count are both u8 in the frozen format, so a stream cannot represent more than this
// many values. The GIN block size (128) is well under this bound.
constexpr size_t kMaxValues = 255;

// Mask of the low `b` bits. b is always in [0, 32] here.
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
// the encoding is longer than a uint32 can hold (malformed input).
inline const uint8_t* get_varint(const uint8_t* p, const uint8_t* limit, uint32_t* out) {
    uint32_t result = 0;
    int shift = 0;
    while (p < limit) {
        uint8_t byte = *p++;
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

// Pick the low-bit width that minimizes the encoded size, using the bit-length histogram method
// (same family as Lucene PForUtil's freqs[] and Doris/TurboPFor's _p4bits): the encoded size depends
// only on the distribution of value bit-lengths, not on the values themselves. So summarize that
// distribution once in O(n), then evaluate every candidate width against the histogram instead of
// rescanning all values per width. Produces the exact same width (hence the same bytes) as a brute
// per-width rescan, in O(n + W^2) instead of O(n*W), W = 33.
//
// This is a writer-side heuristic only; it is NOT part of the on-disk format and can be retuned
// freely without breaking old segments.
int choose_bit_width(const uint32_t* vals, size_t n) {
    // hist[k] = number of values whose bit-length is exactly k (k in 0..32).
    int hist[33] = {0};
    int maxbits = 0;
    for (size_t i = 0; i < n; ++i) {
        int bl = bit_length(vals[i]);
        ++hist[bl];
        if (bl > maxbits) maxbits = bl;
    }

    int best_b = 0;
    size_t best_cost = static_cast<size_t>(-1);
    for (int b = 0; b <= maxbits; ++b) {
        // A value with bit-length k > b overflows b low bits and becomes an exception costing
        // 1 (pos) + varint_len(high) bytes; high has (k - b) bits so varint_len = ceil((k-b)/7).
        size_t exc_bytes = 0;
        for (int k = b + 1; k <= maxbits; ++k) {
            if (hist[k] != 0) {
                exc_bytes += static_cast<size_t>(hist[k]) * (1 + ((k - b) + 6) / 7);
            }
        }
        size_t stream_bytes = (n * static_cast<size_t>(b) + 7) / 8;
        size_t cost = 2 /*header*/ + exc_bytes + stream_bytes;
        if (cost < best_cost) {
            best_cost = cost;
            best_b = b;
        }
    }
    return best_b;
}

} // namespace

void encode(const uint32_t* vals, size_t n, faststring* out) {
    CHECK_LE(n, kMaxValues); // enforce in all builds: pos/num_exceptions are u8 (GIN block size is 128)

    const int b = choose_bit_width(vals, n);

    uint8_t num_exc = 0;
    for (size_t i = 0; i < n; ++i) {
        uint32_t high = (b >= 32) ? 0u : (vals[i] >> b);
        if (high != 0) ++num_exc;
    }

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

    // low stream: n values, b bits each, LSB-first
    const size_t stream_bytes = (n * static_cast<size_t>(b) + 7) / 8;
    if (stream_bytes > 0) {
        const size_t start = out->size();
        out->resize(start + stream_bytes);
        uint8_t* p = out->data() + start;
        std::memset(p, 0, stream_bytes);
        const uint32_t mask = low_mask(b);
        for (size_t i = 0; i < n; ++i) {
            uint32_t v = vals[i] & mask;
            size_t bitpos = i * static_cast<size_t>(b);
            for (int j = 0; j < b; ++j) {
                if ((v >> j) & 1u) {
                    p[(bitpos + j) >> 3] |= static_cast<uint8_t>(1u << ((bitpos + j) & 7));
                }
            }
        }
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
    }

    const size_t stream_bytes = (n * static_cast<size_t>(b) + 7) / 8;
    if (static_cast<size_t>(limit - p) < stream_bytes) return 0;
    const uint8_t* s = p;
    for (size_t i = 0; i < n; ++i) {
        uint32_t v = 0;
        size_t bitpos = i * static_cast<size_t>(b);
        for (int j = 0; j < b; ++j) {
            if ((s[(bitpos + j) >> 3] >> ((bitpos + j) & 7)) & 1u) {
                v |= (1u << j);
            }
        }
        out[i] = v;
    }
    p += stream_bytes;

    // apply patches; the checks above guarantee b < 32 whenever num_exc > 0 (so (high << b) is
    // well-defined) and every exc_pos < n (so the index stays in bounds).
    for (int e = 0; e < num_exc; ++e) {
        out[exc_pos[e]] |= (exc_high[e] << b);
    }

    return static_cast<size_t>(p - data);
}

} // namespace starrocks::gin_pfor
