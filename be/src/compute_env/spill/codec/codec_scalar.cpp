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

// M1 scalar spill codecs: bool (bitpack / RLE) and fixed-width integers (RLE / DELTA /
// FOR / PFOR), with a shared nullable framing that encodes the null bitmap separately
// (or omits it entirely when there are no nulls -- the common case, which the legacy
// path pays streamvbyte for on every column).
//
// Payload layouts (little-endian; all self-describing):
//   framing     : [tag u8]                       0=non-nullable
//                 [tag u8][flags?]               1=nullable-no-nulls (flags omitted)
//                                                2=nullable, flags as BOOL_RLE payload
//                                                3=nullable, flags raw [u32 n][bytes]
//   BOOL_BITPACK: [u32 n][ceil(n/8) bytes]
//   BOOL_RLE    : [u32 n][u32 nruns][{u32 len, u8 val} x nruns]
//   INT_RLE     : [u8 w][u32 n][u32 nruns][{u32 len, valW} x nruns]
//   INT_FOR     : [u8 w][u32 n][u32 blen][bitpacked block: u8 bw, minW, packed n*bw bits]
//   INT_DELTA   : [u8 w][u32 n][firstW][u32 blen][bitpacked block: zigzag deltas (n-1)]
//   INT_PFOR    : [u8 w][u32 n][u8 bw][minW][u32 packed_bytes][packed n*bw bits]
//                 [u32 nexc][{u16 pos} x nexc][{valW (v-min)} x nexc]
//
// Width routing: I32 covers int32/uint32/date/decimal32; I64 covers int64/uint64/
// timestamp/decimal64 (raw fixed-length bytes reinterpreted as uintN).

#include "compute_env/spill/codec/codec_scalar.h"

#include <cmath>
#include <cstring>
#include <limits>
#include <mutex>

#include "base/bit/bit_stream_utils.h"
#include "base/bit/bit_stream_utils.inline.h"
#include "base/bit/frame_of_reference_coding.h"
#include "base/compression/block_compression.h"
#include "base/hash/hash.h"
#include "base/phmap/phmap.h"
#include "base/string/faststring.h"
#include "column/binary_column.h"
#include "column/column.h"
#include "column/nullable_column.h"
#include "column/serde/column_array_serde.h"
#include "compute_env/spill/codec/codec_util.h"
#include "compute_env/spill/codec/leaf_classify.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/port.h"

namespace starrocks::spill {

namespace {

// ---------------------------------------------------------------------------
// low-level helpers
// ---------------------------------------------------------------------------

Status check_remaining(const uint8_t* buf, const uint8_t* end, size_t need) {
    return codec_check_remaining(buf, end, need);
}

// bool RLE core, shared by BOOL_RLE and the nullable-flags framing.
size_t bool_rle_count_runs(const uint8_t* v, size_t n) {
    if (n == 0) return 0;
    size_t runs = 1;
    for (size_t i = 1; i < n; ++i) runs += (v[i] != v[i - 1]);
    return runs;
}

uint8_t* bool_rle_encode(const uint8_t* v, size_t n, uint8_t* buf) {
    UNALIGNED_STORE32(buf, static_cast<uint32_t>(n));
    uint8_t* nruns_pos = buf + 4;
    buf += 8;
    uint32_t nruns = 0;
    size_t i = 0;
    while (i < n) {
        size_t j = i + 1;
        while (j < n && v[j] == v[i]) ++j;
        UNALIGNED_STORE32(buf, static_cast<uint32_t>(j - i));
        buf[4] = v[i];
        buf += 5;
        ++nruns;
        i = j;
    }
    UNALIGNED_STORE32(nruns_pos, nruns);
    return buf;
}

// Decodes into `out` (already sized to hold n bytes read from the payload header by the
// caller via bool_rle_peek_n). Reports whether any decoded value is non-zero.
StatusOr<const uint8_t*> bool_rle_decode(const uint8_t* buf, const uint8_t* end, uint8_t* out, size_t expect_n,
                                         bool* any_nonzero) {
    RETURN_IF_ERROR(check_remaining(buf, end, 8));
    uint32_t n = UNALIGNED_LOAD32(buf);
    uint32_t nruns = UNALIGNED_LOAD32(buf + 4);
    buf += 8;
    if (n != expect_n) {
        return Status::Corruption("bool rle row count mismatch");
    }
    RETURN_IF_ERROR(check_remaining(buf, end, static_cast<size_t>(nruns) * 5));
    bool any = false;
    size_t filled = 0;
    for (uint32_t r = 0; r < nruns; ++r) {
        uint32_t len = UNALIGNED_LOAD32(buf);
        uint8_t val = buf[4];
        buf += 5;
        if (filled + len > n) return Status::Corruption("bool rle overflow");
        memset(out + filled, val, len);
        filled += len;
        any |= (val != 0);
    }
    if (filled != n) return Status::Corruption("bool rle underflow");
    if (any_nonzero != nullptr) *any_nonzero = any;
    return buf;
}

StatusOr<size_t> bool_rle_peek_n(const uint8_t* buf, const uint8_t* end) {
    RETURN_IF_ERROR(check_remaining(buf, end, 4));
    return UNALIGNED_LOAD32(buf);
}

// ---------------------------------------------------------------------------
// nullable framing base
// ---------------------------------------------------------------------------

class LeafScalarCodec : public SpillColumnCodec {
public:
    int64_t max_encoded_size(const Column& col, uint32_t param) const final {
        LeafView v = classify_leaf(col);
        if (!leaf_applicable(v)) return 0;
        int64_t leaf_sz = leaf_max_size(v, param);
        if (leaf_sz <= 0) return 0;
        int64_t null_sz = v.nullable != nullptr ? 5 + static_cast<int64_t>(v.rows) : 0; // worst: raw flags
        return 1 + null_sz + leaf_sz;
    }

    StatusOr<uint8_t*> encode(const Column& col, uint8_t* buf, uint32_t param, CodecContext* ctx) const final {
        LeafView v = classify_leaf(col);
        if (!leaf_applicable(v)) {
            return Status::NotSupported("column not applicable for scalar spill codec");
        }
        if (v.nullable == nullptr) {
            *buf++ = 0;
            return leaf_encode(v, buf, param, ctx);
        }
        const uint8_t* flags = v.nullable->immutable_null_column_data().data();
        const size_t n = v.rows;
        bool has_null = v.nullable->has_null(); // may be stale-true; verify by scan
        if (has_null) {
            has_null = false;
            for (size_t i = 0; i < n; ++i) {
                if (flags[i] != 0) {
                    has_null = true;
                    break;
                }
            }
        }
        if (!has_null) {
            *buf++ = 1;
            return leaf_encode(v, buf, param, ctx);
        }
        size_t nruns = bool_rle_count_runs(flags, n);
        if (8 + nruns * 5 < 4 + n) {
            *buf++ = 2;
            buf = bool_rle_encode(flags, n, buf);
        } else {
            *buf++ = 3;
            UNALIGNED_STORE32(buf, static_cast<uint32_t>(n));
            memcpy(buf + 4, flags, n);
            buf += 4 + n;
        }
        return leaf_encode(v, buf, param, ctx);
    }

    StatusOr<const uint8_t*> decode(const uint8_t* buf, const uint8_t* end, Column* col) const final {
        RETURN_IF_ERROR(check_remaining(buf, end, 1));
        uint8_t tag = *buf++;
        if (tag == 0) {
            return leaf_decode(buf, end, col);
        }
        auto* nc = dynamic_cast<NullableColumn*>(col);
        if (nc == nullptr) {
            return Status::Corruption("nullable spill payload but target column is not nullable");
        }
        Column* data = nc->data_column_raw_ptr();
        if (tag == 1) {
            ASSIGN_OR_RETURN(buf, leaf_decode(buf, end, data));
            nc->null_column_raw_ptr()->resize(data->size()); // zero-filled flags
            return buf;
        }
        bool any = false;
        size_t n = 0;
        if (tag == 2) {
            ASSIGN_OR_RETURN(n, bool_rle_peek_n(buf, end));
            RETURN_IF_ERROR(codec_check_count(n)); // RLE flag count is not buffer-bounded
            uint8_t* flags = nullptr;
            if (!leaf_mutable_bytes(nc->null_column_raw_ptr(), n, &flags)) {
                return Status::Corruption("spill null flags target is not a bool column");
            }
            ASSIGN_OR_RETURN(buf, bool_rle_decode(buf, end, flags, n, &any));
        } else if (tag == 3) {
            RETURN_IF_ERROR(check_remaining(buf, end, 4));
            n = UNALIGNED_LOAD32(buf);
            buf += 4;
            RETURN_IF_ERROR(check_remaining(buf, end, n));
            uint8_t* flags = nullptr;
            if (!leaf_mutable_bytes(nc->null_column_raw_ptr(), n, &flags)) {
                return Status::Corruption("spill null flags target is not a bool column");
            }
            memcpy(flags, buf, n);
            buf += n;
            for (size_t i = 0; i < n && !any; ++i) any = flags[i] != 0;
        } else {
            return Status::Corruption("unknown nullable framing tag in spill payload");
        }
        nc->set_has_null(any);
        ASSIGN_OR_RETURN(buf, leaf_decode(buf, end, data));
        if (data->size() != n) {
            return Status::Corruption("null flags / data size mismatch in spill payload");
        }
        return buf;
    }

protected:
    virtual bool leaf_applicable(const LeafView& v) const = 0;
    virtual int64_t leaf_max_size(const LeafView& v, uint32_t param) const = 0;
    virtual StatusOr<uint8_t*> leaf_encode(const LeafView& v, uint8_t* buf, uint32_t param,
                                           CodecContext* ctx) const = 0;
    virtual StatusOr<const uint8_t*> leaf_decode(const uint8_t* buf, const uint8_t* end, Column* leaf) const = 0;
};

// ---------------------------------------------------------------------------
// bool codecs
// ---------------------------------------------------------------------------

class BoolBitpackCodec final : public LeafScalarCodec {
public:
    CodecId id() const override { return CodecId::BOOL_BITPACK; }

protected:
    bool leaf_applicable(const LeafView& v) const override { return v.kind == LeafKind::BOOL; }

    int64_t leaf_max_size(const LeafView& v, uint32_t) const override { return 4 + (v.rows + 7) / 8; }

    StatusOr<uint8_t*> leaf_encode(const LeafView& v, uint8_t* buf, uint32_t, CodecContext* ctx) const override {
        const size_t n = v.rows;
        UNALIGNED_STORE32(buf, static_cast<uint32_t>(n));
        buf += 4;
        memset(buf, 0, (n + 7) / 8);
        for (size_t i = 0; i < n; ++i) {
            buf[i >> 3] |= static_cast<uint8_t>(v.data[i] != 0) << (i & 7);
        }
        return buf + (n + 7) / 8;
    }

    StatusOr<const uint8_t*> leaf_decode(const uint8_t* buf, const uint8_t* end, Column* leaf) const override {
        RETURN_IF_ERROR(check_remaining(buf, end, 4));
        size_t n = UNALIGNED_LOAD32(buf);
        buf += 4;
        RETURN_IF_ERROR(check_remaining(buf, end, (n + 7) / 8));
        uint8_t* out = nullptr;
        if (!leaf_mutable_bytes(leaf, n, &out)) {
            return Status::Corruption("bool bitpack target is not a bool column");
        }
        for (size_t i = 0; i < n; ++i) {
            out[i] = (buf[i >> 3] >> (i & 7)) & 1;
        }
        return buf + (n + 7) / 8;
    }
};

class BoolRleCodec final : public LeafScalarCodec {
public:
    CodecId id() const override { return CodecId::BOOL_RLE; }

protected:
    bool leaf_applicable(const LeafView& v) const override { return v.kind == LeafKind::BOOL; }

    int64_t leaf_max_size(const LeafView& v, uint32_t) const override { return 8 + static_cast<int64_t>(v.rows) * 5; }

    StatusOr<uint8_t*> leaf_encode(const LeafView& v, uint8_t* buf, uint32_t, CodecContext* ctx) const override {
        return bool_rle_encode(v.data, v.rows, buf);
    }

    StatusOr<const uint8_t*> leaf_decode(const uint8_t* buf, const uint8_t* end, Column* leaf) const override {
        ASSIGN_OR_RETURN(size_t n, bool_rle_peek_n(buf, end));
        RETURN_IF_ERROR(codec_check_count(n)); // RLE count is not buffer-bounded
        uint8_t* out = nullptr;
        if (!leaf_mutable_bytes(leaf, n, &out)) {
            return Status::Corruption("bool rle target is not a bool column");
        }
        return bool_rle_decode(buf, end, out, n, nullptr);
    }
};

// ---------------------------------------------------------------------------
// integer codecs (width-dispatched over uint32_t / uint64_t)
// ---------------------------------------------------------------------------

struct IntLeafHeader {
    uint8_t width;
    uint32_t rows;
};

uint8_t* write_int_header(uint8_t* buf, size_t width, size_t rows) {
    buf[0] = static_cast<uint8_t>(width);
    UNALIGNED_STORE32(buf + 1, static_cast<uint32_t>(rows));
    return buf + 5;
}

StatusOr<const uint8_t*> read_int_header(const uint8_t* buf, const uint8_t* end, size_t expect_width,
                                         IntLeafHeader* h) {
    RETURN_IF_ERROR(check_remaining(buf, end, 5));
    h->width = buf[0];
    h->rows = UNALIGNED_LOAD32(buf + 1);
    if (h->width != expect_width) {
        return Status::Corruption("spill int codec width mismatch");
    }
    RETURN_IF_ERROR(codec_check_count(h->rows)); // guard before the caller resizes to rows
    return buf + 5;
}

// --- INT_RLE ---

template <typename UT>
uint8_t* int_rle_encode(const UT* v, size_t n, uint8_t* buf) {
    buf = write_int_header(buf, sizeof(UT), n);
    uint8_t* nruns_pos = buf;
    buf += 4;
    uint32_t nruns = 0;
    size_t i = 0;
    while (i < n) {
        size_t j = i + 1;
        while (j < n && v[j] == v[i]) ++j;
        UNALIGNED_STORE32(buf, static_cast<uint32_t>(j - i));
        memcpy(buf + 4, &v[i], sizeof(UT));
        buf += 4 + sizeof(UT);
        ++nruns;
        i = j;
    }
    UNALIGNED_STORE32(nruns_pos, nruns);
    return buf;
}

template <typename UT>
StatusOr<const uint8_t*> int_rle_decode(const uint8_t* buf, const uint8_t* end, UT* out, size_t n) {
    RETURN_IF_ERROR(check_remaining(buf, end, 4));
    uint32_t nruns = UNALIGNED_LOAD32(buf);
    buf += 4;
    RETURN_IF_ERROR(check_remaining(buf, end, static_cast<size_t>(nruns) * (4 + sizeof(UT))));
    size_t filled = 0;
    for (uint32_t r = 0; r < nruns; ++r) {
        uint32_t len = UNALIGNED_LOAD32(buf);
        UT val;
        memcpy(&val, buf + 4, sizeof(UT));
        buf += 4 + sizeof(UT);
        if (filled + len > n) return Status::Corruption("int rle overflow");
        for (uint32_t k = 0; k < len; ++k) out[filled + k] = val;
        filled += len;
    }
    if (filled != n) return Status::Corruption("int rle underflow");
    return buf;
}

// --- INT_FOR (bitpack block from codec_util.h) ---

template <typename UT>
StatusOr<uint8_t*> int_for_encode(const UT* v, size_t n, uint8_t* buf) {
    buf = write_int_header(buf, sizeof(UT), n);
    return bitpack_block_encode<UT>(v, n, buf);
}

template <typename UT>
StatusOr<const uint8_t*> int_for_decode(const uint8_t* buf, const uint8_t* end, UT* out, size_t n) {
    return bitpack_block_decode<UT>(buf, end, out, n);
}

// --- INT_DELTA (first value + FOR-packed zigzag deltas) ---

template <typename UT>
std::vector<UT>& delta_scratch() {
    thread_local std::vector<UT> scratch;
    return scratch;
}

template <typename UT>
StatusOr<uint8_t*> int_delta_encode(const UT* v, size_t n, uint8_t* buf) {
    buf = write_int_header(buf, sizeof(UT), n);
    if (n == 0) {
        return bitpack_block_encode<UT>(nullptr, 0, buf);
    }
    memcpy(buf, &v[0], sizeof(UT));
    buf += sizeof(UT);
    auto& deltas = delta_scratch<UT>();
    deltas.resize(n - 1);
    for (size_t i = 1; i < n; ++i) {
        deltas[i - 1] = zigzag_encode<UT>(v[i] - v[i - 1]);
    }
    return bitpack_block_encode<UT>(deltas.data(), n - 1, buf);
}

template <typename UT>
StatusOr<const uint8_t*> int_delta_decode(const uint8_t* buf, const uint8_t* end, UT* out, size_t n) {
    if (n == 0) {
        return bitpack_block_decode<UT>(buf, end, out, 0);
    }
    RETURN_IF_ERROR(check_remaining(buf, end, sizeof(UT)));
    memcpy(&out[0], buf, sizeof(UT));
    buf += sizeof(UT);
    auto& deltas = delta_scratch<UT>();
    deltas.resize(n - 1);
    ASSIGN_OR_RETURN(buf, bitpack_block_decode<UT>(buf, end, deltas.data(), n - 1));
    for (size_t i = 1; i < n; ++i) {
        out[i] = out[i - 1] + zigzag_decode<UT>(deltas[i - 1]);
    }
    return buf;
}

// --- INT_PFOR (FOR with exception patching; bit width chosen by total-cost search) ---

template <typename UT>
StatusOr<uint8_t*> int_pfor_encode(const UT* v, size_t n, uint8_t* buf) {
    buf = write_int_header(buf, sizeof(UT), n);
    UT min_v = std::numeric_limits<UT>::max();
    for (size_t i = 0; i < n; ++i) min_v = std::min(min_v, v[i]);
    if (n == 0) min_v = 0;

    // bit-width histogram of (v - min)
    constexpr int kMaxBits = sizeof(UT) * 8;
    size_t hist[kMaxBits + 1] = {0};
    for (size_t i = 0; i < n; ++i) hist[bits<UT>(v[i] - min_v)]++;
    // choose bw minimizing packed + exception cost
    int best_bw = kMaxBits;
    size_t best_cost = std::numeric_limits<size_t>::max();
    size_t exc_above = 0; // values with bits > bw, computed by suffix sweep
    for (int bw = kMaxBits; bw >= 0; --bw) {
        size_t cost = (n * bw + 7) / 8 + exc_above * (2 + sizeof(UT));
        if (cost < best_cost) {
            best_cost = cost;
            best_bw = bw;
        }
        if (bw > 0) exc_above += hist[bw]; // moving to bw-1: values needing exactly bw bits become exceptions
    }
    const int bw = best_bw;
    const UT mask = bw >= kMaxBits ? ~UT{0} : ((UT{1} << bw) - 1);

    buf[0] = static_cast<uint8_t>(bw);
    memcpy(buf + 1, &min_v, sizeof(UT));
    buf += 1 + sizeof(UT);

    thread_local faststring packed;
    thread_local std::vector<uint16_t> exc_pos;
    thread_local std::vector<UT> exc_val;
    exc_pos.clear();
    exc_val.clear();
    {
        BitWriter bw_writer(&packed); // ctor clears `packed`
        for (size_t i = 0; i < n; ++i) {
            UT adj = v[i] - min_v;
            if (bits<UT>(adj) > static_cast<uint8_t>(bw)) {
                exc_pos.push_back(static_cast<uint16_t>(i));
                exc_val.push_back(adj);
                adj &= mask;
            }
            if (bw > 0) bw_writer.PutValue(static_cast<uint64_t>(adj), bw);
        }
        bw_writer.Flush();
        UNALIGNED_STORE32(buf, static_cast<uint32_t>(bw_writer.bytes_written()));
        memcpy(buf + 4, packed.data(), bw_writer.bytes_written());
        buf += 4 + bw_writer.bytes_written();
    }
    UNALIGNED_STORE32(buf, static_cast<uint32_t>(exc_pos.size()));
    buf += 4;
    memcpy(buf, exc_pos.data(), exc_pos.size() * sizeof(uint16_t));
    buf += exc_pos.size() * sizeof(uint16_t);
    memcpy(buf, exc_val.data(), exc_val.size() * sizeof(UT));
    buf += exc_val.size() * sizeof(UT);
    return buf;
}

template <typename UT>
StatusOr<const uint8_t*> int_pfor_decode(const uint8_t* buf, const uint8_t* end, UT* out, size_t n) {
    RETURN_IF_ERROR(check_remaining(buf, end, 1 + sizeof(UT) + 4));
    int bw = buf[0];
    UT min_v;
    memcpy(&min_v, buf + 1, sizeof(UT));
    buf += 1 + sizeof(UT);
    uint32_t packed_bytes = UNALIGNED_LOAD32(buf);
    buf += 4;
    RETURN_IF_ERROR(check_remaining(buf, end, packed_bytes));
    if (bw > 0) {
        BitReader reader(buf, packed_bytes);
        if (!reader.GetBatch(bw, out, static_cast<int>(n))) {
            return Status::Corruption("spill PFOR unpack failed");
        }
    } else {
        memset(out, 0, n * sizeof(UT));
    }
    buf += packed_bytes;
    RETURN_IF_ERROR(check_remaining(buf, end, 4));
    uint32_t nexc = UNALIGNED_LOAD32(buf);
    buf += 4;
    RETURN_IF_ERROR(check_remaining(buf, end, nexc * (sizeof(uint16_t) + sizeof(UT))));
    const auto* pos = reinterpret_cast<const uint16_t*>(buf);
    const uint8_t* val_base = buf + nexc * sizeof(uint16_t);
    for (uint32_t e = 0; e < nexc; ++e) {
        uint16_t p;
        memcpy(&p, &pos[e], sizeof(uint16_t));
        if (p >= n) return Status::Corruption("spill PFOR exception out of range");
        UT val;
        memcpy(&val, val_base + e * sizeof(UT), sizeof(UT));
        out[p] = val;
    }
    for (size_t i = 0; i < n; ++i) out[i] += min_v;
    return val_base + nexc * sizeof(UT);
}

// --- the width-dispatching codec wrapper ---

enum class IntAlgo { RLE, DELTA, FOR, PFOR };

template <IntAlgo A>
class IntCodec final : public LeafScalarCodec {
public:
    explicit IntCodec(CodecId cid) : _cid(cid) {}
    CodecId id() const override { return _cid; }

protected:
    bool leaf_applicable(const LeafView& v) const override {
        if (v.kind != LeafKind::I32 && v.kind != LeafKind::I64) return false;
        if (A == IntAlgo::PFOR && v.rows > std::numeric_limits<uint16_t>::max()) return false;
        return true;
    }

    int64_t leaf_max_size(const LeafView& v, uint32_t) const override {
        const int64_t n = v.rows;
        const int64_t w = leaf_kind_width(v.kind);
        switch (A) {
        case IntAlgo::RLE:
            return 9 + n * (4 + w);
        case IntAlgo::DELTA:
        case IntAlgo::FOR:
            // bitpacked-block worst: full-width values + block meta
            return 9 + w + n * w + (n / 128 + 2) * 16 + 64;
        case IntAlgo::PFOR:
            // worst: full-width packing plus every value an exception
            return 10 + w + n * w + 4 + n * (2 + w) + 16;
        }
        return 0;
    }

    StatusOr<uint8_t*> leaf_encode(const LeafView& v, uint8_t* buf, uint32_t, CodecContext* ctx) const override {
        if (v.kind == LeafKind::I32) {
            return encode_t<uint32_t>(reinterpret_cast<const uint32_t*>(v.data), v.rows, buf);
        }
        return encode_t<uint64_t>(reinterpret_cast<const uint64_t*>(v.data), v.rows, buf);
    }

    StatusOr<const uint8_t*> leaf_decode(const uint8_t* buf, const uint8_t* end, Column* leaf) const override {
        // peek the width byte to route (the header is validated inside decode_t)
        RETURN_IF_ERROR(check_remaining(buf, end, 5));
        uint8_t width = buf[0];
        // The stream width alone must not choose the write type: decode_t<UT> writes `rows`
        // elements of sizeof(UT) into a buffer the destination column sizes by ITS OWN element
        // width. A corrupt/truncated payload with a flipped width byte would otherwise overflow
        // (e.g. width 4->8 into an int32 column = 2x heap write). Require them to agree.
        size_t dst_width = leaf_element_bytes(leaf);
        if (width != dst_width) {
            return Status::Corruption("spill int codec width does not match target column");
        }
        if (width == 4) return decode_t<uint32_t>(buf, end, leaf);
        if (width == 8) return decode_t<uint64_t>(buf, end, leaf);
        return Status::Corruption("spill int codec unsupported width");
    }

private:
    template <typename UT>
    StatusOr<uint8_t*> encode_t(const UT* v, size_t n, uint8_t* buf) const {
        switch (A) {
        case IntAlgo::RLE:
            return int_rle_encode<UT>(v, n, buf);
        case IntAlgo::DELTA:
            return int_delta_encode<UT>(v, n, buf);
        case IntAlgo::FOR:
            return int_for_encode<UT>(v, n, buf);
        case IntAlgo::PFOR:
            return int_pfor_encode<UT>(v, n, buf);
        }
        return Status::InternalError("unreachable");
    }

    template <typename UT>
    StatusOr<const uint8_t*> decode_t(const uint8_t* buf, const uint8_t* end, Column* leaf) const {
        IntLeafHeader h;
        ASSIGN_OR_RETURN(buf, read_int_header(buf, end, sizeof(UT), &h));
        uint8_t* out_bytes = nullptr;
        if (!leaf_mutable_bytes(leaf, h.rows, &out_bytes)) {
            return Status::Corruption("spill int codec target not fixed-length");
        }
        auto* out = reinterpret_cast<UT*>(out_bytes);
        switch (A) {
        case IntAlgo::RLE:
            return int_rle_decode<UT>(buf, end, out, h.rows);
        case IntAlgo::DELTA:
            return int_delta_decode<UT>(buf, end, out, h.rows);
        case IntAlgo::FOR:
            return int_for_decode<UT>(buf, end, out, h.rows);
        case IntAlgo::PFOR:
            return int_pfor_decode<UT>(buf, end, out, h.rows);
        }
        return Status::InternalError("unreachable");
    }

    const CodecId _cid;
};

// ---------------------------------------------------------------------------
// string codecs (BinaryColumn with u32 offsets; strings are scalars too)
// ---------------------------------------------------------------------------

using StrColumn = BinaryColumnBase<uint32_t>;

const StrColumn* as_str(const Column* leaf) {
    return dynamic_cast<const StrColumn*>(leaf);
}

StrColumn* as_mut_str(Column* leaf) {
    return dynamic_cast<StrColumn*>(leaf);
}

// Rebuild a BinaryColumn from per-row lengths + a filler that writes the byte blob.
// Offsets install via AdaptiveOffsets::set_small_buffer, mirroring ColumnArraySerde.
template <typename FillFn>
Status build_str_column(StrColumn* col, const std::vector<uint32_t>& lens, FillFn&& fill) {
    Buffer<uint32_t> offsets;
    raw::stl_vector_resize_uninitialized(&offsets, lens.size() + 1);
    offsets[0] = 0;
    uint64_t total = 0;
    for (size_t i = 0; i < lens.size(); ++i) {
        total += lens[i];
        if (total > std::numeric_limits<uint32_t>::max()) {
            return Status::Corruption("spill string payload exceeds u32 offsets");
        }
        offsets[i + 1] = static_cast<uint32_t>(total);
    }
    col->get_offset().set_small_buffer(std::move(offsets));
    auto& bytes = col->get_bytes();
    bytes.resize(total);
    return fill(bytes.data(), total);
}

std::vector<uint32_t>& u32_scratch(int slot) {
    thread_local std::vector<uint32_t> scratch[4];
    return scratch[slot];
}

// STR_DICT: [u32 n][u32 dict_n][u32 dict_bytes][dict blob][dict lens block][codes block]
class StrDictCodec final : public LeafScalarCodec {
public:
    CodecId id() const override { return CodecId::STR_DICT; }

protected:
    bool leaf_applicable(const LeafView& v) const override { return v.kind == LeafKind::STR; }

    int64_t leaf_max_size(const LeafView& v, uint32_t) const override {
        const auto* c = as_str(v.leaf);
        if (c == nullptr) return 0;
        return 12 + static_cast<int64_t>(c->get_immutable_bytes().size()) +
               2 * bitpack_block_max_size<uint32_t>(v.rows);
    }

    StatusOr<uint8_t*> leaf_encode(const LeafView& v, uint8_t* buf, uint32_t, CodecContext* ctx) const override {
        const auto* c = as_str(v.leaf);
        const auto& off = c->get_offset();
        const auto* bytes = c->get_immutable_bytes().data();
        const size_t n = v.rows;

        auto& codes = u32_scratch(0);
        auto& dict_lens = u32_scratch(1);
        codes.resize(n);
        dict_lens.clear();
        thread_local std::vector<Slice> dict_slices;
        dict_slices.clear();
        phmap::flat_hash_map<Slice, uint32_t, SliceHash> map;
        map.reserve(std::min<size_t>(n, 4096));
        for (size_t i = 0; i < n; ++i) {
            Slice s(reinterpret_cast<const char*>(bytes + off[i]), off[i + 1] - off[i]);
            auto [it, inserted] = map.try_emplace(s, static_cast<uint32_t>(dict_slices.size()));
            if (inserted) {
                dict_slices.push_back(s);
                dict_lens.push_back(static_cast<uint32_t>(s.size));
            }
            codes[i] = it->second;
        }

        UNALIGNED_STORE32(buf, static_cast<uint32_t>(n));
        UNALIGNED_STORE32(buf + 4, static_cast<uint32_t>(dict_slices.size()));
        uint64_t dict_bytes = 0;
        for (const auto& s : dict_slices) dict_bytes += s.size;
        UNALIGNED_STORE32(buf + 8, static_cast<uint32_t>(dict_bytes));
        buf += 12;
        for (const auto& s : dict_slices) {
            memcpy(buf, s.data, s.size);
            buf += s.size;
        }
        buf = bitpack_block_encode<uint32_t>(dict_lens.data(), dict_lens.size(), buf);
        return bitpack_block_encode<uint32_t>(codes.data(), n, buf);
    }

    StatusOr<const uint8_t*> leaf_decode(const uint8_t* buf, const uint8_t* end, Column* leaf) const override {
        auto* c = as_mut_str(leaf);
        if (c == nullptr) return Status::Corruption("spill dict codec target is not a string column");
        RETURN_IF_ERROR(codec_check_remaining(buf, end, 12));
        uint32_t n = UNALIGNED_LOAD32(buf);
        uint32_t dict_n = UNALIGNED_LOAD32(buf + 4);
        uint32_t dict_bytes = UNALIGNED_LOAD32(buf + 8);
        buf += 12;
        RETURN_IF_ERROR(codec_check_count(n));
        RETURN_IF_ERROR(codec_check_count(dict_n));
        RETURN_IF_ERROR(codec_check_remaining(buf, end, dict_bytes));
        const uint8_t* dict_blob = buf;
        buf += dict_bytes;

        auto& dict_lens = u32_scratch(1);
        dict_lens.resize(dict_n);
        ASSIGN_OR_RETURN(buf, bitpack_block_decode<uint32_t>(buf, end, dict_lens.data(), dict_n));
        auto& dict_offs = u32_scratch(2);
        dict_offs.resize(dict_n + 1);
        dict_offs[0] = 0;
        // Accumulate in uint64 and bound every prefix: a u32 running sum would wrap (e.g.
        // dict_lens {0x80000000, 0x80000000} sums to 0), passing the final == dict_bytes check
        // while individual offsets point far past the dict_bytes-sized blob -> OOB read in the
        // memcpy below. Each prefix must stay within the declared blob.
        uint64_t acc = 0;
        for (uint32_t i = 0; i < dict_n; ++i) {
            acc += dict_lens[i];
            if (acc > dict_bytes) {
                return Status::Corruption("spill dict offset exceeds blob");
            }
            dict_offs[i + 1] = static_cast<uint32_t>(acc);
        }
        if (dict_n > 0 && acc != dict_bytes) {
            return Status::Corruption("spill dict blob size mismatch");
        }

        auto& codes = u32_scratch(0);
        codes.resize(n);
        ASSIGN_OR_RETURN(buf, bitpack_block_decode<uint32_t>(buf, end, codes.data(), n));

        auto& lens = u32_scratch(3);
        lens.resize(n);
        for (uint32_t i = 0; i < n; ++i) {
            if (codes[i] >= dict_n) return Status::Corruption("spill dict code out of range");
            lens[i] = dict_lens[codes[i]];
        }
        RETURN_IF_ERROR(build_str_column(c, lens, [&](uint8_t* out, uint64_t) {
            for (uint32_t i = 0; i < n; ++i) {
                memcpy(out, dict_blob + dict_offs[codes[i]], lens[i]);
                out += lens[i];
            }
            return Status::OK();
        }));
        return buf;
    }
};

// STR_FRONT: [u32 n][prefix-lens block][suffix-lens block][u32 suffix_bytes][suffix blob]
class StrFrontCodec final : public LeafScalarCodec {
public:
    CodecId id() const override { return CodecId::STR_FRONT; }

protected:
    bool leaf_applicable(const LeafView& v) const override { return v.kind == LeafKind::STR; }

    int64_t leaf_max_size(const LeafView& v, uint32_t) const override {
        const auto* c = as_str(v.leaf);
        if (c == nullptr) return 0;
        return 8 + static_cast<int64_t>(c->get_immutable_bytes().size()) + 2 * bitpack_block_max_size<uint32_t>(v.rows);
    }

    StatusOr<uint8_t*> leaf_encode(const LeafView& v, uint8_t* buf, uint32_t, CodecContext* ctx) const override {
        const auto* c = as_str(v.leaf);
        const auto& off = c->get_offset();
        const auto* bytes = c->get_immutable_bytes().data();
        const size_t n = v.rows;

        auto& plens = u32_scratch(0);
        auto& slens = u32_scratch(1);
        plens.resize(n);
        slens.resize(n);
        uint64_t suffix_bytes = 0;
        for (size_t i = 0; i < n; ++i) {
            uint32_t len = off[i + 1] - off[i];
            uint32_t plen = 0;
            if (i > 0) {
                uint32_t prev_len = off[i] - off[i - 1];
                const uint8_t* prev = bytes + off[i - 1];
                const uint8_t* cur = bytes + off[i];
                uint32_t lim = std::min(len, prev_len);
                while (plen < lim && prev[plen] == cur[plen]) ++plen;
            }
            plens[i] = plen;
            slens[i] = len - plen;
            suffix_bytes += slens[i];
        }

        UNALIGNED_STORE32(buf, static_cast<uint32_t>(n));
        buf += 4;
        buf = bitpack_block_encode<uint32_t>(plens.data(), n, buf);
        buf = bitpack_block_encode<uint32_t>(slens.data(), n, buf);
        UNALIGNED_STORE32(buf, static_cast<uint32_t>(suffix_bytes));
        buf += 4;
        for (size_t i = 0; i < n; ++i) {
            memcpy(buf, bytes + off[i] + plens[i], slens[i]);
            buf += slens[i];
        }
        return buf;
    }

    StatusOr<const uint8_t*> leaf_decode(const uint8_t* buf, const uint8_t* end, Column* leaf) const override {
        auto* c = as_mut_str(leaf);
        if (c == nullptr) return Status::Corruption("spill front codec target is not a string column");
        RETURN_IF_ERROR(codec_check_remaining(buf, end, 4));
        uint32_t n = UNALIGNED_LOAD32(buf);
        buf += 4;
        RETURN_IF_ERROR(codec_check_count(n));
        auto& plens = u32_scratch(0);
        auto& slens = u32_scratch(1);
        plens.resize(n);
        slens.resize(n);
        ASSIGN_OR_RETURN(buf, bitpack_block_decode<uint32_t>(buf, end, plens.data(), n));
        ASSIGN_OR_RETURN(buf, bitpack_block_decode<uint32_t>(buf, end, slens.data(), n));
        RETURN_IF_ERROR(codec_check_remaining(buf, end, 4));
        uint32_t suffix_bytes = UNALIGNED_LOAD32(buf);
        buf += 4;
        RETURN_IF_ERROR(codec_check_remaining(buf, end, suffix_bytes));

        auto& lens = u32_scratch(2);
        lens.resize(n);
        uint64_t sum_slens = 0;
        for (uint32_t i = 0; i < n; ++i) {
            if (i == 0 && plens[i] != 0) return Status::Corruption("spill front coding first prefix != 0");
            // prefix is copied from the previous reconstructed row; it must not exceed it,
            // otherwise the prefix memcpy below reads beyond that row.
            if (i > 0 && plens[i] > lens[i - 1]) {
                return Status::Corruption("spill front coding prefix exceeds previous row");
            }
            lens[i] = plens[i] + slens[i];
            sum_slens += slens[i];
        }
        // The fill loop consumes exactly Σ slens bytes from the suffix blob; a corrupt slens
        // block whose sum exceeds the declared suffix_bytes would otherwise read past the
        // bounds-checked blob (mirror the DICT codec's declared-size check).
        if (sum_slens != suffix_bytes) {
            return Status::Corruption("spill front coding suffix length mismatch");
        }
        const uint8_t* suffix = buf;
        RETURN_IF_ERROR(build_str_column(c, lens, [&](uint8_t* out, uint64_t) {
            uint8_t* prev = nullptr;
            for (uint32_t i = 0; i < n; ++i) {
                if (plens[i] > 0) {
                    // prefix comes from the previous reconstructed row
                    memcpy(out, prev, plens[i]);
                }
                memcpy(out + plens[i], suffix, slens[i]);
                suffix += slens[i];
                prev = out;
                out += lens[i];
            }
            return Status::OK();
        }));
        return buf + suffix_bytes;
    }
};

// STR_BLOCK_*: [u32 n][lens block][u8 compressed][u32 raw_bytes][u32 blob_bytes][blob]
class StrBlockCodec final : public LeafScalarCodec {
public:
    StrBlockCodec(CodecId cid, CompressionTypePB type) : _cid(cid), _type(type) {}
    CodecId id() const override { return _cid; }

protected:
    bool leaf_applicable(const LeafView& v) const override { return v.kind == LeafKind::STR; }

    int64_t leaf_max_size(const LeafView& v, uint32_t) const override {
        const auto* c = as_str(v.leaf);
        if (c == nullptr) return 0;
        const auto* codec = get_codec();
        if (codec == nullptr) return 0;
        size_t raw = c->get_immutable_bytes().size();
        return 13 + bitpack_block_max_size<uint32_t>(v.rows) +
               static_cast<int64_t>(std::max(codec->max_compressed_len(raw), raw));
    }

    StatusOr<uint8_t*> leaf_encode(const LeafView& v, uint8_t* buf, uint32_t, CodecContext* ctx) const override {
        const auto* c = as_str(v.leaf);
        const auto& off = c->get_offset();
        auto bytes = c->get_immutable_bytes();
        const size_t n = v.rows;

        auto& lens = u32_scratch(0);
        lens.resize(n);
        for (size_t i = 0; i < n; ++i) lens[i] = off[i + 1] - off[i];

        UNALIGNED_STORE32(buf, static_cast<uint32_t>(n));
        buf += 4;
        buf = bitpack_block_encode<uint32_t>(lens.data(), n, buf);

        const auto* codec = get_codec();
        uint8_t* flag_pos = buf;
        buf += 9; // flag + raw + blob sizes
        size_t blob_bytes;
        bool compressed = false;
        if (codec != nullptr && bytes.size() > 0) {
            Slice in(reinterpret_cast<const char*>(bytes.data()), bytes.size());
            Slice out(reinterpret_cast<char*>(buf), codec->max_compressed_len(bytes.size()));
            auto st = codec->compress(in, &out);
            if (st.ok() && out.size < bytes.size()) {
                compressed = true;
                blob_bytes = out.size;
            }
        }
        if (!compressed) {
            memcpy(buf, bytes.data(), bytes.size());
            blob_bytes = bytes.size();
        }
        flag_pos[0] = compressed ? 1 : 0;
        UNALIGNED_STORE32(flag_pos + 1, static_cast<uint32_t>(bytes.size()));
        UNALIGNED_STORE32(flag_pos + 5, static_cast<uint32_t>(blob_bytes));
        return buf + blob_bytes;
    }

    StatusOr<const uint8_t*> leaf_decode(const uint8_t* buf, const uint8_t* end, Column* leaf) const override {
        auto* c = as_mut_str(leaf);
        if (c == nullptr) return Status::Corruption("spill block codec target is not a string column");
        RETURN_IF_ERROR(codec_check_remaining(buf, end, 4));
        uint32_t n = UNALIGNED_LOAD32(buf);
        buf += 4;
        RETURN_IF_ERROR(codec_check_count(n));
        auto& lens = u32_scratch(0);
        lens.resize(n);
        ASSIGN_OR_RETURN(buf, bitpack_block_decode<uint32_t>(buf, end, lens.data(), n));
        RETURN_IF_ERROR(codec_check_remaining(buf, end, 9));
        bool compressed = buf[0] != 0;
        uint32_t raw_bytes = UNALIGNED_LOAD32(buf + 1);
        uint32_t blob_bytes = UNALIGNED_LOAD32(buf + 5);
        buf += 9;
        RETURN_IF_ERROR(codec_check_remaining(buf, end, blob_bytes));

        RETURN_IF_ERROR(build_str_column(c, lens, [&](uint8_t* out, uint64_t total) {
            if (total != raw_bytes) {
                return Status::Corruption("spill block codec raw size mismatch");
            }
            if (!compressed) {
                // `out` is sized by `total` (== raw_bytes); the stored blob length must equal it,
                // or this memcpy overruns the target heap buffer (a corrupt blob_bytes >> raw_bytes
                // with compressed=0 is otherwise unchecked -- the compressed path bounds writes via
                // the decompressor, this raw path had no such guard).
                if (blob_bytes != raw_bytes) {
                    return Status::Corruption("spill block codec uncompressed size mismatch");
                }
                memcpy(out, buf, blob_bytes);
                return Status::OK();
            }
            const auto* codec = get_codec();
            if (codec == nullptr) return Status::Corruption("spill block codec unavailable");
            Slice in(reinterpret_cast<const char*>(buf), blob_bytes);
            Slice out_slice(reinterpret_cast<char*>(out), total);
            return codec->decompress(in, &out_slice);
        }));
        return buf + blob_bytes;
    }

private:
    const BlockCompressionCodec* get_codec() const {
        const BlockCompressionCodec* codec = nullptr;
        (void)get_block_compression_codec(_type, &codec);
        return codec;
    }

    const CodecId _cid;
    const CompressionTypePB _type;
};

// Applicable to ANY column (including complex types, int128, float32, large binary):
// the level-0 ColumnArraySerde bytes of the whole column are run through a block
// compressor. This is the decision tree's fallback compression layer -- specialized
// codecs beat it where they apply, and the selector's cost model arbitrates.
// Payload: [u8 compressed][u32 raw_bytes][u32 blob_bytes][blob]
class GenericBlockCodec final : public SpillColumnCodec {
public:
    GenericBlockCodec(CodecId cid, CompressionTypePB type) : _cid(cid), _type(type) {}
    CodecId id() const override { return _cid; }

    int64_t max_encoded_size(const Column& col, uint32_t) const override {
        const auto* codec = get_codec();
        if (codec == nullptr) return 0;
        int64_t raw = serde::ColumnArraySerde::max_serialized_size(col, 0);
        if (raw <= 0) return 0;
        return 9 + static_cast<int64_t>(std::max<size_t>(codec->max_compressed_len(raw), raw));
    }

    StatusOr<uint8_t*> encode(const Column& col, uint8_t* buf, uint32_t, CodecContext*) const override {
        // level-0 serialize into scratch, then compress into the output buffer
        thread_local std::string raw_scratch;
        int64_t cap = serde::ColumnArraySerde::max_serialized_size(col, 0);
        raw_scratch.resize(cap + 16);
        auto* raw_base = reinterpret_cast<uint8_t*>(raw_scratch.data());
        ASSIGN_OR_RETURN(auto* raw_end, serde::ColumnArraySerde::serialize(col, raw_base, false, 0));
        const size_t raw_bytes = raw_end - raw_base;

        const auto* codec = get_codec();
        uint8_t* header = buf;
        buf += 9;
        size_t blob_bytes = 0;
        bool compressed = false;
        if (codec != nullptr && raw_bytes > 0) {
            Slice in(reinterpret_cast<const char*>(raw_base), raw_bytes);
            Slice out(reinterpret_cast<char*>(buf), codec->max_compressed_len(raw_bytes));
            auto st = codec->compress(in, &out);
            if (st.ok() && out.size < raw_bytes) {
                compressed = true;
                blob_bytes = out.size;
            }
        }
        if (!compressed) {
            memcpy(buf, raw_base, raw_bytes);
            blob_bytes = raw_bytes;
        }
        header[0] = compressed ? 1 : 0;
        UNALIGNED_STORE32(header + 1, static_cast<uint32_t>(raw_bytes));
        UNALIGNED_STORE32(header + 5, static_cast<uint32_t>(blob_bytes));
        return buf + blob_bytes;
    }

    StatusOr<const uint8_t*> decode(const uint8_t* buf, const uint8_t* end, Column* col) const override {
        RETURN_IF_ERROR(codec_check_remaining(buf, end, 9));
        const bool compressed = buf[0] != 0;
        const uint32_t raw_bytes = UNALIGNED_LOAD32(buf + 1);
        const uint32_t blob_bytes = UNALIGNED_LOAD32(buf + 5);
        buf += 9;
        RETURN_IF_ERROR(codec_check_count(raw_bytes)); // guards raw_scratch resize on the decompress path
        RETURN_IF_ERROR(codec_check_remaining(buf, end, blob_bytes));

        const uint8_t* raw_base = buf;
        thread_local std::string raw_scratch;
        if (compressed) {
            const auto* codec = get_codec();
            if (codec == nullptr) return Status::Corruption("spill generic block codec unavailable");
            raw_scratch.resize(static_cast<size_t>(raw_bytes) + 16);
            Slice in(reinterpret_cast<const char*>(buf), blob_bytes);
            Slice out(raw_scratch.data(), raw_bytes);
            RETURN_IF_ERROR(codec->decompress(in, &out));
            raw_base = reinterpret_cast<const uint8_t*>(raw_scratch.data());
        } else if (blob_bytes != raw_bytes) {
            return Status::Corruption("spill generic block raw size mismatch");
        }
        ASSIGN_OR_RETURN(auto* cur,
                         serde::ColumnArraySerde::deserialize(raw_base, raw_base + raw_bytes, col, false, 0));
        if (static_cast<size_t>(cur - raw_base) != raw_bytes) {
            return Status::Corruption("spill generic block decode length mismatch");
        }
        return buf + blob_bytes;
    }

private:
    const BlockCompressionCodec* get_codec() const {
        const BlockCompressionCodec* codec = nullptr;
        (void)get_block_compression_codec(_type, &codec);
        return codec;
    }

    const CodecId _cid;
    const CompressionTypePB _type;
};

// ---------------------------------------------------------------------------
// float codecs (double leaves)
// ---------------------------------------------------------------------------

// FP_ALP: the ALP essence, self-written. Detect a decimal scale 10^e such that
// v * 10^e round-trips to the exact double; store scaled int64s through the FOR
// bitpack block, non-conforming values as (pos, raw double) exceptions.
// Payload: [u8 e][u32 n][zigzag-scaled block][u32 nexc][{u16 pos} x][{f64 raw} x]
class FpAlpCodec final : public LeafScalarCodec {
public:
    CodecId id() const override { return CodecId::FP_ALP; }

protected:
    bool leaf_applicable(const LeafView& v) const override {
        return v.kind == LeafKind::F64 && v.rows <= std::numeric_limits<uint16_t>::max();
    }

    int64_t leaf_max_size(const LeafView& v, uint32_t) const override {
        const int64_t n = v.rows;
        return 5 + bitpack_block_max_size<uint64_t>(n) + 4 + n * (2 + 8) + 16;
    }

    StatusOr<uint8_t*> leaf_encode(const LeafView& v, uint8_t* buf, uint32_t, CodecContext* ctx) const override {
        const auto* vals = reinterpret_cast<const double*>(v.data);
        const size_t n = v.rows;

        int best_e = detect_exponent(vals, n);
        const double f = kPow10[best_e];
        const double inv_f = 1.0 / f;

        auto& scaled = u64_scratch();
        scaled.resize(n);
        thread_local std::vector<uint16_t> exc_pos;
        thread_local std::vector<double> exc_val;
        exc_pos.clear();
        exc_val.clear();
        for (size_t i = 0; i < n; ++i) {
            int64_t s;
            if (scale_exact(vals[i], f, inv_f, &s)) {
                scaled[i] = zigzag_encode<uint64_t>(static_cast<uint64_t>(s));
            } else {
                scaled[i] = 0;
                exc_pos.push_back(static_cast<uint16_t>(i));
                exc_val.push_back(vals[i]);
            }
        }

        buf[0] = static_cast<uint8_t>(best_e);
        UNALIGNED_STORE32(buf + 1, static_cast<uint32_t>(n));
        buf += 5;
        buf = bitpack_block_encode<uint64_t>(scaled.data(), n, buf);
        UNALIGNED_STORE32(buf, static_cast<uint32_t>(exc_pos.size()));
        buf += 4;
        memcpy(buf, exc_pos.data(), exc_pos.size() * sizeof(uint16_t));
        buf += exc_pos.size() * sizeof(uint16_t);
        memcpy(buf, exc_val.data(), exc_val.size() * sizeof(double));
        buf += exc_val.size() * sizeof(double);
        return buf;
    }

    StatusOr<const uint8_t*> leaf_decode(const uint8_t* buf, const uint8_t* end, Column* leaf) const override {
        RETURN_IF_ERROR(codec_check_remaining(buf, end, 5));
        int e = buf[0];
        uint32_t n = UNALIGNED_LOAD32(buf + 1);
        buf += 5;
        RETURN_IF_ERROR(codec_check_count(n));
        if (e >= static_cast<int>(sizeof(kPow10) / sizeof(kPow10[0]))) {
            return Status::Corruption("spill ALP exponent out of range");
        }
        uint8_t* out_bytes = nullptr;
        if (!leaf_mutable_bytes(leaf, n, &out_bytes)) {
            return Status::Corruption("spill ALP target is not a double column");
        }
        auto* out = reinterpret_cast<double*>(out_bytes);

        auto& scaled = u64_scratch();
        scaled.resize(n);
        ASSIGN_OR_RETURN(buf, bitpack_block_decode<uint64_t>(buf, end, scaled.data(), n));
        const double f = kPow10[e];
        for (uint32_t i = 0; i < n; ++i) {
            // division, matching the encode-side exactness check (see scale_exact)
            out[i] = static_cast<double>(static_cast<int64_t>(zigzag_decode<uint64_t>(scaled[i]))) / f;
        }
        RETURN_IF_ERROR(codec_check_remaining(buf, end, 4));
        uint32_t nexc = UNALIGNED_LOAD32(buf);
        buf += 4;
        RETURN_IF_ERROR(codec_check_remaining(buf, end, nexc * (2 + 8)));
        const uint8_t* val_base = buf + nexc * sizeof(uint16_t);
        for (uint32_t x = 0; x < nexc; ++x) {
            uint16_t p;
            memcpy(&p, buf + x * sizeof(uint16_t), sizeof(uint16_t));
            if (p >= n) return Status::Corruption("spill ALP exception out of range");
            memcpy(&out[p], val_base + x * sizeof(double), sizeof(double));
        }
        return val_base + nexc * sizeof(double);
    }

private:
    static constexpr double kPow10[15] = {1, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14};

    static std::vector<uint64_t>& u64_scratch() {
        thread_local std::vector<uint64_t> scratch;
        return scratch;
    }

    static bool scale_exact(double v, double f, double /*inv_f*/, int64_t* out) {
        if (!std::isfinite(v)) return false;
        double scaled = v * f;
        if (scaled >= 4.6e18 || scaled <= -4.6e18) return false; // int64 headroom
        auto s = static_cast<int64_t>(std::llround(scaled));
        // Reconstruction check MUST use division: k/f is correctly rounded while k*(1/f)
        // is not (1/f is itself inexact), so the multiply form rejects nearly everything.
        if (static_cast<double>(s) / f != v) return false;
        *out = s;
        return true;
    }

    // pick the exponent that makes the most sampled values scale exactly (smallest wins ties)
    static int detect_exponent(const double* vals, size_t n) {
        const size_t sample = std::min<size_t>(n, 32);
        int best_e = 0;
        size_t best_hits = 0;
        for (int e = 0; e < static_cast<int>(sizeof(kPow10) / sizeof(kPow10[0])); ++e) {
            size_t hits = 0;
            const double f = kPow10[e];
            const double inv_f = 1.0 / f;
            for (size_t i = 0; i < sample; ++i) {
                int64_t s;
                hits += scale_exact(vals[i], f, inv_f, &s);
            }
            if (hits > best_hits) {
                best_hits = hits;
                best_e = e;
                if (hits == sample) break;
            }
        }
        return best_e;
    }
};

} // namespace

void install_scalar_codecs(std::vector<const SpillColumnCodec*>* by_id) {
    static IntCodec<IntAlgo::RLE> int_rle(CodecId::INT_RLE);
    static IntCodec<IntAlgo::DELTA> int_delta(CodecId::INT_DELTA);
    static IntCodec<IntAlgo::FOR> int_for(CodecId::INT_FOR);
    static IntCodec<IntAlgo::PFOR> int_pfor(CodecId::INT_PFOR);
    static BoolBitpackCodec bool_bitpack;
    static BoolRleCodec bool_rle;
    static StrDictCodec str_dict;
    static StrFrontCodec str_front;
    static StrBlockCodec str_block_lz4(CodecId::STR_BLOCK_LZ4, CompressionTypePB::LZ4);
    static StrBlockCodec str_block_zstd(CodecId::STR_BLOCK_ZSTD, CompressionTypePB::ZSTD);
    static FpAlpCodec fp_alp;
    static GenericBlockCodec gb_lz4(CodecId::GENERIC_BLOCK_LZ4, CompressionTypePB::LZ4);
    static GenericBlockCodec gb_zstd(CodecId::GENERIC_BLOCK_ZSTD, CompressionTypePB::ZSTD);

    auto put = [&](const SpillColumnCodec* c) {
        auto idx = static_cast<uint16_t>(c->id());
        if (by_id->size() <= idx) by_id->resize(idx + 1, nullptr);
        (*by_id)[idx] = c;
    };
    put(&int_rle);
    put(&int_delta);
    put(&int_for);
    put(&int_pfor);
    put(&bool_bitpack);
    put(&bool_rle);
    put(&str_dict);
    put(&str_front);
    put(&str_block_lz4);
    put(&str_block_zstd);
    put(&fp_alp);
    put(&gb_lz4);
    put(&gb_zstd);
}

} // namespace starrocks::spill
