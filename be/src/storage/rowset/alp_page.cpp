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

#include "storage/rowset/alp_page.h"

#include <vector>

// Vendored cwida/ALP (see be/src/thirdparty/alp). The headers are pulled in
// only here so the rest of the storage layer never sees them.
#include "alp.hpp"
#include "fastlanes/ffor.hpp"

namespace starrocks::alppage {

namespace {

// Per-vector meta layout, see alp_page.h.
inline void write_vector_meta(uint8_t* meta, uint8_t bw, uint8_t fac, uint8_t exp, uint16_t exc_c, uint64_t base) {
    meta[0] = bw;
    meta[1] = fac;
    meta[2] = exp;
    meta[3] = 0;
    encode_fixed16_le(&meta[4], exc_c);
    encode_fixed16_le(&meta[6], 0);
    encode_fixed64_le(&meta[8], base);
}

} // namespace

template <typename PT>
void alp_encode_body(const PT* vals, size_t num_padded, faststring* out) {
    using ST = typename alp::inner_t<PT>::st;
    constexpr size_t V = ALP_PAGE_VECTOR_SIZE;
    static_assert(V == alp::config::VECTOR_SIZE, "page vector size must match ALP");
    DCHECK_EQ(num_padded % V, 0);
    const size_t num_vectors = num_padded / V;
    constexpr size_t RAW_VECTOR_BYTES = V * sizeof(PT);

    std::vector<PT> sample_buf(V);
    alp::state<PT> stt;
    alp::encoder<PT>::init(vals, 0, num_padded, sample_buf.data(), stt);
    // ALP_RD (for "real" doubles with pseudo-random mantissas) is not
    // implemented; such pages degrade to per-vector raw fallback, which
    // matches what bitshuffle+lz4 achieves on incompressible floats.
    const bool alp_usable = (stt.scheme == alp::Scheme::ALP);

    std::vector<PT> exc(V);
    std::vector<uint16_t> pos(V);
    std::vector<uint16_t> exc_c_arr(V);
    std::vector<ST> encoded(V);
    std::vector<ST> base_arr(V);
    std::vector<ST> packed(V);

    for (size_t vi = 0; vi < num_vectors; vi++) {
        const PT* in = vals + vi * V;
        bool raw = !alp_usable;
        alp::bw_t bw = 0;
        uint16_t exc_c = 0;
        size_t packed_bytes = 0;
        size_t payload_bytes = 0;
        if (!raw) {
            alp::encoder<PT>::encode(in, exc.data(), pos.data(), exc_c_arr.data(), encoded.data(), stt);
            alp::encoder<PT>::analyze_ffor(encoded.data(), bw, base_arr.data());
            exc_c = exc_c_arr[0];
            packed_bytes = static_cast<size_t>(bw) * V / 8;
            payload_bytes = align_up_8(packed_bytes + exc_c * (sizeof(PT) + sizeof(uint16_t)));
            // If ALP does not actually win on this vector, store it verbatim.
            raw = payload_bytes >= RAW_VECTOR_BYTES;
        }

        size_t meta_off = out->size();
        if (raw) {
            out->resize(meta_off + ALP_PAGE_VECTOR_META_SIZE + RAW_VECTOR_BYTES);
            uint8_t* dst = out->data() + meta_off;
            write_vector_meta(dst, ALP_PAGE_RAW_VECTOR_MARKER, 0, 0, 0, 0);
            memcpy(dst + ALP_PAGE_VECTOR_META_SIZE, in, RAW_VECTOR_BYTES);
            continue;
        }

        ffor::ffor(encoded.data(), packed.data(), bw, base_arr.data());

        out->resize(meta_off + ALP_PAGE_VECTOR_META_SIZE + payload_bytes);
        uint8_t* dst = out->data() + meta_off;
        uint64_t base_u64 = 0;
        memcpy(&base_u64, &base_arr[0], sizeof(ST));
        write_vector_meta(dst, bw, stt.fac, stt.exp, exc_c, base_u64);
        uint8_t* payload = dst + ALP_PAGE_VECTOR_META_SIZE;
        memcpy(payload, packed.data(), packed_bytes);
        memcpy(payload + packed_bytes, exc.data(), exc_c * sizeof(PT));
        memcpy(payload + packed_bytes + exc_c * sizeof(PT), pos.data(), exc_c * sizeof(uint16_t));
        size_t used = packed_bytes + exc_c * (sizeof(PT) + sizeof(uint16_t));
        if (payload_bytes > used) {
            memset(payload + used, 0, payload_bytes - used);
        }
    }
}

template <typename PT>
Status alp_decode_body(const uint8_t* body, size_t body_size, size_t num_padded, PT* out) {
    using ST = typename alp::inner_t<PT>::st;
    constexpr size_t V = ALP_PAGE_VECTOR_SIZE;
    DCHECK_EQ(num_padded % V, 0);
    const size_t num_vectors = num_padded / V;
    constexpr size_t RAW_VECTOR_BYTES = V * sizeof(PT);

    // Scratch buffer for the (rare) case that a packed section is not
    // naturally aligned for ST loads.
    std::vector<ST> aligned_scratch(V);

    size_t off = 0;
    for (size_t vi = 0; vi < num_vectors; vi++) {
        if (off + ALP_PAGE_VECTOR_META_SIZE > body_size) {
            return Status::Corruption(strings::Substitute("ALP page body truncated at vector $0, offset $1, size $2",
                                                          vi, off, body_size));
        }
        const uint8_t* meta = body + off;
        uint8_t bw = meta[0];
        uint8_t fac = meta[1];
        uint8_t exp = meta[2];
        uint16_t exc_c = decode_fixed16_le(&meta[4]);
        uint64_t base_u64 = decode_fixed64_le(&meta[8]);
        off += ALP_PAGE_VECTOR_META_SIZE;
        PT* out_vec = out + vi * V;

        if (bw == ALP_PAGE_RAW_VECTOR_MARKER) {
            if (off + RAW_VECTOR_BYTES > body_size) {
                return Status::Corruption(strings::Substitute("ALP raw vector $0 truncated", vi));
            }
            memcpy(out_vec, body + off, RAW_VECTOR_BYTES);
            off += RAW_VECTOR_BYTES;
            continue;
        }

        if (bw > sizeof(PT) * 8) {
            return Status::Corruption(strings::Substitute("invalid ALP bit width $0 at vector $1", (int)bw, vi));
        }
        size_t packed_bytes = static_cast<size_t>(bw) * V / 8;
        size_t payload_bytes = align_up_8(packed_bytes + exc_c * (sizeof(PT) + sizeof(uint16_t)));
        if (off + payload_bytes > body_size) {
            return Status::Corruption(strings::Substitute("ALP vector $0 truncated", vi));
        }
        const uint8_t* packed = body + off;
        const ST* packed_st = reinterpret_cast<const ST*>(packed);
        if (reinterpret_cast<uintptr_t>(packed) % alignof(ST) != 0) {
            memcpy(aligned_scratch.data(), packed, packed_bytes);
            packed_st = aligned_scratch.data();
        }
        ST base;
        memcpy(&base, &base_u64, sizeof(ST));
        generated::falp::fallback::scalar::falp(packed_st, out_vec, bw, &base, fac, exp);

        // Patch exceptions with unaligned-safe reads.
        const uint8_t* exc_vals = packed + packed_bytes;
        const uint8_t* exc_pos = exc_vals + exc_c * sizeof(PT);
        for (uint16_t i = 0; i < exc_c; i++) {
            uint16_t p = decode_fixed16_le(exc_pos + i * sizeof(uint16_t));
            if (p >= V) {
                return Status::Corruption(strings::Substitute("invalid ALP exception position $0", (int)p));
            }
            PT v;
            memcpy(&v, exc_vals + i * sizeof(PT), sizeof(PT));
            out_vec[p] = v;
        }
        off += payload_bytes;
    }
    if (off != body_size) {
        return Status::Corruption(
                strings::Substitute("ALP page body size mismatch, consumed $0 of $1", off, body_size));
    }
    return Status::OK();
}

template void alp_encode_body<float>(const float*, size_t, faststring*);
template void alp_encode_body<double>(const double*, size_t, faststring*);
template Status alp_decode_body<float>(const uint8_t*, size_t, size_t, float*);
template Status alp_decode_body<double>(const uint8_t*, size_t, size_t, double*);

} // namespace starrocks::alppage
