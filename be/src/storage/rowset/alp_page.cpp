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

#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <vector>

// Thirdparty cwida/ALP (libalp.a). The headers are pulled in only here so
// the rest of the storage layer never sees them.
#include "alp.hpp"
#include "base/coding.h"
#include "column/container_resource.h"
#include "column/fixed_length_column.h"
#include "common/logging.h"
#include "fastlanes/ffor.hpp"
#include "gutil/port.h"
#include "gutil/strings/substitute.h"
#include "storage/types.h"

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
        // The scale indexes come straight from page metadata and index the
        // Constants<PT> factor/exponent tables inside FALP; validate them
        // before use so a malformed page cannot cause out-of-bounds reads.
        constexpr uint8_t MAX_SCALE_INDEX = alp::Constants<PT>::MAX_EXPONENT;
        if (exp > MAX_SCALE_INDEX || fac > exp) {
            return Status::Corruption(strings::Substitute("invalid ALP scale indexes fac=$0 exp=$1 at vector $2",
                                                          (int)fac, (int)exp, vi));
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

namespace starrocks {

template <LogicalType Type>
AlpPageBuilder<Type>::AlpPageBuilder(const PageBuilderOptions& options)
        : _max_count(options.data_page_size / SIZE_OF_TYPE) {
    _data.reserve(alppage::padded_element_count(_max_count) * SIZE_OF_TYPE);
}

template <LogicalType Type>
void AlpPageBuilder<Type>::reserve_head(uint8_t head_size) {
    CHECK(_reserved_head_size == 0);
    _reserved_head_size = head_size;
}

template <LogicalType Type>
uint32_t AlpPageBuilder<Type>::add(const uint8_t* vals, uint32_t count) {
    DCHECK(!_finished);
    uint32_t to_add = std::min<uint32_t>(_max_count - _count, count);
    size_t old_sz = _data.size();
    _data.resize(old_sz + to_add * SIZE_OF_TYPE);
    memcpy(&_data[old_sz], vals, to_add * SIZE_OF_TYPE);
    _count += to_add;
    return to_add;
}

template <LogicalType Type>
faststring* AlpPageBuilder<Type>::finish() {
    if (_count > 0) {
        _first_value = cell(0);
        _last_value = cell(_count - 1);
    }
    return _finish();
}

template <LogicalType Type>
void AlpPageBuilder<Type>::reset() {
    _count = 0;
    _data.clear();
    _finished = false;
}

template <LogicalType Type>
Status AlpPageBuilder<Type>::get_first_value(void* value) const {
    DCHECK(_finished);
    if (_count == 0) {
        return Status::NotFound("page is empty");
    }
    memcpy(value, &_first_value, SIZE_OF_TYPE);
    return Status::OK();
}

template <LogicalType Type>
Status AlpPageBuilder<Type>::get_last_value(void* value) const {
    DCHECK(_finished);
    if (_count == 0) {
        return Status::NotFound("page is empty");
    }
    memcpy(value, &_last_value, SIZE_OF_TYPE);
    return Status::OK();
}

template <LogicalType Type>
typename AlpPageBuilder<Type>::CppType AlpPageBuilder<Type>::cell(int idx) const {
    DCHECK_GE(idx, 0);
    CppType ret;
    memcpy(&ret, &_data[idx * SIZE_OF_TYPE], SIZE_OF_TYPE);
    return ret;
}

template <LogicalType Type>
faststring* AlpPageBuilder<Type>::_finish() {
    // Pad up to a whole number of ALP vectors with +0.0, which every
    // (factor, exponent) combination encodes exactly.
    size_t num_padded = alppage::padded_element_count(_count);
    size_t padding_bytes = (num_padded - _count) * SIZE_OF_TYPE;
    size_t old_sz = _data.size();
    _data.resize(old_sz + padding_bytes);
    memset(&_data[old_sz], 0, padding_bytes);

    _encoded.clear();
    _encoded.resize(_reserved_head_size + ALP_PAGE_HEADER_SIZE);
    if (num_padded > 0) {
        alppage::alp_encode_body(reinterpret_cast<const CppType*>(_data.data()), num_padded, &_encoded);
    }

    uint8_t* header = _encoded.data() + _reserved_head_size;
    encode_fixed32_le(&header[0], _count);
    encode_fixed32_le(&header[4], _encoded.size() - _reserved_head_size);
    encode_fixed32_le(&header[8], num_padded);
    encode_fixed32_le(&header[12], SIZE_OF_TYPE);
    _finished = true;
    return &_encoded;
}

template <LogicalType Type>
Status AlpPageDecoder<Type>::init() {
    CHECK(!_parsed);
    if (_data.size < ALP_PAGE_HEADER_SIZE) {
        return Status::InternalError(strings::Substitute("file corruption: invalid data size:$0, header size:$1",
                                                         _data.size, ALP_PAGE_HEADER_SIZE));
    }
    _num_elements = decode_fixed32_le((const uint8_t*)&_data[0]);
    _num_element_after_padding = decode_fixed32_le((const uint8_t*)&_data[8]);
    if (_num_element_after_padding != alppage::padded_element_count(_num_elements)) {
        return Status::InternalError(
                strings::Substitute("num of element information corrupted, padded:$0, num_elements:$1",
                                    _num_element_after_padding, _num_elements));
    }
    size_t size_of_element = decode_fixed32_le((const uint8_t*)&_data[12]);
    if (size_of_element != SIZE_OF_TYPE) {
        return Status::InternalError(
                strings::Substitute("invalid size_of_elem:$0, expected:$1", size_of_element, (size_t)SIZE_OF_TYPE));
    }
    if (_data.size != _num_element_after_padding * SIZE_OF_TYPE + ALP_PAGE_HEADER_SIZE) {
        return Status::InternalError(
                strings::Substitute("size information unmatched, data size:$0, expected:$1", _data.size,
                                    _num_element_after_padding * SIZE_OF_TYPE + ALP_PAGE_HEADER_SIZE));
    }
    _parsed = true;
    return Status::OK();
}

template <LogicalType Type>
Status AlpPageDecoder<Type>::seek_to_position_in_page(uint32_t pos) {
    DCHECK(_parsed) << "Must call init()";
    DCHECK_LE(pos, _num_elements);
    if (pos > _num_elements) {
        return Status::InternalError(strings::Substitute("invalid pos:$0, num_elements:$1", pos, _num_elements));
    }
    _cur_index = pos;
    return Status::OK();
}

template <LogicalType Type>
Status AlpPageDecoder<Type>::seek_at_or_after_value(const void* value, bool* exact_match) {
    DCHECK(_parsed) << "Must call init() firstly";
    if (_num_elements == 0) {
        return Status::NotFound("page is empty");
    }
    size_t left = 0;
    size_t right = _num_elements;
    while (left < right) {
        size_t mid = left + (right - left) / 2;
        const void* mid_value = get_data(mid * SIZE_OF_TYPE);
        if (TypeComparator<Type>::cmp(mid_value, value) < 0) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    if (left >= _num_elements) {
        return Status::NotFound("all value small than the value");
    }
    const void* find_value = get_data(left * SIZE_OF_TYPE);
    *exact_match = TypeComparator<Type>::cmp(find_value, value) == 0;
    _cur_index = left;
    return Status::OK();
}

template <LogicalType Type>
Status AlpPageDecoder<Type>::next_batch(size_t* count, Column* dst) {
    SparseRange<> read_range;
    uint32_t begin = current_index();
    read_range.add(Range<>(begin, begin + *count));
    RETURN_IF_ERROR(next_batch(read_range, dst));
    *count = current_index() - begin;
    return Status::OK();
}

template <LogicalType Type>
Status AlpPageDecoder<Type>::next_batch(const SparseRange<>& range, Column* dst) {
    DCHECK(_parsed);
    if (PREDICT_FALSE(_cur_index >= _num_elements)) {
        return Status::OK();
    }
    size_t to_read = std::min(static_cast<size_t>(range.span_size()), static_cast<size_t>(_num_elements - _cur_index));
    SparseRangeIterator<> iter = range.new_iterator();
    while (to_read > 0) {
        _cur_index = iter.begin();
        Range<> r = iter.next(to_read);
        ContainerResource container(_page_handle, get_data(_cur_index * SIZE_OF_TYPE), r.span_size() * SIZE_OF_TYPE);
        int n = dst->append_numbers(container);
        DCHECK_EQ(r.span_size(), n);
        _cur_index += r.span_size();
        to_read -= r.span_size();
    }
    return Status::OK();
}

template <LogicalType Type>
Status AlpPageDecoder<Type>::read_by_rowids(const ordinal_t first_ordinal_in_page, const rowid_t* rowids,
                                            size_t* count, Column* column) {
    DCHECK(_parsed);
    if (PREDICT_FALSE(*count == 0)) {
        return Status::OK();
    }
    size_t total = *count;
    size_t read_count = 0;
    auto data = std::make_unique_for_overwrite<CppType[]>(total);
    for (size_t i = 0; i < total; i++) {
        ordinal_t ord = rowids[i] - first_ordinal_in_page;
        if (UNLIKELY(ord >= _num_elements)) {
            break;
        }
        data[read_count++] = *reinterpret_cast<const CppType*>(get_data(ord * SIZE_OF_TYPE));
    }
    if (read_count > 0) {
        size_t nappend = column->append_numbers(data.get(), SIZE_OF_TYPE * read_count);
        if (UNLIKELY(nappend != read_count)) {
            return Status::InternalError(
                    strings::Substitute("append_numbers failed, expected rows[$0], actual rows[$1]", read_count,
                                        nappend));
        }
    }
    *count = read_count;
    return Status::OK();
}

template class AlpPageBuilder<TYPE_FLOAT>;
template class AlpPageBuilder<TYPE_DOUBLE>;
template class AlpPageDecoder<TYPE_FLOAT>;
template class AlpPageDecoder<TYPE_DOUBLE>;

} // namespace starrocks
