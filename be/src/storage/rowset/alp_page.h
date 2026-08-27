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

#include <glog/logging.h>

#include <algorithm>
#include <cstdint>
#include <cstring>

#include "base/coding.h"
#include "base/string/faststring.h"
#include "base/string/slice.h"
#include "column/container_resource.h"
#include "column/fixed_length_column.h"
#include "common/logging.h"
#include "gutil/port.h"
#include "gutil/strings/substitute.h"
#include "storage/olap_common.h"
#include "storage/rowset/common.h"
#include "storage/rowset/options.h"
#include "storage/rowset/page_builder.h"
#include "storage/rowset/page_decoder.h"
#include "storage/types.h"
#include "types/storage_type_traits.h"

namespace starrocks {

// ALP (Adaptive Lossless floating-Point, cwida/ALP, vendored under
// be/src/thirdparty/alp) column encoding for FLOAT/DOUBLE data pages.
//
// The page format is as follows:
//
// 1. Header: (16 bytes, same shape as the bitshuffle page header)
//
//    <num_elements>       [le32] number of elements encoded in the page.
//    <encoded_size>       [le32] encoded page size incl. this header.
//    <padded_num_elements>[le32] element count padded up to a multiple of
//                                ALP_PAGE_VECTOR_SIZE; padding values are +0.0.
//    <size_of_element>    [le32] 4 (float) or 8 (double).
//
// 2. Encoded body: per 1024-value vector, back to back:
//
//    vector meta (16 bytes):
//      [0]  bit_width  (u8, 0xFF marks a raw fallback vector)
//      [1]  factor idx (u8)
//      [2]  exponent idx (u8)
//      [3]  reserved (0)
//      [4]  exceptions count (le16)
//      [6]  reserved (0)
//      [8]  frame-of-reference base (le64, low size_of_element bytes valid)
//    raw vector:      ALP_PAGE_VECTOR_SIZE * size_of_element verbatim bytes.
//    encoded vector:  fastlanes-ffor packed data (ALP_PAGE_VECTOR_SIZE *
//                     bit_width / 8 bytes), exception values
//                     (count * size_of_element), exception positions
//                     (count * le16), zero padding up to an 8-byte boundary
//                     so every vector stays aligned.
//
// Like BIT_SHUFFLE, the page is fully decoded once at page-load time by
// AlpDataDecoder (storage_page_decoder.cpp), so the page cache holds raw
// values and AlpPageDecoder is a plain memcpy decoder. A BE that does not
// know ALP_ENCODING fails to open the segment via EncodingInfo::get instead
// of misreading it; the write side is gated by
// config::enable_alp_float_encoding (default off).

inline constexpr size_t ALP_PAGE_HEADER_SIZE = 16;
inline constexpr size_t ALP_PAGE_VECTOR_SIZE = 1024;
inline constexpr size_t ALP_PAGE_VECTOR_META_SIZE = 16;
inline constexpr uint8_t ALP_PAGE_RAW_VECTOR_MARKER = 0xFF;

namespace alppage {

inline constexpr size_t align_up_8(size_t v) {
    return (v + 7) & ~size_t(7);
}

inline constexpr size_t padded_element_count(size_t n) {
    return (n + ALP_PAGE_VECTOR_SIZE - 1) / ALP_PAGE_VECTOR_SIZE * ALP_PAGE_VECTOR_SIZE;
}

// Encode |num_padded| (a multiple of ALP_PAGE_VECTOR_SIZE) values into |out|
// (appended). Implemented in alp_page.cpp so that the vendored ALP headers
// stay out of this widely-included header.
template <typename PT>
void alp_encode_body(const PT* vals, size_t num_padded, faststring* out);

// Decode an encoded body back into |out| (capacity num_padded values).
template <typename PT>
Status alp_decode_body(const uint8_t* body, size_t body_size, size_t num_padded, PT* out);

} // namespace alppage

template <LogicalType Type>
class AlpPageBuilder final : public PageBuilder {
    using CppType = StorageCppType<Type>;
    static_assert(Type == TYPE_FLOAT || Type == TYPE_DOUBLE, "ALP encoding only supports FLOAT/DOUBLE");

public:
    explicit AlpPageBuilder(const PageBuilderOptions& options) : _max_count(options.data_page_size / SIZE_OF_TYPE) {
        _data.reserve(alppage::padded_element_count(_max_count) * SIZE_OF_TYPE);
    }

    void reserve_head(uint8_t head_size) override {
        CHECK(_reserved_head_size == 0);
        _reserved_head_size = head_size;
    }

    bool is_page_full() override { return _count >= _max_count; }

    uint32_t add(const uint8_t* vals, uint32_t count) override {
        DCHECK(!_finished);
        uint32_t to_add = std::min<uint32_t>(_max_count - _count, count);
        size_t old_sz = _data.size();
        _data.resize(old_sz + to_add * SIZE_OF_TYPE);
        memcpy(&_data[old_sz], vals, to_add * SIZE_OF_TYPE);
        _count += to_add;
        return to_add;
    }

    faststring* finish() override {
        if (_count > 0) {
            _first_value = cell(0);
            _last_value = cell(_count - 1);
        }
        return _finish();
    }

    void reset() override {
        _count = 0;
        _data.clear();
        _finished = false;
    }

    uint32_t count() const override { return _count; }

    uint64_t size() const override { return _data.size(); }

    Status get_first_value(void* value) const override {
        DCHECK(_finished);
        if (_count == 0) {
            return Status::NotFound("page is empty");
        }
        memcpy(value, &_first_value, SIZE_OF_TYPE);
        return Status::OK();
    }

    Status get_last_value(void* value) const override {
        DCHECK(_finished);
        if (_count == 0) {
            return Status::NotFound("page is empty");
        }
        memcpy(value, &_last_value, SIZE_OF_TYPE);
        return Status::OK();
    }

    CppType cell(int idx) const {
        DCHECK_GE(idx, 0);
        CppType ret;
        memcpy(&ret, &_data[idx * SIZE_OF_TYPE], SIZE_OF_TYPE);
        return ret;
    }

private:
    faststring* _finish() {
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

    enum { SIZE_OF_TYPE = StorageCppTypeSize<Type> };
    uint8_t _reserved_head_size{0};
    uint32_t _max_count;
    uint32_t _count{0};
    faststring _data;
    faststring _encoded;
    CppType _first_value{};
    CppType _last_value{};
    bool _finished{false};
};

// AlpPageDecoder operates on pages already decoded by AlpDataDecoder at
// page-load time (mirroring BitShufflePageDecoder): the incoming slice is
// the 16-byte header followed by padded_num_elements raw values.
template <LogicalType Type>
class AlpPageDecoder final : public PageDecoder {
    using CppType = StorageCppType<Type>;
    static_assert(Type == TYPE_FLOAT || Type == TYPE_DOUBLE, "ALP encoding only supports FLOAT/DOUBLE");

public:
    explicit AlpPageDecoder(Slice data) : _data(data) {}

    Status init() override {
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

    Status seek_to_position_in_page(uint32_t pos) override {
        DCHECK(_parsed) << "Must call init()";
        DCHECK_LE(pos, _num_elements);
        if (pos > _num_elements) {
            return Status::InternalError(strings::Substitute("invalid pos:$0, num_elements:$1", pos, _num_elements));
        }
        _cur_index = pos;
        return Status::OK();
    }

    Status seek_at_or_after_value(const void* value, bool* exact_match) override {
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

    void at_index(uint32_t idx, CppType* out) const {
        memcpy(out, &_data[ALP_PAGE_HEADER_SIZE + idx * SIZE_OF_TYPE], SIZE_OF_TYPE);
    }

    const void* get_data(size_t pos) { return static_cast<const void*>(&_data[pos + ALP_PAGE_HEADER_SIZE]); }

    Status next_batch(size_t* count, Column* dst) override {
        SparseRange<> read_range;
        uint32_t begin = current_index();
        read_range.add(Range<>(begin, begin + *count));
        RETURN_IF_ERROR(next_batch(read_range, dst));
        *count = current_index() - begin;
        return Status::OK();
    }

    Status next_batch(const SparseRange<>& range, Column* dst) override {
        DCHECK(_parsed);
        if (PREDICT_FALSE(_cur_index >= _num_elements)) {
            return Status::OK();
        }
        size_t to_read =
                std::min(static_cast<size_t>(range.span_size()), static_cast<size_t>(_num_elements - _cur_index));
        SparseRangeIterator<> iter = range.new_iterator();
        while (to_read > 0) {
            _cur_index = iter.begin();
            Range<> r = iter.next(to_read);
            ContainerResource container(_page_handle, get_data(_cur_index * SIZE_OF_TYPE),
                                        r.span_size() * SIZE_OF_TYPE);
            int n = dst->append_numbers(container);
            DCHECK_EQ(r.span_size(), n);
            _cur_index += r.span_size();
            to_read -= r.span_size();
        }
        return Status::OK();
    }

    Status read_by_rowids(const ordinal_t first_ordinal_in_page, const rowid_t* rowids, size_t* count,
                          Column* column) override {
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
                return Status::InternalError(strings::Substitute(
                        "append_numbers failed, expected rows[$0], actual rows[$1]", read_count, nappend));
            }
        }
        *count = read_count;
        return Status::OK();
    }

    uint32_t count() const override { return _num_elements; }

    uint32_t current_index() const override { return _cur_index; }

    EncodingTypePB encoding_type() const override { return ALP_ENCODING; }

    bool supports_read_by_rowids() const override { return true; }

private:
    enum { SIZE_OF_TYPE = StorageCppTypeSize<Type> };

    Slice _data;
    uint32_t _num_elements{0};
    size_t _num_element_after_padding{0};
    size_t _cur_index{0};
    bool _parsed{false};
};

} // namespace starrocks
