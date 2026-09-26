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

#include <cstdint>
#include <cstring>

#include "base/string/faststring.h"
#include "base/string/slice.h"
#include "common/status.h"
#include "storage/olap_common.h"
#include "storage/rowset/common.h"
#include "storage/rowset/options.h"
#include "storage/rowset/page_builder.h"
#include "storage/rowset/page_decoder.h"
#include "types/storage_type_traits.h"

namespace starrocks {

// ALP (Adaptive Lossless floating-Point, cwida/ALP, built as the thirdparty
// library libalp.a) column encoding for FLOAT/DOUBLE data pages.
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
// (appended). Implemented in alp_page.cpp so that the ALP library headers
// stay out of this widely-included header.
template <typename PT>
void alp_encode_body(const PT* vals, size_t num_padded, faststring* out);

// Decode an encoded body back into |out| (capacity num_valid values). The
// tail vector's padding values are decoded into scratch space and dropped so
// that partial pages do not retain padded bytes in the page cache.
template <typename PT>
Status alp_decode_body(const uint8_t* body, size_t body_size, size_t num_padded, size_t num_valid, PT* out);

} // namespace alppage

// Method bodies live in alp_page.cpp (with explicit instantiations for
// TYPE_FLOAT and TYPE_DOUBLE) so that line coverage is attributed to a
// translation unit.
template <LogicalType Type>
class AlpPageBuilder final : public PageBuilder {
    using CppType = StorageCppType<Type>;
    static_assert(Type == TYPE_FLOAT || Type == TYPE_DOUBLE, "ALP encoding only supports FLOAT/DOUBLE");

public:
    explicit AlpPageBuilder(const PageBuilderOptions& options);

    void reserve_head(uint8_t head_size) override;

    bool is_page_full() override { return _count >= _max_count; }

    uint32_t add(const uint8_t* vals, uint32_t count) override;

    faststring* finish() override;

    void reset() override;

    uint32_t count() const override { return _count; }

    uint64_t size() const override { return _data.size(); }

    Status get_first_value(void* value) const override;

    Status get_last_value(void* value) const override;

    CppType cell(int idx) const;

private:
    faststring* _finish();

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

    Status init() override;

    Status seek_to_position_in_page(uint32_t pos) override;

    Status seek_at_or_after_value(const void* value, bool* exact_match) override;

    void at_index(uint32_t idx, CppType* out) const {
        memcpy(out, &_data[ALP_PAGE_HEADER_SIZE + idx * SIZE_OF_TYPE], SIZE_OF_TYPE);
    }

    const void* get_data(size_t pos) { return static_cast<const void*>(&_data[pos + ALP_PAGE_HEADER_SIZE]); }

    Status next_batch(size_t* count, Column* dst) override;

    Status next_batch(const SparseRange<>& range, Column* dst) override;

    Status read_by_rowids(const ordinal_t first_ordinal_in_page, const rowid_t* rowids, size_t* count,
                          Column* column) override;

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
