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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/rle_page.h

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

#include "column/column.h"
#include "storage/range.h"
#include "storage/rowset/options.h"
#include "storage/rowset/page_builder.h"
#include "storage/rowset/page_decoder.h"
#include "storage/type_traits.h"
#include "util/coding.h"
#include "util/rle_encoding.h"
#include "util/slice.h"

namespace starrocks {

enum { RLE_PAGE_HEADER_SIZE = 4 };

// RLE builder for generic integer and bool types. What is missing is some way
// to enforce that this can only be instantiated for INT and BOOL types.
//
// The page format is as follows:
//
// 1. Header: (4 bytes total)
//
//    <num_elements> [32-bit]
//      The number of elements encoded in the page.
//
//    NOTE: all on-disk ints are encoded little-endian
//
// 2. Element data
//
//    The header is followed by the rle-encoded element data.
//
// This Rle encoding algorithm is only effective for repeated INT type and bool type,
// It is not good for sequence number or random number. BitshufflePage is recommended
// for these case.
//
// TODO(hkp): optimize rle algorithm
template <LogicalType Type>
class RlePageBuilder final : public PageBuilder {
public:
    explicit RlePageBuilder(const PageBuilderOptions& options)
            : _options(options),
              _rle_encoder(nullptr),
              _first_value(0),
              _last_value(0) {
        _bit_width = (Type == TYPE_BOOLEAN) ? 1 : SIZE_OF_TYPE * 8;
        _rle_encoder = std::make_unique<RleEncoder<CppType>>(&_buf, _bit_width);
        reset();
    }

    ~RlePageBuilder() override = default;

    bool is_page_full() override { return _rle_encoder->len() >= _options.data_page_size; }

    uint32_t add(const uint8_t* vals, uint32_t count) override {
        DCHECK(!_finished);
        auto new_vals = reinterpret_cast<const CppType*>(vals);
        for (int i = 0; i < count; ++i) {
            // note: vals is not guaranteed to be aligned for now, thus memcpy here
            CppType value;
            memcpy(&value, &new_vals[i], SIZE_OF_TYPE);
            _rle_encoder->Put(value);
        }

        if (_count == 0) {
            memcpy(&_first_value, new_vals, SIZE_OF_TYPE);
        }
        memcpy(&_last_value, &new_vals[count - 1], SIZE_OF_TYPE);

        _count += count;
        return count;
    }

    faststring* finish() override {
        DCHECK(!_finished);
        _finished = true;
        // here should Flush first and then encode the count header
        // or it will lead to a bug if the header is less than 8 byte and the data is small
        _rle_encoder->Flush();
        encode_fixed32_le(&_buf[0], _count);
        return &_buf;
    }

    void reset() override {
        _count = 0;
        _rle_encoder->Clear();
        _rle_encoder->Reserve(RLE_PAGE_HEADER_SIZE, 0);
    }

    uint32_t count() const override { return _count; }

    uint64_t size() const override { return _rle_encoder->len(); }

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

private:
    typedef typename TypeTraits<Type>::CppType CppType;
    enum { SIZE_OF_TYPE = TypeTraits<Type>::size };

    PageBuilderOptions _options;
    uint32_t _count{0};
    bool _finished{false};
    int _bit_width{0};
    std::unique_ptr<RleEncoder<CppType>> _rle_encoder;
    faststring _buf;
    CppType _first_value;
    CppType _last_value;
};

template <LogicalType Type>
class RlePageDecoder final : public PageDecoder {
public:
    RlePageDecoder(Slice slice) : _data(slice) {}

    Status init() override {
        CHECK(!_parsed);

        if (_data.size < RLE_PAGE_HEADER_SIZE) {
            return Status::Corruption("not enough bytes for header in RleBitMapBlockDecoder");
        }
        _num_elements = decode_fixed32_le((const uint8_t*)&_data[0]);
        _parsed = true;
        _bit_width = (Type == TYPE_BOOLEAN) ? 1 : SIZE_OF_TYPE * 8;
        _rle_decoder = RleDecoder<CppType>((uint8_t*)_data.data + RLE_PAGE_HEADER_SIZE,
                                           _data.size - RLE_PAGE_HEADER_SIZE, _bit_width);
        seek_to_position_in_page(0);
        return Status::OK();
    }

    Status seek_to_position_in_page(uint32_t pos) override {
        DCHECK(_parsed) << "Must call init()";
        DCHECK_LE(pos, _num_elements) << "Tried to seek to " << pos << " which is > number of elements ("
                                      << _num_elements << ") in the block!";
        // If the block is empty (e.g. the column is filled with nulls), there is no data to seek.
        if (PREDICT_FALSE(_num_elements == 0)) {
            return Status::OK();
        }
        if (_cur_index == pos) {
            // No need to seek.
            return Status::OK();
        } else if (_cur_index < pos) {
            uint nskip = pos - _cur_index;
            _rle_decoder.Skip(nskip);
        } else {
            _rle_decoder = RleDecoder<CppType>((uint8_t*)_data.data + RLE_PAGE_HEADER_SIZE,
                                               _data.size - RLE_PAGE_HEADER_SIZE, _bit_width);
            _rle_decoder.Skip(pos);
        }
        _cur_index = pos;
        return Status::OK();
    }

    Status next_batch(size_t* n, Column* dst) override {
        SparseRange<> read_range;
        uint32_t begin = current_index();
        read_range.add(Range<>(begin, begin + *n));
        RETURN_IF_ERROR(next_batch(read_range, dst));
        *n = current_index() - begin;
        return Status::OK();
    }

    Status next_batch(const SparseRange<>& range, Column* dst) override {
        DCHECK(_parsed);
        if (PREDICT_FALSE(_cur_index >= _num_elements)) {
            return Status::OK();
        }
        CppType value{};

        size_t to_read =
                std::min(static_cast<size_t>(range.span_size()), static_cast<size_t>(_num_elements - _cur_index));
        SparseRangeIterator<> iter = range.new_iterator();
        while (to_read > 0) {
            seek_to_position_in_page(iter.begin());
            Range<> r = iter.next(to_read);
            for (size_t i = 0; i < r.span_size(); ++i) {
                if (PREDICT_FALSE(!_rle_decoder.Get(&value))) {
                    return Status::Corruption("RLE decode failed");
                }
                [[maybe_unused]] int p = dst->append_numbers(&value, sizeof(value));
                DCHECK_EQ(1, p);
            }
            _cur_index += r.span_size();
            to_read -= r.span_size();
        }
        return Status::OK();
    }

    uint32_t count() const override { return _num_elements; }

    uint32_t current_index() const override { return _cur_index; }

    EncodingTypePB encoding_type() const override { return RLE; }

private:
    typedef typename TypeTraits<Type>::CppType CppType;
    enum { SIZE_OF_TYPE = TypeTraits<Type>::size };

    Slice _data;
    bool _parsed{false};
    uint32_t _num_elements{0};
    uint32_t _cur_index{0};
    int _bit_width{0};
    RleDecoder<CppType> _rle_decoder;
};

} // namespace starrocks
