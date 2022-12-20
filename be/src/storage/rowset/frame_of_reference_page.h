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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/frame_of_reference_page.h

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
#include "storage/rowset/options.h"      // for PageBuilderOptions/PageDecoderOptions
#include "storage/rowset/page_builder.h" // for PageBuilder
#include "storage/rowset/page_decoder.h" // for PageDecoder
#include "storage/type_traits.h"
#include "util/frame_of_reference_coding.h"

namespace starrocks {

// Encode page use frame-of-reference coding
template <LogicalType Type>
class FrameOfReferencePageBuilder final : public PageBuilder {
public:
    explicit FrameOfReferencePageBuilder(const PageBuilderOptions& options) : _options(options), _encoder(&_buf) {}

    ~FrameOfReferencePageBuilder() override = default;

    bool is_page_full() override { return _encoder.len() >= _options.data_page_size; }

    uint32_t add(const uint8_t* vals, uint32_t count) override {
        DCHECK(!_finished);
        if (count == 0) {
            return 0;
        }
        auto new_vals = reinterpret_cast<const CppType*>(vals);
        if (_count == 0) {
            _first_val = *new_vals;
        }
        _encoder.put_batch(new_vals, count);
        _count += count;
        _last_val = new_vals[count - 1];
        return count;
    }

    faststring* finish() override {
        DCHECK(!_finished);
        _finished = true;
        _encoder.flush();
        return &_buf;
    }

    void reset() override {
        _count = 0;
        _finished = false;
        _encoder.clear();
    }

    uint32_t count() const override { return _count; }

    uint64_t size() const override { return _buf.size(); }

    Status get_first_value(void* value) const override {
        if (_count == 0) {
            return Status::NotFound("page is empty");
        }
        memcpy(value, &_first_val, sizeof(CppType));
        return Status::OK();
    }

    Status get_last_value(void* value) const override {
        if (_count == 0) {
            return Status::NotFound("page is empty");
        }
        memcpy(value, &_last_val, sizeof(CppType));
        return Status::OK();
    }

private:
    typedef typename TypeTraits<Type>::CppType CppType;
    PageBuilderOptions _options;
    uint32_t _count{0};
    bool _finished{false};
    faststring _buf;
    CppType _first_val;
    CppType _last_val;
    ForEncoder<CppType> _encoder;
};

template <LogicalType Type>
class FrameOfReferencePageDecoder final : public PageDecoder {
public:
    FrameOfReferencePageDecoder(Slice data, const PageDecoderOptions& options)
            : _data(data),

              _decoder((uint8_t*)_data.data, _data.size) {}

    ~FrameOfReferencePageDecoder() override = default;

    Status init() override {
        CHECK(!_parsed);
        bool result = _decoder.init();
        if (result) {
            _num_elements = _decoder.count();
            _parsed = true;
            return Status::OK();
        } else {
            return Status::Corruption("The frame of reference page metadata maybe broken");
        }
    }

    Status seek_to_position_in_page(uint32_t pos) override {
        DCHECK(_parsed) << "Must call init() firstly";
        DCHECK_LE(pos, _num_elements) << "Tried to seek to " << pos << " which is > number of elements ("
                                      << _num_elements << ") in the block!";
        // If the block is empty (e.g. the column is filled with nulls), there is no data to seek.
        if (PREDICT_FALSE(_num_elements == 0)) {
            return Status::OK();
        }

        int32_t skip_num = pos - _cur_index;
        _decoder.skip(skip_num);
        _cur_index = pos;
        return Status::OK();
    }

    Status seek_at_or_after_value(const void* value, bool* exact_match) override {
        DCHECK(_parsed) << "Must call init() firstly";
        bool found = _decoder.seek_at_or_after_value(value, exact_match);
        if (!found) {
            return Status::NotFound("not found");
        }
        _cur_index = _decoder.current_index();
        return Status::OK();
    }

    Status next_batch(size_t* n, Column* dst) override {
        SparseRange read_range;
        uint32_t begin = current_index();
        read_range.add(Range(begin, begin + *n));
        RETURN_IF_ERROR(next_batch(read_range, dst));
        *n = current_index() - begin;
        return Status::OK();
    }

    Status next_batch(const SparseRange& range, Column* dst) override {
        DCHECK(_parsed) << "Must call init() firstly";
        if (PREDICT_FALSE(range.span_size() == 0 || _cur_index >= _num_elements)) {
            return Status::OK();
        }

        // clang-format off
        static_assert(Type == TYPE_TINYINT ||
                      Type == TYPE_SMALLINT ||
                      Type == TYPE_INT ||
                      Type == TYPE_BIGINT ||
                      Type == TYPE_LARGEINT ||
                      Type == TYPE_DATE_V1 ||
                      Type == TYPE_DATE ||
                      Type == TYPE_DATETIME_V1 ||
                      Type == TYPE_DATETIME ||
                      Type == TYPE_DECIMALV2 ||
                      Type == TYPE_DECIMAL32 ||
                      Type == TYPE_DECIMAL64 ||
                      Type == TYPE_DECIMAL128,
                      "unexpected field type");
        // clang-format on
        size_t to_read =
                std::min(static_cast<size_t>(range.span_size()), static_cast<size_t>(_num_elements - _cur_index));
        SparseRangeIterator iter = range.new_iterator();
        while (to_read > 0 && _cur_index < _num_elements) {
            seek_to_position_in_page(iter.begin());
            Range r = iter.next(to_read);
            const size_t ori_size = dst->size();
            dst->resize(ori_size + r.span_size());
            auto* p = reinterpret_cast<CppType*>(dst->mutable_raw_data()) + ori_size;
            bool res = _decoder.get_batch(p, r.span_size());
            DCHECK(res);
            _cur_index += r.span_size();
            to_read -= r.span_size();
        }
        return Status::OK();
    }

    uint32_t count() const override { return _num_elements; }

    uint32_t current_index() const override { return _cur_index; }

    EncodingTypePB encoding_type() const override { return FOR_ENCODING; }

private:
    typedef typename TypeTraits<Type>::CppType CppType;

    bool _parsed{false};
    Slice _data;
    uint32_t _num_elements{0};
    uint32_t _cur_index{0};
    ForDecoder<CppType> _decoder;
};

} // namespace starrocks
