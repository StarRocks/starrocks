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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/plain_page.h

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
#include "storage/olap_common.h"
#include "storage/range.h"
#include "storage/rowset/options.h"
#include "storage/rowset/page_builder.h"
#include "storage/rowset/page_decoder.h"
#include "storage/type_traits.h"
#include "storage/types.h"
#include "util/coding.h"
#include "util/faststring.h"

namespace starrocks {

static const size_t PLAIN_PAGE_HEADER_SIZE = sizeof(uint32_t);

template <LogicalType Type>
class PlainPageBuilder final : public PageBuilder {
public:
    PlainPageBuilder(const PageBuilderOptions& options) : _options(options) {
        // Reserve enough space for the page, plus a bit of slop since
        // we often overrun the page by a few values.
        _buffer.reserve(_options.data_page_size + 1024);
        _max_count = _options.data_page_size / SIZE_OF_TYPE;
        reset();
    }

    bool is_page_full() override { return _buffer.size() > _options.data_page_size; }

    uint32_t add(const uint8_t* vals, uint32_t count) override {
        if (is_page_full()) {
            return 0;
        }
        size_t old_size = _buffer.size();
        uint32_t to_add = std::min(_max_count - _count, count);
        _buffer.resize(old_size + to_add * SIZE_OF_TYPE);
        memcpy(&_buffer[old_size], vals, to_add * SIZE_OF_TYPE);
        _count += to_add;
        return to_add;
    }

    faststring* finish() override {
        encode_fixed32_le((uint8_t*)&_buffer[0], _count);
        if (_count > 0) {
            _first_value.assign_copy(&_buffer[PLAIN_PAGE_HEADER_SIZE], SIZE_OF_TYPE);
            _last_value.assign_copy(&_buffer[PLAIN_PAGE_HEADER_SIZE + (_count - 1) * SIZE_OF_TYPE], SIZE_OF_TYPE);
        }
        return &_buffer;
    }

    void reset() override {
        _buffer.reserve(_options.data_page_size + 1024);
        _count = 0;
        _buffer.clear();
        _buffer.resize(PLAIN_PAGE_HEADER_SIZE);
    }

    uint32_t count() const override { return _count; }

    uint64_t size() const override { return _buffer.size(); }

    Status get_first_value(void* value) const override {
        if (_count == 0) {
            return Status::NotFound("page is empty");
        }
        memcpy(value, _first_value.data(), SIZE_OF_TYPE);
        return Status::OK();
    }

    Status get_last_value(void* value) const override {
        if (_count == 0) {
            return Status::NotFound("page is empty");
        }
        memcpy(value, _last_value.data(), SIZE_OF_TYPE);
        return Status::OK();
    }

private:
    faststring _buffer;
    PageBuilderOptions _options;
    uint32_t _count;
    uint32_t _max_count;
    typedef typename TypeTraits<Type>::CppType CppType;
    enum { SIZE_OF_TYPE = TypeTraits<Type>::size };
    faststring _first_value;
    faststring _last_value;
};

template <LogicalType Type>
class PlainPageDecoder : public PageDecoder {
public:
    PlainPageDecoder(Slice data) : _data(data) {}

    Status init() override {
        CHECK(!_parsed);

        if (_data.size < PLAIN_PAGE_HEADER_SIZE) {
            std::stringstream ss;
            ss << "file corruption: not enough bytes for header in PlainPageDecoder ."
                  "invalid data size:"
               << _data.size << ", header size:" << PLAIN_PAGE_HEADER_SIZE;
            return Status::InternalError(ss.str());
        }

        _num_elems = decode_fixed32_le((const uint8_t*)&_data[0]);

        if (_data.size != PLAIN_PAGE_HEADER_SIZE + _num_elems * SIZE_OF_TYPE) {
            std::stringstream ss;
            ss << "file corruption: unexpected data size.";
            return Status::InternalError(ss.str());
        }

        _parsed = true;

        seek_to_position_in_page(0);
        return Status::OK();
    }

    Status seek_to_position_in_page(uint32_t pos) override {
        CHECK(_parsed) << "Must call init()";

        if (PREDICT_FALSE(_num_elems == 0)) {
            DCHECK_EQ(0, pos);
            return Status::InternalError("invalid pos");
        }

        DCHECK_LE(pos, _num_elems);

        _cur_idx = pos;
        return Status::OK();
    }

    Status seek_at_or_after_value(const void* value, bool* exact_match) override {
        DCHECK(_parsed) << "Must call init() firstly";

        if (_num_elems == 0) {
            return Status::NotFound("page is empty");
        }

        size_t left = 0;
        size_t right = _num_elems;

        const void* mid_value = nullptr;

        // find the first value >= target. after loop,
        // - left == index of first value >= target when found
        // - left == _num_elems when not found (all values < target)
        while (left < right) {
            size_t mid = left + (right - left) / 2;
            mid_value = &_data[PLAIN_PAGE_HEADER_SIZE + mid * SIZE_OF_TYPE];
            if (TypeComparator<Type>::cmp(mid_value, value) < 0) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        if (left >= _num_elems) {
            return Status::NotFound("all value small than the value");
        }
        const void* find_value = &_data[PLAIN_PAGE_HEADER_SIZE + left * SIZE_OF_TYPE];
        if (TypeComparator<Type>::cmp(find_value, value) == 0) {
            *exact_match = true;
        } else {
            *exact_match = false;
        }

        _cur_idx = left;
        return Status::OK();
    }

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

        size_t to_read = range.span_size();
        if (PREDICT_FALSE(to_read == 0 || _cur_idx >= _num_elems)) {
            return Status::OK();
        }

        SparseRangeIterator<> iter = range.new_iterator();
        while (iter.has_more() && _cur_idx < _num_elems) {
            _cur_idx = iter.begin();
            Range<> r = iter.next(to_read);
            uint32_t max_fetch = std::min(r.span_size(), _num_elems - _cur_idx);
            int n = dst->append_numbers(&_data[PLAIN_PAGE_HEADER_SIZE + _cur_idx * SIZE_OF_TYPE],
                                        max_fetch * SIZE_OF_TYPE);
            DCHECK_EQ(max_fetch, n);
            _cur_idx += max_fetch;
        }
        return Status::OK();
    }

    uint32_t count() const override {
        DCHECK(_parsed);
        return _num_elems;
    }

    uint32_t current_index() const override {
        DCHECK(_parsed);
        return _cur_idx;
    }

    EncodingTypePB encoding_type() const override { return PLAIN_ENCODING; }

private:
    Slice _data;
    bool _parsed{false};
    uint32_t _num_elems{0};
    uint32_t _cur_idx{0};
    typedef typename TypeTraits<Type>::CppType CppType;
    enum { SIZE_OF_TYPE = TypeTraits<Type>::size };
};

} // namespace starrocks
