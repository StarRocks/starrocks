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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/binary_plain_page.h

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

// Simplistic page encoding for strings.
//
// The page consists of:
// Strings:
//   raw strings that were written
// Trailer
//  Offsets:
//    offsets pointing to the beginning of each string
//  num_elems (32-bit fixed)
//

#pragma once

#include <cstdint>

#include "common/logging.h"
#include "runtime/mem_pool.h"
#include "storage/olap_common.h"
#include "storage/range.h"
#include "storage/rowset/options.h"
#include "storage/rowset/page_builder.h"
#include "storage/rowset/page_decoder.h"
#include "storage/types.h"
#include "util/coding.h"
#include "util/faststring.h"

namespace starrocks {
class Column;
}

namespace starrocks {

class BinaryPlainPageBuilder final : public PageBuilder {
public:
    explicit BinaryPlainPageBuilder(const PageBuilderOptions& options) : _options(options) { reset(); }

    void reserve_head(uint8_t head_size) override {
        CHECK_EQ(0, _reserved_head_size);
        _reserved_head_size = head_size;
        _buffer.resize(_reserved_head_size);
    }

    bool is_page_full() override {
        // data_page_size is 0, do not limit the page size
        return (_options.data_page_size != 0) & (_size_estimate > _options.data_page_size);
    }

    uint32_t add(const uint8_t* vals, uint32_t count) override {
        DCHECK(!_finished);
        const auto* slices = reinterpret_cast<const Slice*>(vals);
        for (auto i = 0; i < count; i++) {
            if (!add_slice(slices[i])) {
                return i;
            }
        }
        return count;
    }

    bool add_slice(const Slice& s) {
        if (is_page_full()) {
            return false;
        }
        DCHECK_EQ(_buffer.size(), _reserved_head_size + _next_offset);
        _offsets.push_back(_next_offset);
        _buffer.append(s.data, s.size);

        _next_offset += s.size;
        _size_estimate += s.size;
        _size_estimate += sizeof(uint32_t);
        return true;
    }

    faststring* finish() override {
        DCHECK(!_finished);
        DCHECK_EQ(_next_offset + _reserved_head_size, _buffer.size());
        _buffer.reserve(_size_estimate);
        // Set up trailer
        for (uint32_t _offset : _offsets) {
            put_fixed32_le(&_buffer, _offset);
        }
        put_fixed32_le(&_buffer, _offsets.size());
        if (!_offsets.empty()) {
            _copy_value_at(0, &_first_value);
            _copy_value_at(_offsets.size() - 1, &_last_value);
        }
        _finished = true;
        return &_buffer;
    }

    void reset() override {
        _offsets.clear();
        _buffer.reserve(_options.data_page_size == 0 ? 65536 : _options.data_page_size);
        _buffer.resize(_reserved_head_size);
        _next_offset = 0;
        _size_estimate = sizeof(uint32_t);
        _finished = false;
    }

    uint32_t count() const override { return _offsets.size(); }

    uint64_t size() const override { return _size_estimate; }

    Status get_first_value(void* value) const override {
        DCHECK(_finished);
        if (_offsets.empty()) {
            return Status::NotFound("page is empty");
        }
        *reinterpret_cast<Slice*>(value) = Slice(_first_value);
        return Status::OK();
    }

    Status get_last_value(void* value) const override {
        DCHECK(_finished);
        if (_offsets.empty()) {
            return Status::NotFound("page is empty");
        }
        *reinterpret_cast<Slice*>(value) = Slice(_last_value);
        return Status::OK();
    }

    Slice get_value(size_t idx) const {
        DCHECK(!_finished);
        DCHECK_LT(idx, _offsets.size());
        size_t end = (idx + 1) < _offsets.size() ? _offsets[idx + 1] : _next_offset;
        size_t off = _offsets[idx];
        return {&_buffer[_reserved_head_size + off], end - off};
    }

private:
    void _copy_value_at(size_t idx, faststring* value) const {
        Slice s = get_value(idx);
        value->assign_copy((const uint8_t*)s.data, s.size);
    }

    uint8_t _reserved_head_size{0};
    size_t _size_estimate{0};
    size_t _next_offset{0};
    faststring _buffer;
    // Offsets of each entry, relative to the start of the page
    std::vector<uint32_t> _offsets;
    PageBuilderOptions _options;
    faststring _first_value;
    faststring _last_value;
    bool _finished{false};
};

template <LogicalType Type>
class BinaryPlainPageDecoder final : public PageDecoder {
public:
    explicit BinaryPlainPageDecoder(Slice data)
            : _data(data), _parsed(false), _num_elems(0), _offsets_pos(0), _cur_idx(0) {}

    [[nodiscard]] Status init() override {
        RETURN_IF(_parsed, Status::OK());

        if (_data.size < sizeof(uint32_t)) {
            std::stringstream ss;
            ss << "file corruption: not enough bytes for trailer in BinaryPlainPageDecoder ."
                  "invalid data size:"
               << _data.size << ", trailer size:" << sizeof(uint32_t);
            return Status::Corruption(ss.str());
        }

        // Decode trailer
        _num_elems = decode_fixed32_le((const uint8_t*)&_data[_data.get_size() - sizeof(uint32_t)]);
        _offsets_pos =
                static_cast<uint32_t>(_data.get_size()) - (_num_elems + 1) * static_cast<uint32_t>(sizeof(uint32_t));
        _offsets_ptr = reinterpret_cast<uint32_t*>(_data.data + _offsets_pos);

        _parsed = true;

        return Status::OK();
    }

    [[nodiscard]] Status seek_to_position_in_page(uint32_t pos) override {
        DCHECK_LE(pos, _num_elems);
        _cur_idx = pos;
        return Status::OK();
    }

    [[nodiscard]] Status next_batch(size_t* count, Column* dst) override;

    [[nodiscard]] Status next_batch(const SparseRange<>& range, Column* dst) override;

    bool append_range(uint32_t idx, uint32_t end, Column* dst) const;

    uint32_t count() const override {
        DCHECK(_parsed);
        return _num_elems;
    }

    uint32_t current_index() const override {
        DCHECK(_parsed);
        return _cur_idx;
    }

    EncodingTypePB encoding_type() const override { return PLAIN_ENCODING; }

    Slice string_at_index(uint32_t idx) const {
        const uint32_t start_offset = offset(idx);
        uint32_t len = offset(static_cast<int>(idx) + 1) - start_offset;
        return {&_data[start_offset], len};
    }

    int find(const Slice& word) const {
        DCHECK(_parsed);
        for (uint32_t i = 0; i < _num_elems; i++) {
            const uint32_t off1 = offset_uncheck(i);
            const uint32_t off2 = offset(i + 1);
            Slice s(&_data[off1], off2 - off1);
            if (s == word) {
                return i;
            }
        }
        return -1;
    }

    uint32_t max_value_length() const {
        uint32_t max_length = 0;
        for (int i = 0; i < _num_elems; ++i) {
            uint32_t length = offset(i + 1) - offset_uncheck(i);
            if (length > max_length) {
                max_length = length;
            }
        }
        return max_length;
    }

    uint32_t dict_size() { return _num_elems; }

private:
    // Return the offset within '_data' where the string value with index 'idx' can be found.
    uint32_t offset(int idx) const { return idx < _num_elems ? offset_uncheck(idx) : _offsets_pos; }

    uint32_t offset_uncheck(int idx) const {
#if __BYTE_ORDER == __LITTLE_ENDIAN
        return _offsets_ptr[idx];
#else
        const uint32_t pos = _offsets_pos + idx * static_cast<uint32_t>(sizeof(uint32_t));
        const auto* const p = reinterpret_cast<const uint8_t*>(&_data[pos]);
        return decode_fixed32_le(p);
#endif
    }

    Slice _data;
    bool _parsed;

    uint32_t _num_elems;
    uint32_t _offsets_pos;
    uint32_t* _offsets_ptr = nullptr;

    // Index of the currently seeked element in the page.
    uint32_t _cur_idx;
};

} // namespace starrocks
