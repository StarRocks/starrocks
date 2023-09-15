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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/binary_prefix_page.h

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

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <map>
#include <string>
#include <vector>

#include "runtime/mem_pool.h"
#include "storage/range.h"
#include "storage/rowset/options.h"
#include "storage/rowset/page_builder.h"
#include "storage/rowset/page_decoder.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace starrocks {

// prefix encoding for string dictionary
//
// BinaryPrefixPage := Entry^EntryNum, Trailer
// Entry := SharedPrefixLength(vint), UnsharedLength(vint), Byte^UnsharedLength
// Trailer := NumEntry(uint32_t), RESTART_POINT_INTERVAL(uint8_t)
//            RestartPointStartOffset(uint32_t)^NumRestartPoints,NumRestartPoints(uint32_t)
class BinaryPrefixPageBuilder final : public PageBuilder {
public:
    BinaryPrefixPageBuilder(const PageBuilderOptions& options) : _options(options) { reset(); }

    bool is_page_full() override { return size() >= _options.data_page_size; }

    uint32_t add(const uint8_t* vals, uint32_t add_count) override;

    faststring* finish() override;

    void reset() override {
        _restart_points_offset.clear();
        _last_entry.clear();
        _count = 0;
        _buffer.clear();
        _finished = false;
    }

    uint64_t size() const override {
        if (_finished) {
            return _buffer.size();
        } else {
            return _buffer.size() + (_restart_points_offset.size() + 2) * sizeof(uint32_t);
        }
    }

    uint32_t count() const override { return _count; }

    Status get_first_value(void* value) const override {
        DCHECK(_finished);
        if (_count == 0) {
            return Status::NotFound("page is empty");
        }
        *reinterpret_cast<Slice*>(value) = Slice(_first_entry);
        return Status::OK();
    }

    Status get_last_value(void* value) const override {
        DCHECK(_finished);
        if (_count == 0) {
            return Status::NotFound("page is empty");
        }
        *reinterpret_cast<Slice*>(value) = Slice(_last_entry);
        return Status::OK();
    }

private:
    PageBuilderOptions _options;
    std::vector<uint32_t> _restart_points_offset;
    faststring _first_entry;
    faststring _last_entry;
    uint32_t _count = 0;
    bool _finished = false;
    faststring _buffer;
    // This is a empirical value, Kudu and LevelDB use this default value
    static const uint8_t RESTART_POINT_INTERVAL = 16;
};

template <LogicalType Type>
class BinaryPrefixPageDecoder final : public PageDecoder {
public:
    BinaryPrefixPageDecoder(Slice data) : _data(data) {}

    [[nodiscard]] Status init() override;

    [[nodiscard]] Status seek_to_position_in_page(uint32_t pos) override;

    [[nodiscard]] Status seek_at_or_after_value(const void* value, bool* exact_match) override;

    [[nodiscard]] Status next_batch(size_t* n, Column* dst) override;

    [[nodiscard]] Status next_batch(const SparseRange<>& range, Column* dst) override;

    uint32_t count() const override {
        DCHECK(_parsed);
        return _num_values;
    }

    uint32_t current_index() const override {
        DCHECK(_parsed);
        return _cur_pos;
    }

    EncodingTypePB encoding_type() const override { return PREFIX_ENCODING; }

private:
    // decode shared and non-shared entry length from `ptr`.
    // return ptr past the parsed value when success.
    // return nullptr on failure
    const uint8_t* _decode_value_lengths(const uint8_t* ptr, uint32_t* shared, uint32_t* non_shared);

    // return start pointer of the restart point at index `restart_point_index`
    const uint8_t* _get_restart_point(size_t restart_point_index) const {
        return reinterpret_cast<const uint8_t*>(_data.get_data()) +
               decode_fixed32_le(_restarts_ptr + restart_point_index * sizeof(uint32_t));
    }

    // read next value at `_cur_pos` and `_next_ptr` into `_current_value`.
    // return OK and advance `_next_ptr` on success. `_cur_pos` is not modified.
    // return NotFound when no more entry can be read.
    // return other error status otherwise.
    Status _read_next_value();

    // seek to the first value at the given restart point
    Status _seek_to_restart_point(uint32_t restart_point_index);

    Status _read_next_value_to_output(Slice prev, MemPool* mem_pool, Slice* output);

    Status _copy_current_to_output(MemPool* mem_pool, Slice* output);

    Status _next_value(faststring* value);

    Slice _data;
    bool _parsed = false;
    uint32_t _num_values = 0;
    uint8_t _restart_point_internal = 0;
    uint32_t _num_restarts = 0;
    // pointer to _footer start
    const uint8_t* _footer_start = nullptr;
    // pointer to restart offsets array
    const uint8_t* _restarts_ptr = nullptr;
    // ordinal of the first value to return in next_batch()
    uint32_t _cur_pos = 0;
    // first value to return in next_batch()
    faststring _current_value;
    // pointer to the start of next value to read, advanced by `_read_next_value`
    const uint8_t* _next_ptr = nullptr;
};

} // namespace starrocks
