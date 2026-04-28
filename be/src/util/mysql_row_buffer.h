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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/mysql_row_buffer.h

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

#include "storage/uint24.h"
#include "types/date_value.hpp"
#include "types/timestamp_value.h"
#include "util/raw_container.h"
#include "util/slice.h"

namespace starrocks {

// Options controlling how MysqlRowBuffer serializes certain types.
// Currently only affects how BINARY / VARBINARY data is serialized.
struct MysqlRowBufferOptions {
    enum class BinaryEncodingFormat {
        RAW,    // do not encode; preserve the original bytes
        HEX,    // encode as lowercase hex string, e.g. "48656c6c6f"  (default)
        BASE64, // encode as standard base64 string, e.g. "SGVsbG8="
    };

    enum class BinaryEncodingLevel {
        ALL,
        NESTED,
    };

    BinaryEncodingFormat binary_encoding_format = BinaryEncodingFormat::HEX;
    BinaryEncodingLevel binary_encoding_level = BinaryEncodingLevel::NESTED;
};

// Reference:
//   https://dev.mysql.com/doc/internals/en/com-query-response.html#text-resultset-row
class MysqlRowBuffer final {
public:
    MysqlRowBuffer() = default;
    MysqlRowBuffer(bool is_binary_format) : _is_binary_format(is_binary_format) {}
    MysqlRowBuffer(bool is_binary_format, MysqlRowBufferOptions options)
            : _is_binary_format(is_binary_format), _options(options) {}

    const MysqlRowBufferOptions& options() const { return _options; }
    ~MysqlRowBuffer() = default;

    void reset() { _data.clear(); }

    void start_binary_row(uint32_t num_cols);

    void push_null(bool is_binary_protocol = false);
    void push_tinyint(int8_t data) { push_number(data); }
    void push_smallint(int16_t data) { push_number(data); }
    void push_int(int32_t data) { push_number(data); }
    void push_bigint(int64_t data) { push_number(data); }
    void push_largeint(__int128 data) { push_number(data); }
    void push_float(float data) { push_number(data); }
    void push_double(double data) { push_number(data); }
    void push_string(const char* str, size_t length, char escape_char = '"');
    void push_string(const Slice& s) { push_string(s.data, s.size); }

    // Serialize raw binary data (BINARY / VARBINARY). Encoding behavior depends on options():
    // - level = nested: encode only inside nested types.
    // - level = all: encode both top-level and nested binary values.
    // - format = raw: preserve the original bytes and do not hex/base64 encode.
    void push_binary(const char* data, size_t length);

    template <typename T>
    void push_number(T data, bool is_binary_protocol = false);
    void push_number(uint24_t data) { push_number((uint32_t)data); }

    template <typename T>
    void push_number_binary_format(T data);

    void push_decimal(const Slice& s);

    void push_date(const DateValue& data, bool is_binary_protocol = false);
    void push_timestamp(const TimestampValue& data, bool is_binary_protocol = false);

    void begin_push_array() { _enter_scope('['); }
    void finish_push_array() { _leave_scope(']'); }

    void begin_push_bracket() { _enter_scope('{'); }
    void finish_push_bracket() { _leave_scope('}'); }

    void separator(char c);

    int length() const { return _data.size(); }

    // move content into |dst| and clear this buffer.
    void move_content(std::string* dst) {
        dst->swap(reinterpret_cast<std::string&>(_data));
        _data.clear();
    }

    const std::string& data() const { return reinterpret_cast<const std::string&>(_data); }

    void reserve(size_t count) { _data.reserve(count); }
    void update_field_pos() { _field_pos++; }

private:
    char* _resize_extra(size_t n) {
        const size_t old_sz = _data.size();
        _data.resize(old_sz + n);
        return _data.data() + old_sz;
    }

    void _enter_scope(char c);
    void _leave_scope(char c);
    size_t _length_after_escape(const char* str, size_t length, char escape_char = '"');
    char* _escape(char* dst, const char* src, size_t length, char escape_char = '"');
    void _push_string_normal(const char* str, size_t lenght);

    raw::RawString _data;
    // Tracks how many nested scopes (ARRAY / MAP / STRUCT) are currently open.
    // 0 means top-level; > 0 means inside at least one nested type.
    uint32_t _nesting_level = 0;
    uint32_t _array_offset = 0;

    bool _is_binary_format = false;
    // used for calculate null position if is_binary_format = true
    uint32_t _field_pos = 0;
    MysqlRowBufferOptions _options;
};

} // namespace starrocks
