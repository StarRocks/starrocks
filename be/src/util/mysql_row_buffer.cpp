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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/mysql_row_buffer.cpp

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

#include "util/mysql_row_buffer.h"

#include <fmt/compile.h>
#include <fmt/format.h>
#include <ryu/ryu.h>

#include <cstdio>
#include <type_traits>

#include "common/logging.h"
#include "gutil/strings/fastmem.h"
#include "types/large_int_value.h"
#include "util/mysql_global.h"

namespace starrocks {

// the first byte:
// <= 250: length
// = 251: NULL
// = 252: the next two byte is length
// = 253: the next three byte is length
// = 254: the next eighth byte is length
static uint8_t* pack_vlen(uint8_t* packet, uint64_t length) {
    if (length < 251ULL) {
        int1store(packet, length);
        return packet + 1;
    }

    /* 251 is reserved for NULL */
    if (length < 65536ULL) {
        *packet++ = 252;
        int2store(packet, length);
        return packet + 2;
    }

    if (length < 16777216ULL) {
        *packet++ = 253;
        int3store(packet, length);
        return packet + 3;
    }

    *packet++ = 254;
    int8store(packet, length);
    return packet + 8;
}

void MysqlRowBuffer::push_null(bool is_binary_protocol) {
    if (is_binary_protocol) {
        uint offset = (_field_pos + 2) / 8 + 1;
        uint bit = (1 << ((_field_pos + 2) & 7));
        /* Room for this as it's allocated start_binary_row*/
        char* to = _data.data() + offset;
        *to = (char)((uchar)*to | (uchar)bit);
        update_field_pos();
        return;
    }

    if (_array_level == 0) {
        _data.push_back(0xfb);
    } else {
        // lowercase 'null' is more convenient for JSON parsing
        _data.append("null");
    }
}

template <typename T>
void MysqlRowBuffer::push_number_binary_format(T data) {
    if constexpr (std::is_same_v<T, float>) {
        char buff[4];
        float4store(buff, data);
        _data.append(buff, 4);
    } else if constexpr (std::is_same_v<T, double>) {
        char buff[8];
        float8store(buff, data);
        _data.append(buff, 8);
    } else if constexpr (std::is_same_v<std::make_signed_t<T>, int8_t>) {
        char buff[1];
        int1store(buff, data);
        _data.append(buff, 1);
    } else if constexpr (std::is_same_v<std::make_signed_t<T>, int16_t>) {
        char buff[2];
        int2store(buff, data);
        _data.append(buff, 2);
    } else if constexpr (std::is_same_v<std::make_signed_t<T>, int32_t>) {
        char buff[4];
        int4store(buff, data);
        _data.append(buff, 4);
    } else if constexpr (std::is_same_v<std::make_signed_t<T>, int64_t>) {
        char buff[8];
        int8store(buff, data);
        _data.append(buff, 8);
    } else if constexpr (std::is_same_v<std::make_signed_t<T>, __int128>) {
        std::string value = LargeIntValue::to_string(data);
        _push_string_normal(value.data(), value.size());
    } else {
        CHECK(false) << "unhandled data type";
    }
}

template <typename T>
void MysqlRowBuffer::push_number(T data, bool is_binary_protocol) {
    static_assert(std::is_arithmetic_v<T> || std::is_same_v<T, __int128>);

    if (is_binary_protocol) {
        return push_number_binary_format(data);
    }

    int length = 0;
    char* end = nullptr;
    char* pos = nullptr;
    const int length_prefix_bytes = _array_level == 0 ? 1 : 0;
    if constexpr (std::is_same_v<T, float>) {
        // 1 for length, 1 for sign, other for digits.
        pos = _resize_extra(2 + MAX_FLOAT_STR_LENGTH);
        length = f2s_buffered_n(data, pos + length_prefix_bytes);
    } else if constexpr (std::is_same_v<T, double>) {
        // 1 for string trail, 1 for length, 1 for sign, other for digits
        pos = _resize_extra(2 + MAX_DOUBLE_STR_LENGTH);
        length = d2s_buffered_n(data, pos + length_prefix_bytes);
    } else if constexpr (std::is_same_v<std::make_signed_t<T>, int8_t>) {
        pos = _resize_extra(2 + MAX_TINYINT_WIDTH);
        end = fmt::format_to(pos + length_prefix_bytes, FMT_COMPILE("{}"), data);
        length = end - pos - length_prefix_bytes;
    } else if constexpr (std::is_same_v<std::make_signed_t<T>, int16_t>) {
        pos = _resize_extra(2 + MAX_SMALLINT_WIDTH);
        end = fmt::format_to(pos + length_prefix_bytes, FMT_COMPILE("{}"), data);
        length = end - pos - length_prefix_bytes;
    } else if constexpr (std::is_same_v<std::make_signed_t<T>, int32_t>) {
        pos = _resize_extra(2 + MAX_INT_WIDTH);
        end = fmt::format_to(pos + length_prefix_bytes, FMT_COMPILE("{}"), data);
        length = end - pos - length_prefix_bytes;
    } else if constexpr (std::is_same_v<std::make_signed_t<T>, int64_t>) {
        pos = _resize_extra(2 + MAX_BIGINT_WIDTH);
        end = fmt::format_to(pos + length_prefix_bytes, FMT_COMPILE("{}"), data);
        length = end - pos - length_prefix_bytes;
    } else if constexpr (std::is_same_v<std::make_signed_t<T>, __int128>) {
        pos = _resize_extra(2 + 40);
        end = fmt::format_to(pos + length_prefix_bytes, FMT_COMPILE("{}"), data);
        length = end - pos - length_prefix_bytes;
    } else {
        CHECK(false) << "unhandled data type";
    }
    if (length_prefix_bytes > 0) {
        int1store(pos, length);
    }
    pos += length + length_prefix_bytes;
    DCHECK(pos >= _data.data() && pos <= _data.data() + _data.size());
    _data.resize(pos - _data.data());
}

void MysqlRowBuffer::push_string(const char* str, size_t length, char escape_char) {
    if (_array_level == 0) {
        _push_string_normal(str, length);
    } else {
        // Surround the string with two double-quotas.
        const size_t escaped_len = 2 + _length_after_escape(str, length, escape_char);
        char* pos = _resize_extra(escaped_len);
        *pos++ = escape_char;
        if (escaped_len == length + 2) {
            // No '\' or '"' exists in |str|, copy directly.
            strings::memcpy_inlined(pos, str, length);
            pos += length;
        } else {
            // Escape '\' and '"'.
            pos = _escape(pos, str, length, escape_char);
        }
        *pos++ = escape_char;
        DCHECK_EQ(_data.data() + _data.size(), pos);
        _data.resize(pos - _data.data());
    }
}

void MysqlRowBuffer::push_decimal(const Slice& s) {
    if (_array_level == 0) {
        _push_string_normal(s.data, s.size);
    } else {
        char* pos = _resize_extra(s.size);
        strings::memcpy_inlined(pos, s.data, s.size);
        pos += s.size;
        DCHECK_EQ(_data.data() + _data.size(), pos);
        _data.resize(pos - _data.data());
    }
}

void MysqlRowBuffer::push_date(const DateValue& data, bool is_binary_protocol) {
    if (is_binary_protocol) {
        int y, m, d;
        data.to_date(&y, &m, &d);
        char buff[5];
        // first pos store the length
        buff[0] = 4;
        buff[1] = (uint8_t)y;
        buff[2] = (uint8_t)(y >> 8);
        buff[3] = m;
        buff[4] = d;
        _data.append(buff, 5);
    } else {
        std::string s = data.to_string();
        push_string(s.data(), s.size());
    }
}

void MysqlRowBuffer::push_timestamp(const TimestampValue& data, bool is_binary_protocol) {
    if (is_binary_protocol) {
        int y, m, d, h, min, s, u;
        data.to_timestamp(&y, &m, &d, &h, &min, &s, &u);
        char buff[8];
        // first pos store the length
        buff[0] = u == 0 ? 7 : 11;
        buff[1] = (uint8_t)y;
        buff[2] = (uint8_t)(y >> 8);
        buff[3] = m;
        buff[4] = d;
        buff[5] = h;
        buff[6] = min;
        buff[7] = s;
        _data.append(buff, 8);
        if (u > 0) {
            char micro[4];
            micro[0] = (uint8_t)u;
            micro[1] = (uint8_t)(u >> 8);
            micro[2] = (uint8_t)(u >> 16);
            micro[3] = (uint8_t)(u >> 24);
            _data.append(micro, 4);
        }
    } else {
        std::string s = data.to_string();
        push_string(s.data(), s.size());
    }
}

void MysqlRowBuffer::_enter_scope(char c) {
    if (++_array_level == 1) {
        // Leave one space for storing the string length.
        _data.push_back(0x00);
        _array_offset = _data.size();
    }
    _data.push_back(c);
}

void MysqlRowBuffer::_leave_scope(char c) {
    DCHECK_GT(_array_level, 0);
    _data.push_back(c);
    if (--_array_level == 0) {
        uint64_t curr_scope_len = _data.size() - _array_offset;
        if (curr_scope_len < 251) {
            int1store(&_data[_array_offset - 1], curr_scope_len);
        } else if (curr_scope_len < 65536ULL) {
            _data.resize(_data.size() + 2);
            memmove(_data.data() + _array_offset + 2, _data.data() + _array_offset, curr_scope_len);
            int1store(&_data[_array_offset - 1], 252);
            int2store(&_data[_array_offset], curr_scope_len);
        } else if (curr_scope_len < 16777216ULL) {
            _data.resize(_data.size() + 3);
            memmove(_data.data() + _array_offset + 3, _data.data() + _array_offset, curr_scope_len);
            int1store(&_data[_array_offset - 1], 253);
            int3store(&_data[_array_offset], curr_scope_len);
        } else {
            _data.resize(_data.size() + 8);
            memmove(_data.data() + _array_offset + 8, _data.data() + _array_offset, curr_scope_len);
            int1store(&_data[_array_offset - 1], 254);
            int8store(&_data[_array_offset], curr_scope_len);
        }
    }
}

void MysqlRowBuffer::separator(char c) {
    DCHECK_GT(_array_level, 0);
    _data.push_back(c);
}

size_t MysqlRowBuffer::_length_after_escape(const char* str, size_t length, char escape_char) {
    size_t new_len = length;
    for (size_t i = 0; i < length; i++) {
        new_len += ((str[i] == escape_char) | (str[i] == '\\'));
        //                         ^^ use '|' or instead of '||' intentionally.
    }
    return new_len;
}

char* MysqlRowBuffer::_escape(char* dst, const char* src, size_t length, char escape_char) {
    for (size_t i = 0; i < length; i++) {
        char c = src[i];
        if (c == escape_char) {
            *dst++ = '\\';
            *dst++ = escape_char;
        } else if (c == '\\') {
            *dst++ = '\\';
            *dst++ = '\\';
        } else {
            *dst++ = c;
        }
    }
    return dst;
}

void MysqlRowBuffer::_push_string_normal(const char* str, size_t length) {
    char* pos = _resize_extra(9 + length);
    pos = reinterpret_cast<char*>(pack_vlen(reinterpret_cast<uint8_t*>(pos), length));
    strings::memcpy_inlined(pos, str, length);
    pos += length;
    DCHECK(pos >= _data.data() && pos <= _data.data() + _data.size());
    _data.resize(pos - _data.data());
}

template void MysqlRowBuffer::push_number<int8_t>(int8_t, bool);
template void MysqlRowBuffer::push_number<int16_t>(int16_t, bool);
template void MysqlRowBuffer::push_number<int32_t>(int32_t, bool);
template void MysqlRowBuffer::push_number<int64_t>(int64_t, bool);
template void MysqlRowBuffer::push_number<uint8_t>(uint8_t, bool);
template void MysqlRowBuffer::push_number<uint16_t>(uint16_t, bool);
template void MysqlRowBuffer::push_number<uint32_t>(uint32_t, bool);
template void MysqlRowBuffer::push_number<uint64_t>(uint64_t, bool);
template void MysqlRowBuffer::push_number<__int128>(__int128, bool);
template void MysqlRowBuffer::push_number<float>(float, bool);
template void MysqlRowBuffer::push_number<double>(double, bool);

void MysqlRowBuffer::start_binary_row(uint32_t num_cols) {
    DCHECK(_is_binary_format) << "start_binary_row() only for is_binary_format=true";
    int bit_fields = (num_cols + 9) / 8;
    char* pos = _resize_extra(bit_fields + 1);
    memset(pos, 0, 1 + bit_fields);
    _field_pos = 0;
}

} // namespace starrocks

/* vim: set ts=4 sw=4 sts=4 tw=100 */
