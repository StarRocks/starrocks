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

#include <queue>
#include <unordered_set>

#include "formats/csv/converter.h"

namespace starrocks::vectorized {
class CSVBuffer {
public:
    // Does NOT take the ownership of |buff|.
    CSVBuffer(char* buff, size_t cap) : _begin(buff), _position_offset(0), _limit_offset(0), _end(buff + cap) {}

    void append(char c) {
        *(_begin + _limit_offset) = c;
        _limit_offset++;
    }

    // Returns the number of bytes between the current position and the limit.
    size_t available() const { return _limit_offset - _position_offset; }

    // Returns the number of elements between the the limit and the end.
    size_t free_space() const { return _end - _begin - _limit_offset; }

    // Returns this buffer's capacity.
    size_t capacity() const { return _end - _begin; }

    size_t have_read() const { return _position_offset; }

    char* position() { return _begin + _position_offset; }

    // Returns this buffer's read position offset.
    size_t position_offset() { return _position_offset; }

    void set_position_offset(size_t position_offset) { _position_offset = position_offset; }

    // Returns this buffer's write position.
    char* limit() { return _begin + _limit_offset; }

    // Returns this buffer's write position offset.
    size_t limit_offset() { return _limit_offset; }

    void set_limit_offset(size_t limit_offset) { _limit_offset = limit_offset; }

    void add_limit(size_t n) { _limit_offset += n; }

    // Finds the first character equal to the given character |c|. Search begins at |pos|.
    // Return: address of the first character of the found character or NULL if no such
    // character is found.
    char* find(char c, size_t pos = 0) { return (char*)memchr(position() + pos, c, available() - pos); }

    char* find(const std::string& str, size_t pos = 0) {
        return (char*)memmem(position() + pos, available() - pos, str.c_str(), str.size());
    }

    void skip(size_t n) { _position_offset += n; }

    char get_char(size_t p) { return _begin[p]; }

    char* base_ptr() { return _begin; }

    // Compacts this buffer.
    // The bytes between the buffer's current position and its limit, if any,
    // are copied to the beginning of the buffer.
    void compact() {
        memmove(_begin, _begin + _position_offset, available());
        _limit_offset -= _position_offset;
        _position_offset = 0;
    }

private:
    char* _begin;
    size_t _position_offset;
    size_t _limit_offset;
    char* _end;
};

enum ParseState { START = 0, ORDINARY = 1, DELIMITER = 2, NEWLINE = 3, ESCAPE = 4, ENCLOSE = 5, ENCLOSE_ESCAPE = 6 };

struct CSVField {
    size_t start_pos;
    size_t length;
    bool isEscapeField;
    CSVField(size_t pos, size_t len, bool isEscape) : start_pos(pos), length(len), isEscapeField(isEscape) {}
};

struct CSVLine {
    std::vector<CSVField> fields;
    size_t parsed_start;
    size_t parsed_end;

    std::string debug_string(const char* buffBasePtr, const char* escapeDataPtr) {
        std::stringstream ss;
        for (int i = 0; i < fields.size(); i++) {
            auto& field = fields[i];
            const char* basePtr = field.isEscapeField ? escapeDataPtr : buffBasePtr;
            if (i != fields.size() - 1) {
                ss << std::string(basePtr + field.start_pos, field.length) << "|";
            } else {
                ss << std::string(basePtr + field.start_pos, field.length) << std::endl;
            }
        }
        return ss.str();
    }
};

class CSVReader {
#ifndef BE_TEST
    constexpr static size_t kMinBufferSize = 8 * 1024 * 1024L;
    constexpr static size_t kMaxBufferSize = 512 * 1024 * 1024L;
#else
    constexpr static size_t kMinBufferSize = 128 * 1024L;
    constexpr static size_t kMaxBufferSize = 512 * 1024L;
#endif

public:
    using Record = Slice;
    using Field = Slice;
    using Fields = std::vector<Field>;

    CSVReader(const std::string& row_delimiter, const std::string& column_separator, bool trim_space = false,
              char escape = 0, char enclose = 0, const size_t bufferSize = kMinBufferSize)
            : _row_delimiter(row_delimiter),
              _column_separator(column_separator),
              _trim_space(trim_space),
              _escape(escape),
              _enclose(enclose),
              _storage(bufferSize),
              _buff(_storage.data(), _storage.size()) {
        _row_delimiter_length = row_delimiter.size();
        _column_separator_length = column_separator.size();
    }

    virtual ~CSVReader() = default;

    Status next_record(Record* record);

    Status next_record(CSVLine& line);

    Status more_lines();

    void set_limit(size_t limit) { _limit = limit; }

    void split_record(const Record& record, Fields* fields) const;

    bool isRowDelimiter(bool expandBuffer);

    bool isColumnSeparator(bool expandBuffer);

    Status readMore(bool expandBuffer);

    Status buffInit();

    char* buffBasePtr();

    char* escapeDataPtr();

    // For benchmark, we need to separate io from parsing.
    Status init_buff() { return _fill_buffer(); }

protected:
    std::string _row_delimiter;
    std::string _column_separator;
    size_t _row_delimiter_length;
    size_t _column_separator_length;
    bool _trim_space;
    char _escape;
    char _enclose;
    raw::RawVector<char> _storage;
    CSVBuffer _buff;
    raw::RawVector<char> _escape_data;
    virtual Status _fill_buffer() { return Status::InternalError("unsupported csv reader!"); }
    std::queue<CSVLine> _csv_buff;
    std::unordered_set<size_t> _escape_pos;
    std::vector<CSVField> _fields;

private:
    Status _expand_buffer();
    Status _expand_buffer_loosely();

    size_t _parsed_bytes = 0;
    size_t _limit = 0;
    size_t _offset = 0;
};

} // namespace starrocks::vectorized
