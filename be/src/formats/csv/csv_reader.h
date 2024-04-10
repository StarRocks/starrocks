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

namespace starrocks {
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

enum ParseState {
    START = 0,
    ORDINARY = 1,
    COLUMN_DELIMITER = 2,
    NEWROW = 3,
    ESCAPE = 4,
    ENCLOSE = 5,
    ENCLOSE_ESCAPE = 6
};

struct CSVColumn {
    size_t start_pos;
    size_t length;
    bool is_escaped_column;
    CSVColumn(size_t pos, size_t len, bool isEscape) : start_pos(pos), length(len), is_escaped_column(isEscape) {}
};

struct CSVRow {
    std::vector<CSVColumn> columns;
    size_t parsed_start;
    size_t parsed_end;

    std::string debug_string(const char* buffBasePtr, const char* escapeDataPtr) {
        std::stringstream ss;
        for (int i = 0; i < columns.size(); i++) {
            auto& column = columns[i];
            const char* basePtr = column.is_escaped_column ? escapeDataPtr : buffBasePtr;
            if (i != columns.size() - 1) {
                ss << std::string(basePtr + column.start_pos, column.length) << "|";
            } else {
                ss << std::string(basePtr + column.start_pos, column.length) << std::endl;
            }
        }
        return ss.str();
    }
};

struct CSVParseOptions {
    std::string row_delimiter;
    std::string column_delimiter;
    int64_t skip_header;
    bool trim_space;
    char escape;
    char enclose;
    CSVParseOptions(const std::string row_delimiter_, const std::string column_delimiter_, int64_t skip_header_ = 0,
                    bool trim_space_ = false, char escape_ = 0, char enclose_ = 0) {
        row_delimiter = row_delimiter_;
        column_delimiter = column_delimiter_;
        skip_header = skip_header_;
        trim_space = trim_space_;
        escape = escape_;
        enclose = enclose_;
    }
    CSVParseOptions() {
        row_delimiter = '\n';
        column_delimiter = ',';
        skip_header = false;
        trim_space = false;
        escape = 0;
        enclose = 0;
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

    CSVReader(const CSVParseOptions& parse_options, const size_t bufferSize = kMinBufferSize)
            : _parse_options(parse_options), _storage(bufferSize), _buff(_storage.data(), _storage.size()) {
        _row_delimiter_length = parse_options.row_delimiter.size();
        _column_delimiter_length = parse_options.column_delimiter.size();
    }

    virtual ~CSVReader() = default;

    Status next_record(Record* record);

    Status next_record(CSVRow& row);

    Status more_rows();

    void set_limit(size_t limit) { _limit = limit; }

    void split_record(const Record& record, Fields* fields) const;

    bool is_row_delimiter(bool expandBuffer);

    bool is_column_delimiter(bool expandBuffer);

    Status readMore(bool expandBuffer);

    Status buffInit();

    char* buffBasePtr();

    char* escapeDataPtr();

    // For benchmark, we need to separate io from parsing.
    Status init_buff() { return _fill_buffer(); }

    size_t buff_capacity() const;

protected:
    CSVParseOptions _parse_options;
    size_t _row_delimiter_length;
    size_t _column_delimiter_length;
    raw::RawVector<char> _storage;
    CSVBuffer _buff;
    raw::RawVector<char> _escape_data;
    virtual Status _fill_buffer() { return Status::InternalError("unsupported csv reader!"); }
    virtual char* _find_line_delimiter(CSVBuffer& buffer, size_t pos) = 0;
    std::queue<CSVRow> _csv_buff;
    std::unordered_set<size_t> _escape_pos;
    std::vector<CSVColumn> _columns;

private:
    Status _expand_buffer();
    Status _expand_buffer_loosely();

    size_t _parsed_bytes = 0;
    size_t _limit = 0;
};

} // namespace starrocks
