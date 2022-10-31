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

#include "formats/csv/converter.h"

namespace starrocks::vectorized {
class CSVBuffer {
public:
    // Does NOT take the ownership of |buff|.
    CSVBuffer(char* buff, size_t cap) : _begin(buff), _position(buff), _limit(buff), _end(buff + cap) {}

    void append(char c) { *_limit++ = c; }

    // Returns the number of bytes between the current position and the limit.
    size_t available() const { return _limit - _position; }

    // Returns the number of elements between the the limit and the end.
    size_t free_space() const { return _end - _limit; }

    // Returns this buffer's capacity.
    size_t capacity() const { return _end - _begin; }

    // Returns this buffer's read position.
    char* position() { return _position; }

    // Returns this buffer's write position.
    char* limit() { return _limit; }

    void add_limit(size_t n) { _limit += n; }

    // Finds the first character equal to the given character |c|. Search begins at |pos|.
    // Return: address of the first character of the found character or NULL if no such
    // character is found.
    char* find(char c, size_t pos = 0) { return (char*)memchr(position() + pos, c, available() - pos); }

    char* find(const std::string& str, size_t pos = 0) {
        return (char*)memmem(position() + pos, available() - pos, str.c_str(), str.size());
    }

    void skip(size_t n) { _position += n; }

    // Compacts this buffer.
    // The bytes between the buffer's current position and its limit, if any,
    // are copied to the beginning of the buffer.
    void compact() {
        size_t n = available();
        memmove(_begin, _position, available());
        _limit = _begin + n;
        _position = _begin;
    }

private:
    char* _begin;
    char* _position; // next read position
    char* _limit;    // next write position
    char* _end;
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

    CSVReader(const string& row_delimiter, const string& column_separator, bool trim_space)
            : _row_delimiter(row_delimiter),
              _column_separator(column_separator),
              _trim_space(trim_space),
              _storage(kMinBufferSize),
              _buff(_storage.data(), _storage.size()) {
        _row_delimiter_length = row_delimiter.size();
        _column_separator_length = column_separator.size();
    }

    virtual ~CSVReader() = default;

    Status next_record(Record* record);

    void set_limit(size_t limit) { _limit = limit; }

    void split_record(const Record& record, Fields* fields) const;

protected:
    std::string _row_delimiter;
    std::string _column_separator;
    size_t _row_delimiter_length;
    size_t _column_separator_length;
    bool _trim_space;
    raw::RawVector<char> _storage;
    CSVBuffer _buff;

    virtual Status _fill_buffer() { return Status::InternalError("unsupported csv reader!"); }

private:
    Status _expand_buffer();

    size_t _parsed_bytes = 0;
    size_t _limit = 0;
};

} // namespace starrocks::vectorized
