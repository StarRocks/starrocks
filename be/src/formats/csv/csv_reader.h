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

    // 返回已经读取的字节数
    size_t have_read() const { return _position - _begin; }

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

    // 已经读取过的
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

enum ParseState {
    START = 0,
    ORDINARY = 1,
    DELIMITER = 2,
    NEWLINE = 3,
    ESCAPE = 4,
    ENCLOSE = 5,
    // 这个状态表示读到了两个连续的ENCLOSE符号，此时并不能确定第一个enclose符号是转义还是这个字段是空字段，
    // 如果，接下来读到的是delimiter或者newline，那么该字段是空字段，否则是转义符号。
    DOUBLE_ENCLOSE = 6
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

    Status next_record(Fields* fields);

    void set_limit(size_t limit) { _limit = limit; }

    void split_record(const Record& record, Fields* fields) const;

    bool isDelimiter();

    bool isNewline();

protected:
    std::string _row_delimiter;
    std::string _column_separator;
    size_t _row_delimiter_length;
    size_t _column_separator_length;
    // TODO(yangzaorang): 需要加一个bool类型方便处理吗
    char _escape;
    char _enclose;
    bool _trim_space;
    raw::RawVector<char> _storage;
    // _buff其实就是一个连续的内存，作为存放读到的数据的容器
    CSVBuffer _buff;
    // TODO(yangzaorang):
    // 引入一个枚举类型用于表示状态机的状态
    // 如何读数据

    virtual Status _fill_buffer() { return Status::InternalError("unsupported csv reader!"); }

private:
    Status _expand_buffer();

    size_t _parsed_bytes = 0;
    // 这一个range的总数据是多少
    size_t _limit = 0;
    size_t _offset = 0;


};

} // namespace starrocks::vectorized
