// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "common/logging.h"
#include "fs/fs.h"

namespace starrocks {

/**
 * Example usage:
 * ```
 * #include <rapidjson/document.h> // will include "rapidjson/rapidjson.h" 
 * #include "fs/fs_util.h"
 * // ...
 * ASSIGN_OR_RETURN(auto f = fs::new_sequential_file("1.json"));
 * char buffer[65536];
 * RapidJSONReadStreamAdapter adapter(f.get(), buffer, sizeof(buffer));
 * rapidjson::Document d;
 * d.ParseStream(adapter); 
 * ```
 **/
class RapidJSONReadStreamAdapter {
public:
    typedef char Ch; //!< Character type (byte).

    // Constructor
    //  |fp| file pointer opened for read.
    //  |buffer| user-supplied buffer.
    //  |buffer_size| size of buffer in bytes. Must >=4 bytes.
    RapidJSONReadStreamAdapter(SequentialFile* fp, char* buffer, size_t buffer_size)
            : RapidJSONReadStreamAdapter(fp->stream(), buffer, buffer_size) {}

    // Constructor
    //  |fp| file pointer opened for read.
    //  |buffer| user-supplied buffer.
    //  |buffer_size| size of buffer in bytes. Must >=4 bytes.
    RapidJSONReadStreamAdapter(RandomAccessFile* fp, char* buffer, size_t buffer_size)
            : RapidJSONReadStreamAdapter(fp->stream(), buffer, buffer_size) {}

    // Constructor
    //  |is| input stream opened for read.
    //  |buffer| user-supplied buffer.
    //  |buffer_size| size of buffer in bytes. Must >=4 bytes.
    RapidJSONReadStreamAdapter(std::shared_ptr<io::InputStream> is, char* buffer, size_t buffer_size)
            : _is(std::move(is)),
              _buffer(buffer),
              _buffer_size(buffer_size),
              _buffer_last(0),
              _current(_buffer),
              _read_count(0),
              _count(0),
              _eof(false) {
        DCHECK(_is != nullptr);
        DCHECK(buffer_size >= 4);
        Read();
    }

    DISALLOW_COPY_AND_MOVE(RapidJSONReadStreamAdapter);

    Ch Peek() const { return *_current; }
    Ch Take() {
        Ch c = *_current;
        Read();
        return c;
    }
    size_t Tell() const { return _count + static_cast<size_t>(_current - _buffer); }

    // Not implemented
    void Put(Ch) { DCHECK(false); }
    void Flush() { DCHECK(false); }
    Ch* PutBegin() {
        DCHECK(false);
        return 0;
    }
    size_t PutEnd(Ch*) {
        DCHECK(false);
        return 0;
    }

    // For encoding detection only.
    const Ch* Peek4() const { return (_current + 4 - !_eof <= _buffer_last) ? _current : 0; }

    Status status() const { return _status; }

private:
    void Read() {
        if (_current < _buffer_last) {
            ++_current;
        } else if (!_eof) {
            _count += _read_count;
            auto read_count_or = _is->read(_buffer, _buffer_size);
            _status.update(read_count_or.status());
            _read_count = read_count_or.ok() ? *read_count_or : 0;
            _buffer_last = _buffer + _read_count - 1;
            _current = _buffer;
            if (_read_count == 0) {
                _buffer[_read_count] = '\0';
                ++_buffer_last;
                _eof = true;
            }
        }
    }

    std::shared_ptr<io::InputStream> _is;
    Ch* _buffer;
    size_t _buffer_size;
    Ch* _buffer_last;
    Ch* _current;
    size_t _read_count;
    size_t _count; // Number of characters read
    Status _status;
    bool _eof;
};

class RapidJSONWriteStreamAdapter {
public:
    typedef char Ch; // Character type. Only support char.

    RapidJSONWriteStreamAdapter(WritableFile* wf, char* buffer, size_t bufferSize)
            : _wf(wf), _buffer(buffer), _buffer_end(buffer + bufferSize), _current(_buffer) {
        DCHECK(_wf != nullptr);
    }

    DISALLOW_COPY_AND_MOVE(RapidJSONWriteStreamAdapter);

    void Put(char c) {
        if (_current >= _buffer_end) Flush();

        *_current++ = c;
    }

    void PutN(char c, size_t n) {
        size_t avail = static_cast<size_t>(_buffer_end - _current);
        while (n > avail) {
            std::memset(_current, c, avail);
            _current += avail;
            Flush();
            n -= avail;
            avail = static_cast<size_t>(_buffer_end - _current);
        }

        if (n > 0) {
            std::memset(_current, c, n);
            _current += n;
        }
    }

    void Flush() {
        if (_current != _buffer) {
            _status.update(_wf->append({_buffer, static_cast<size_t>(_current - _buffer)}));
            _current = _buffer;
        }
    }

    // Not implemented
    char Peek() const {
        DCHECK(false);
        return 0;
    }
    char Take() {
        DCHECK(false);
        return 0;
    }
    size_t Tell() const {
        DCHECK(false);
        return 0;
    }
    char* PutBegin() {
        DCHECK(false);
        return 0;
    }
    size_t PutEnd(char*) {
        DCHECK(false);
        return 0;
    }

    Status status() const { return _status; }

private:
    WritableFile* _wf;
    char* _buffer;
    char* _buffer_end;
    char* _current;
    Status _status;
};

} // namespace starrocks

namespace rapidjson {
//! Implement specialized version of PutN() with memset() for better performance.
template <>
inline void PutN(starrocks::RapidJSONWriteStreamAdapter& stream, char c, size_t n) {
    stream.PutN(c, n);
}

} // namespace rapidjson
