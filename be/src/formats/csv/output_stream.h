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

#include <ryu/ryu.h>

#include "runtime/decimalv2_value.h"
#include "types/date_value.hpp"
#include "types/timestamp_value.h"
#include "util/mysql_global.h"

namespace starrocks::csv {

class OutputStream {
public:
    constexpr static const size_t kMinBuffSize = 128;

    explicit OutputStream(size_t capacity)
            : _buff(new char[std::max(kMinBuffSize, capacity)]),
              _pos(_buff),
              _end(_buff + std::max(kMinBuffSize, capacity)) {}

    virtual ~OutputStream() { delete[] _buff; }

    OutputStream(const OutputStream&) = delete;
    void operator=(const OutputStream&) = delete;

    template <typename T, std::enable_if_t<std::is_integral_v<T>, void**> = nullptr>
    Status write(T number) {
        RETURN_IF_ERROR(_reserve(std::numeric_limits<T>::digits10 + 2));
        _pos = fmt::format_to(_pos, FMT_COMPILE("{}"), number);
        return Status::OK();
    }

    Status write(const __int128& v) {
        RETURN_IF_ERROR(_reserve(42));
        _pos = fmt::format_to(_pos, FMT_COMPILE("{}"), v);
        return Status::OK();
    }

    Status write(float f) {
        RETURN_IF_ERROR(_reserve(MAX_FLOAT_STR_LENGTH));
        _pos += f2s_buffered_n(f, _pos);
        return Status::OK();
    }

    Status write(double d) {
        RETURN_IF_ERROR(_reserve(MAX_DOUBLE_STR_LENGTH));
        _pos += d2s_buffered_n(d, _pos);
        return Status::OK();
    }

    Status write(const DecimalV2Value& d) {
        RETURN_IF_ERROR(_reserve(64));
        _pos += d.to_string(_pos);
        return Status::OK();
    }

    Status write(DateValue date) {
        RETURN_IF_ERROR(_reserve(10));
        int y, m, d;
        date.to_date(&y, &m, &d);
        date::to_string(y, m, d, _pos);
        _pos += 10;
        return Status::OK();
    }

    Status write(TimestampValue ts) {
        RETURN_IF_ERROR(_reserve(TimestampValue::max_string_length()));
        int len = ts.to_string(_pos, TimestampValue::max_string_length());
        DCHECK_GT(len, 0);
        _pos += len;
        return Status::OK();
    }

    Status write(char c) {
        RETURN_IF_ERROR(_reserve(1));
        *_pos++ = c;
        return Status::OK();
    }

    Status write(Slice s) {
        while (s.size > _free_space()) {
            size_t ncopy = _free_space();
            memcpy(_pos, s.data, ncopy);
            _pos += ncopy;
            RETURN_IF_ERROR(_flush());
            s.remove_prefix(ncopy);
        }
        memcpy(_pos, s.data, s.size);
        _pos += s.size;
        return Status::OK();
    }

    virtual Status finalize() { return _flush(); }

    virtual std::size_t size() { return 0; }

protected:
    virtual Status _sync(const char* data, size_t size) = 0;

private:
    size_t _free_space() const { return _end - _pos; }

    Status _reserve(size_t n) {
        if (_free_space() < n) {
            RETURN_IF_ERROR(_flush());
            if (UNLIKELY(_free_space() < n)) {
                return Status::MemoryLimitExceeded("Reserved size is greater than capacity");
            }
        }
        return Status::OK();
    }

    Status _flush() {
        Status st = _sync(_buff, _pos - _buff);
        _pos = _buff; // Don't care about the status.
        return st;
    }

    char* _buff;
    char* _pos;
    char* _end;
};

} // namespace starrocks::csv
