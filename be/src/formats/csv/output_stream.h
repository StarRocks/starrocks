// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "runtime/date_value.hpp"
#include "runtime/decimalv2_value.h"
#include "runtime/timestamp_value.h"

namespace starrocks::vectorized::csv {

class OutputStream {
public:
    constexpr static const size_t kMinBuffSize = 128;

    explicit OutputStream(size_t capacity)
            : _buff(new char[std::max(kMinBuffSize, capacity)]),
              _pos(_buff),
              _end(_buff + std::max(kMinBuffSize, capacity)) {}

    ~OutputStream() { delete[] _buff; }

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
        RETURN_IF_ERROR(_reserve(std::numeric_limits<float>::digits10 + 2));
        _pos += f2s_buffered_n(f, _pos);
        return Status::OK();
    }

    Status write(double d) {
        RETURN_IF_ERROR(_reserve(std::numeric_limits<double>::digits10 + 2));
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

protected:
    virtual Status _sync(const char* data, size_t size) = 0;

private:
    inline size_t _free_space() const { return _end - _pos; }

    inline Status _reserve(size_t n) {
        if (_free_space() < n) {
            RETURN_IF_ERROR(_flush());
            if (UNLIKELY(_free_space() < n)) {
                return Status::MemoryLimitExceeded("Reserved size is greater than capacity");
            }
        }
        return Status::OK();
    }

    inline Status _flush() {
        Status st = _sync(_buff, _pos - _buff);
        _pos = _buff; // Don't care about the status.
        return st;
    }

    char* _buff;
    char* _pos;
    char* _end;
};

} // namespace starrocks::vectorized::csv
