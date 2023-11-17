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

#include "io/input_stream.h"

namespace starrocks::io {

class SeekableInputStream : public InputStream {
public:
    ~SeekableInputStream() override = default;

    // Repositions the offset of the InputStream to the argument |position|.
    // If |position| less than zero, returns an error.
    // If |position| greater than the size of the InputStream, the subsequent
    // `read()` will return 0.
    virtual Status seek(int64_t position) = 0;

    // Returns the current offset location as measured in bytes from
    // the beginning of the InputStream
    virtual StatusOr<int64_t> position() = 0;

    // Repositions the offset of the InputStream to |offset| then read |count| bytes.
    //
    // Default implementation:
    // ```
    //    RETURN_IF_ERROR(seek(offset));
    //    return read(out, count);
    // ```
    //
    // Return the number of bytes read, or error.
    virtual StatusOr<int64_t> read_at(int64_t offset, void* out, int64_t count);

    // Read exactly the number of |count| bytes data from given position |offset|.
    // This method does not return the number of bytes read because either
    // (1) the entire |count| bytes is read
    // (2) the end of the stream is reached
    // If the eof is reached, an IO error is returned, leave the content of
    // |data| buffer unspecified.
    //
    // Default implementation:
    // ```
    //    RETURN_IF_ERROR(seek(offset));
    //    return read_fully(out, count);
    // ```
    virtual Status read_at_fully(int64_t offset, void* out, int64_t count);

    // Return the total file size in bytes, or error.
    virtual StatusOr<int64_t> get_size() = 0;

    // Default implementation:
    // ```
    //    ASSIGN_OR_RETURN(auto pos, position());
    //    return seek(pos + count);
    // ```
    Status skip(int64_t count) override;

    virtual void set_size(int64_t);

    // Reads all the data in this stream and returns it as std::string.
    //
    // Some implementations may override this method to get all
    // the data without calling `get_size()`.
    // For example, S3InputStream can read the contents of an entire
    // object directly with a single GET OBJECT call, without the need
    // to first send a HEAD OBJECT request to get the object size.
    virtual StatusOr<std::string> read_all();
};

class SeekableInputStreamWrapper : public SeekableInputStream {
public:
    explicit SeekableInputStreamWrapper(std::unique_ptr<SeekableInputStream> stream)
            : _impl(stream.release()), _ownership(kTakesOwnership) {}
    explicit SeekableInputStreamWrapper(SeekableInputStream* stream, Ownership ownership)
            : _impl(stream), _ownership(ownership) {}

    ~SeekableInputStreamWrapper() override {
        if (_ownership == kTakesOwnership) delete _impl;
    }

    // Disallow copy and assignment
    SeekableInputStreamWrapper(const SeekableInputStreamWrapper&) = delete;
    void operator=(const SeekableInputStreamWrapper&) = delete;
    // Disallow move
    SeekableInputStreamWrapper(SeekableInputStreamWrapper&&) = delete;
    void operator=(SeekableInputStreamWrapper&&) = delete;

    StatusOr<int64_t> read(void* data, int64_t count) override { return _impl->read(data, count); }

    Status read_fully(void* data, int64_t count) override { return _impl->read_fully(data, count); }

    Status skip(int64_t count) override { return _impl->skip(count); }

    StatusOr<std::string_view> peek(int64_t nbytes) override { return _impl->peek(nbytes); }

    StatusOr<std::unique_ptr<NumericStatistics>> get_numeric_statistics() override {
        return _impl->get_numeric_statistics();
    }

    StatusOr<int64_t> position() override { return _impl->position(); }

    StatusOr<int64_t> read_at(int64_t offset, void* out, int64_t count) override {
        return _impl->read_at(offset, out, count);
    }

    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        return _impl->read_at_fully(offset, out, count);
    }

    StatusOr<int64_t> get_size() override { return _impl->get_size(); }

    Status seek(int64_t offset) override { return _impl->seek(offset); }

    void set_size(int64_t value) override { return _impl->set_size(value); }

    StatusOr<std::string> read_all() override { return _impl->read_all(); }

private:
    SeekableInputStream* _impl;
    Ownership _ownership;
};

} // namespace starrocks::io
