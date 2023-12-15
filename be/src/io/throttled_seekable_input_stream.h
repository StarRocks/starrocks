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

#include <chrono>
#include <thread>

#include "io/seekable_input_stream.h"

namespace starrocks::io {

// Sleeping is done before reading the stream.
class ThrottledSeekableInputStream : public SeekableInputStreamWrapper {
public:
    // |stream| the underlying stream to read
    // |wait_per_read| the wait duration in milliseconds,
    explicit ThrottledSeekableInputStream(std::unique_ptr<SeekableInputStream> stream, int64_t wait_per_read)
            : SeekableInputStreamWrapper(std::move(stream)), _wait_per_read(wait_per_read) {
        CHECK_GE(_wait_per_read, 0);
    }

    ~ThrottledSeekableInputStream() = default;

    StatusOr<int64_t> read(void* data, int64_t count) override {
        std::this_thread::sleep_for(std::chrono::milliseconds(_wait_per_read));
        return SeekableInputStreamWrapper::read(data, count);
    }

    Status read_fully(void* data, int64_t count) override {
        std::this_thread::sleep_for(std::chrono::milliseconds(_wait_per_read));
        return SeekableInputStreamWrapper::read_fully(data, count);
    }

    StatusOr<int64_t> read_at(int64_t offset, void* out, int64_t count) override {
        std::this_thread::sleep_for(std::chrono::milliseconds(_wait_per_read));
        return SeekableInputStreamWrapper::read_at(offset, out, count);
    }

    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        std::this_thread::sleep_for(std::chrono::milliseconds(_wait_per_read));
        return SeekableInputStreamWrapper::read_at_fully(offset, out, count);
    }

private:
    int64_t _wait_per_read;
};

} // namespace starrocks::io
