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

#include <cmath>

#include "runtime/stream_load/stream_load_pipe.h"
#include "testutil/sync_point.h"
#include "util/time.h"

namespace starrocks {

static constexpr int32_t DEFAULT_STREAM_LOAD_PIPE_NON_BLOCKING_WAIT_US = 500;

class TimeBoundedStreamLoadPipe : public StreamLoadPipe {
public:
    TimeBoundedStreamLoadPipe(const std::string& name, int32_t active_window_ms,
                              int32_t non_blocking_wait_us = DEFAULT_STREAM_LOAD_PIPE_NON_BLOCKING_WAIT_US,
                              size_t max_buffered_bytes = DEFAULT_STREAM_LOAD_PIPE_BUFFERED_BYTES)
            : StreamLoadPipe(true, non_blocking_wait_us, max_buffered_bytes, DEFAULT_STREAM_LOAD_PIPE_CHUNK_SIZE) {
        _name = name;
        _active_window_ns = active_window_ms * (int64_t)1000000;
        _start_time_ns = _get_current_ns();
    }

    Status append(ByteBufferPtr&& buf) override { return StreamLoadPipe::append(std::move(buf)); }

    Status append(const char* data, size_t size) override {
        return Status::NotSupported("TimeBoundedStreamLoadPipe does not support input with char array");
    }

    StatusOr<ByteBufferPtr> read() override;

    Status read(uint8_t* data, size_t* data_size, bool* eof) override;

    int64_t left_active_ns() {
        int64_t left = _active_window_ns - (_get_current_ns() - _start_time_ns);
        return std::max((int64_t)0, left);
    }

private:
    int64_t _get_current_ns() {
        int64_t current_ts = MonotonicNanos();
        TEST_SYNC_POINT_CALLBACK("TimeBoundedStreamLoadPipe::get_current_ns", &current_ts);
        return current_ts;
    }

    Status _finish_pipe_if_needed();

    std::string _name;
    int64_t _start_time_ns;
    int64_t _active_window_ns;
};

} // namespace starrocks
