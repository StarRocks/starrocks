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

#include "runtime/stream_load/stream_load_pipe.h"

namespace starrocks {

class TimeBoundedStreamLoadPipe : public StreamLoadPipe {
public:
    TimeBoundedStreamLoadPipe(int active_time_ms,
                              int32_t non_blocking_wait_ms = DEFAULT_STREAM_LOAD_PIPE_NON_BLOCKING_WAIT_MS,
                              size_t max_buffered_bytes = DEFAULT_STREAM_LOAD_PIPE_BUFFERED_BYTES)
            : StreamLoadPipe(true, non_blocking_wait_ms, max_buffered_bytes, DEFAULT_STREAM_LOAD_PIPE_CHUNK_SIZE) {
        _start_time_ns = MonotonicNanos();
        _active_time_ns = active_time_ms * 1000000;
    }

    Status append(const char* data, size_t size) override {
        return Status::NotSupported("TimeBoundedStreamLoadPipe does not support input with char array");
    }

    StatusOr<ByteBufferPtr> read() override;

    Status read(uint8_t* data, size_t* data_size, bool* eof) override;

private:
    Status _finish_pipe_if_needed();

    int _start_time_ns;
    int _active_time_ns;
};

} // namespace starrocks
