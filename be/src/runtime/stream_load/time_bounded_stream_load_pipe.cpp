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

#include "runtime/stream_load/time_bounded_stream_load_pipe.h"

#include "runtime/batch_write/batch_write_util.h"

namespace starrocks {

StatusOr<ByteBufferPtr> TimeBoundedStreamLoadPipe::read() {
    RETURN_IF_ERROR(_finish_pipe_if_needed());
    return StreamLoadPipe::read();
}

Status TimeBoundedStreamLoadPipe::read(uint8_t* data, size_t* data_size, bool* eof) {
    RETURN_IF_ERROR(_finish_pipe_if_needed());
    return StreamLoadPipe::read(data, data_size, eof);
}

Status TimeBoundedStreamLoadPipe::_finish_pipe_if_needed() {
    auto current_ts = _get_current_ns();
    if (_start_time_ns + _active_window_ns <= current_ts) {
        auto st = StreamLoadPipe::finish();
        TRACE_BATCH_WRITE << "finish pipe: " << _name << ", expect active: " << (_active_window_ns / 1000000)
                          << " ms, actual active: " << (current_ts - _start_time_ns) / 1000000
                          << " ms, num appends: " << num_append_buffers() << ", bytes: " << append_buffer_bytes()
                          << ", status: " << st;
        return st;
    }
    return Status::OK();
}

} // namespace starrocks
