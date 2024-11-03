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
    auto current_ts = MonotonicNanos();
    if (_start_time_ns + _active_time_ns <= current_ts) {
        return StreamLoadPipe::finish();
    }
    return Status::OK();
}

} // namespace starrocks
