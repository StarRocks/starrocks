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

#include "exec/pipeline/sink/sink_io_buffer.h"

namespace starrocks::pipeline {

void SinkIOBuffer::_process_chunk(bthread::TaskIterator<ChunkPtr>& iter) {
    DeferOp op([&]() {
        --_num_pending_chunks;
    });

    if (_is_finished) {
        return;
    }

    const auto& chunk = *iter;
    if (chunk == nullptr) {
        close(_state);
        return;
    }

    if (_is_cancelled && !_is_finished) {
        if (_num_pending_chunks <= 1) {
            close(_state);
        }
        return;
    }

    _add_chunk(chunk);
}

} // namespace starrocks::pipeline