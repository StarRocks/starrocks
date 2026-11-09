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

#include "compute_env/load/stream_load_pipe_attachment.h"

#include <utility>

#include "compute_env/load/stream_load_pipe.h"

namespace starrocks {

StreamLoadPipeAttachment::StreamLoadPipeAttachment(std::shared_ptr<StreamLoadPipe> pipe) : _pipe(std::move(pipe)) {}

void StreamLoadPipeAttachment::cancel(const Status& status) {
    // FragmentContext also calls cancel with OK on normal completion.
    if (!status.ok()) {
        _pipe->cancel(status);
    }
}

void StreamLoadPipeAttachment::close(const Status& status) {
    _pipe->cancel(status);
}

} // namespace starrocks
