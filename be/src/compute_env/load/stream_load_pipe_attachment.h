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

#include <memory>

#include "runtime/fragment_attachment.h"

namespace starrocks {

class StreamLoadPipe;

// Only owns the existing input pipe; HTTP/routine load retains ownership of its context.
class StreamLoadPipeAttachment final : public FragmentAttachment {
public:
    explicit StreamLoadPipeAttachment(std::shared_ptr<StreamLoadPipe> pipe);

    void cancel(const Status& status) override;
    void close(const Status& status) override;

private:
    std::shared_ptr<StreamLoadPipe> _pipe;
};

} // namespace starrocks
