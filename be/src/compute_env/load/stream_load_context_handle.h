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

#include <functional>

#include "runtime/fragment_attachment.h"

namespace starrocks {

class StreamLoadContext;

class StreamLoadContextHandle final : public FragmentAttachment {
public:
    using CloseCallback = std::function<void(StreamLoadContext*)>;

    StreamLoadContextHandle(StreamLoadContext* context, CloseCallback close_cb);
    ~StreamLoadContextHandle() override;

    StreamLoadContextHandle(const StreamLoadContextHandle&) = delete;
    StreamLoadContextHandle& operator=(const StreamLoadContextHandle&) = delete;
    StreamLoadContextHandle(StreamLoadContextHandle&&) = delete;
    StreamLoadContextHandle& operator=(StreamLoadContextHandle&&) = delete;

    void cancel(const Status& status) override;
    void close(const Status& status) override;

private:
    StreamLoadContext* _context = nullptr;
    CloseCallback _close_cb;
    bool _closed = false;
};

} // namespace starrocks
