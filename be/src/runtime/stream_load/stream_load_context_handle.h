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

#include "runtime/fragment_attachment.h"

namespace starrocks {

class BatchWriteMgr;
class StreamContextMgr;
class StreamLoadContext;

class StreamLoadContextHandle final : public FragmentAttachment {
public:
    StreamLoadContextHandle(StreamLoadContext* context, BatchWriteMgr* batch_write_mgr);
    StreamLoadContextHandle(StreamLoadContext* context, StreamContextMgr* stream_context_mgr);
    ~StreamLoadContextHandle() override;

    StreamLoadContextHandle(const StreamLoadContextHandle&) = delete;
    StreamLoadContextHandle& operator=(const StreamLoadContextHandle&) = delete;
    StreamLoadContextHandle(StreamLoadContextHandle&&) = delete;
    StreamLoadContextHandle& operator=(StreamLoadContextHandle&&) = delete;

    void cancel(const Status& status) override;
    void close(const Status& status) override;

private:
    StreamLoadContext* _context = nullptr;
    BatchWriteMgr* _batch_write_mgr = nullptr;
    StreamContextMgr* _stream_context_mgr = nullptr;
    bool _closed = false;
};

} // namespace starrocks
