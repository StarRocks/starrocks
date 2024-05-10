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

#include <fmt/format.h>

#include <boost/thread/future.hpp>
#include <future>

#include "column/chunk.h"
#include "common/status.h"
#include "formats/file_writer.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks::connector {

class IOStatusPoller;
class SinkOperatorMemoryManager;

using Writer = formats::FileWriter;
using Stream = io::AsyncFlushOutputStream;
using WriterAndStream = std::pair<std::unique_ptr<Writer>, Stream*>;
using CommitFunc = std::function<void(const formats::FileWriter::CommitResult& result)>;

class ConnectorChunkSink {
public:
    ConnectorChunkSink() = default;

    virtual ~ConnectorChunkSink() = default;

    virtual Status init() = 0;

    virtual Status add(ChunkPtr chunk) = 0;

    virtual Status finish() = 0;

    void set_io_poller(IOStatusPoller* poller) {
        _io_poller = poller;
    }

    void set_operator_mem_mgr(SinkOperatorMemoryManager* op_mem_mgr) {
        _op_mem_mgr = op_mem_mgr;
    }

protected:
    IOStatusPoller* _io_poller = nullptr;
    SinkOperatorMemoryManager* _op_mem_mgr = nullptr;
};

struct ConnectorChunkSinkContext {
public:
    virtual ~ConnectorChunkSinkContext() = default;
};

class ConnectorChunkSinkProvider {
public:
    virtual ~ConnectorChunkSinkProvider() = default;

    virtual StatusOr<std::unique_ptr<ConnectorChunkSink>> create_chunk_sink(
            std::shared_ptr<ConnectorChunkSinkContext> context, int32_t driver_id) = 0;
};

} // namespace starrocks::connector
