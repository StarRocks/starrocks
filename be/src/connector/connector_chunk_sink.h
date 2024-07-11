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

#include "column/chunk.h"
#include "common/status.h"
#include "connector/utils.h"
#include "formats/file_writer.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"

namespace starrocks::connector {

class AsyncFlushStreamPoller;
class SinkOperatorMemoryManager;

using Writer = formats::FileWriter;
using Stream = io::AsyncFlushOutputStream;
using WriterStreamPair = std::pair<std::unique_ptr<Writer>, Stream*>;
using CommitResult = formats::FileWriter::CommitResult;
using CommitFunc = std::function<void(const CommitResult& result)>;

class ConnectorChunkSink {
public:
    ConnectorChunkSink(std::vector<std::string> partition_columns,
                       std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_column_evaluators,
                       std::unique_ptr<LocationProvider> location_provider,
                       std::unique_ptr<formats::FileWriterFactory> file_writer_factory, int64_t max_file_size,
                       RuntimeState* state, bool support_null_partition);

    void set_io_poller(AsyncFlushStreamPoller* poller) { _io_poller = poller; }

    void set_operator_mem_mgr(SinkOperatorMemoryManager* op_mem_mgr) { _op_mem_mgr = op_mem_mgr; }

    virtual ~ConnectorChunkSink() = default;

    Status init();

    Status add(Chunk* chunk);

    Status finish();

    void rollback();

    virtual void callback_on_commit(const CommitResult& result) = 0;

protected:
    AsyncFlushStreamPoller* _io_poller = nullptr;
    SinkOperatorMemoryManager* _op_mem_mgr = nullptr;

    std::vector<std::string> _partition_column_names;
    std::vector<std::unique_ptr<ColumnEvaluator>> _partition_column_evaluators;
    std::unique_ptr<LocationProvider> _location_provider;
    std::unique_ptr<formats::FileWriterFactory> _file_writer_factory;
    int64_t _max_file_size = 1024L * 1024 * 1024;
    RuntimeState* _state = nullptr;
    bool _support_null_partition{false};
    std::vector<std::function<void()>> _rollback_actions;

    std::unordered_map<std::string, WriterStreamPair> _writer_stream_pairs;
    inline static std::string DEFAULT_PARTITION = "__DEFAULT_PARTITION__";
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
