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

#include <map>

#include "column/chunk.h"
#include "common/status.h"
#include "connector/partition_chunk_writer.h"
#include "connector/utils.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"

namespace starrocks::connector {

class AsyncFlushStreamPoller;
class SinkOperatorMemoryManager;

using PartitionKey = std::pair<std::string, std::vector<int8_t>>;

class ConnectorChunkSink {
public:
    ConnectorChunkSink(std::vector<std::string> partition_columns,
                       std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_column_evaluators,
                       std::unique_ptr<PartitionChunkWriterFactory> partition_chunk_writer_factory, RuntimeState* state,
                       bool support_null_partition);

    void set_io_poller(AsyncFlushStreamPoller* poller) { _io_poller = poller; }

    void set_operator_mem_mgr(SinkOperatorMemoryManager* op_mem_mgr) { _op_mem_mgr = op_mem_mgr; }

    virtual ~ConnectorChunkSink() = default;

    Status init();

    virtual Status add(const ChunkPtr& chunk);

    Status finish();

    void rollback();

    bool is_finished();

    virtual void callback_on_commit(const CommitResult& result) = 0;

    Status write_partition_chunk(const std::string& partition, const vector<int8_t>& partition_field_null_list,
                                 const ChunkPtr& chunk);

    Status status();

    void set_status(const Status& status);

protected:
    void push_rollback_action(const std::function<void()>& action);

    AsyncFlushStreamPoller* _io_poller = nullptr;
    SinkOperatorMemoryManager* _op_mem_mgr = nullptr;

    std::vector<std::string> _partition_column_names;
    std::vector<std::unique_ptr<ColumnEvaluator>> _partition_column_evaluators;
    std::unique_ptr<PartitionChunkWriterFactory> _partition_chunk_writer_factory;
    RuntimeState* _state = nullptr;
    bool _support_null_partition{false};
    std::vector<std::function<void()>> _rollback_actions;

    std::map<PartitionKey, PartitionChunkWriterPtr> _partition_chunk_writers;
    inline static std::string DEFAULT_PARTITION = "__DEFAULT_PARTITION__";

    std::shared_mutex _mutex;
    Status _status;
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
