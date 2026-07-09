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
#include <map>
#include <shared_mutex>
#include <string>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "connector/common/connector_sink_profile.h"
#include "connector/partition_chunk_writer.h"
#include "connector/utils.h"
#include "connector_primitive/connector_sink.h"
#include "runtime/runtime_fwd.h"

namespace starrocks::formats {
class AsyncFlushStreamPoller;
} // namespace starrocks::formats

namespace starrocks::connector {

class PartitionChunkWriterMemoryManager;
class SinkMemoryManager;
class SinkOperatorMemoryManager;

using PartitionKey = std::pair<std::string, std::vector<int8_t>>;

class PartitionedConnectorChunkSink : public ConnectorSink {
public:
    PartitionedConnectorChunkSink(std::vector<std::string> partition_columns,
                                  std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_column_evaluators,
                                  std::unique_ptr<PartitionChunkWriterFactory> partition_chunk_writer_factory,
                                  RuntimeState* state, bool support_null_partition);

    SinkOperatorMemoryManager* op_mem_mgr() const override { return _op_mem_mgr; }

    ~PartitionedConnectorChunkSink() override = default;

    Status init(formats::AsyncFlushStreamPoller* poller, RuntimeProfile* profile,
                SinkMemoryManager* sink_mem_mgr) override;

    Status add(const ChunkPtr& chunk) override;

    Status finish() override;

    void rollback() override;

    bool is_finished() override;

    virtual void callback_on_commit(const CommitResult& result) = 0;

    Status write_partition_chunk(const std::string& partition, const std::vector<int8_t>& partition_field_null_list,
                                 const ChunkPtr& chunk);

    Status status() override;

    void set_status(const Status& status);

    void register_memory_candidates(SinkOperatorMemoryManager* op_mem_mgr) override;

protected:
    void push_rollback_action(const std::function<void()>& action);
    void init_profile();

    formats::AsyncFlushStreamPoller* _io_poller = nullptr;
    SinkOperatorMemoryManager* _op_mem_mgr = nullptr;
    PartitionChunkWriterMemoryManager* _partition_writer_mem_mgr = nullptr;

    std::vector<std::string> _partition_column_names;
    std::vector<std::unique_ptr<ColumnEvaluator>> _partition_column_evaluators;
    std::unique_ptr<PartitionChunkWriterFactory> _partition_chunk_writer_factory;
    RuntimeState* _state = nullptr;
    bool _support_null_partition{false};
    std::vector<std::function<void()>> _rollback_actions;

    std::map<PartitionKey, PartitionChunkWriterPtr> _partition_chunk_writers;
    // passed to PartitionChunkWriterMemoryManager to check memory usage
    std::vector<PartitionChunkWriterPtr> _writers;
    inline static std::string DEFAULT_PARTITION = "__DEFAULT_PARTITION__";

    std::shared_mutex _mutex;
    Status _status;
    RuntimeProfile* _profile = nullptr;
    ConnectorSinkProfile* _sink_profile = nullptr;
};

} // namespace starrocks::connector
