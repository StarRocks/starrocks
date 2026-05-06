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

#include "common/status.h"
#include "connector/connector_chunk_sink.h"
#include "connector/iceberg_chunk_sink.h"
#include "connector/iceberg_delete_sink.h"
#include "connector/sink_memory_manager.h"

namespace starrocks::connector {

// Context for IcebergRowDeltaSink
// Composes delete-sink and data-sink contexts plus routing column indices.
struct IcebergRowDeltaSinkContext : public ConnectorChunkSinkContext {
    ~IcebergRowDeltaSinkContext() override = default;

    // Sub-contexts for creating underlying sinks
    std::shared_ptr<IcebergDeleteSinkContext> delete_sink_ctx;
    std::shared_ptr<IcebergChunkSinkContext> data_sink_ctx;

    // Index of the op_code column in the input chunk (last column)
    int32_t op_code_index = -1;

    // Query-level memory manager for creating child managers for sub-sinks.
    // Set by ConnectorSinkOperatorFactory via set_sink_mem_mgr() after construction.
    SinkMemoryManager* sink_mem_mgr = nullptr;

    void set_sink_mem_mgr(SinkMemoryManager* mgr) override { sink_mem_mgr = mgr; }
};

// IcebergRowDeltaSinkProvider creates IcebergRowDeltaSink for Iceberg UPDATE operations.
// It composes an IcebergDeleteSinkProvider and an IcebergChunkSinkProvider.
class IcebergRowDeltaSinkProvider final : public ConnectorChunkSinkProvider {
public:
    ~IcebergRowDeltaSinkProvider() override = default;

    StatusOr<std::unique_ptr<ConnectorChunkSink>> create_chunk_sink(std::shared_ptr<ConnectorChunkSinkContext> context,
                                                                    int32_t driver_id) override;
};

// IcebergRowDeltaSink routes rows from an input chunk to a delete sink and a data sink
// based on the op_code column. Used for Iceberg UPDATE which atomically deletes old rows
// and inserts new rows (row-level delta / Merge-On-Read).
//
// Op codes:
//   0 = no-op (skip)
//   1 = delete only (position delete)
//   2 = update (delete old row + insert new row)
//   3 = insert only (new data row)
class IcebergRowDeltaSink final : public ConnectorChunkSink {
public:
    // Op code constants
    static constexpr int8_t OP_NO_OP = 0;
    static constexpr int8_t OP_DELETE = 1;
    static constexpr int8_t OP_UPDATE = 2;
    static constexpr int8_t OP_INSERT = 3;

    IcebergRowDeltaSink(std::unique_ptr<ConnectorChunkSink> delete_sink, std::unique_ptr<ConnectorChunkSink> data_sink,
                        int32_t op_code_index, SinkMemoryManager* sink_mem_mgr, RuntimeState* state);

    ~IcebergRowDeltaSink() override = default;

    Status init() override;

    void callback_on_commit(const CommitResult& result) override;

    Status add(const ChunkPtr& chunk) override;

    Status finish() override;

    void rollback() override;

    bool is_finished() override;

private:
    std::unique_ptr<ConnectorChunkSink> _delete_sink;
    std::unique_ptr<ConnectorChunkSink> _data_sink;

    int32_t _op_code_index;

    SinkMemoryManager* _sink_mem_mgr = nullptr;

    // Reused across add() calls to avoid per-chunk heap allocations
    std::vector<uint32_t> _delete_rows;
    std::vector<uint32_t> _data_rows;
};

// A no-op PartitionChunkWriterFactory used by IcebergRowDeltaSink's base class.
// The RowDeltaSink delegates all actual writing to its sub-sinks, so this factory
// is never used to create real writers.
class NopPartitionChunkWriterFactory final : public PartitionChunkWriterFactory {
public:
    ~NopPartitionChunkWriterFactory() override = default;

    Status init() override { return Status::OK(); }

    PartitionChunkWriterPtr create(std::string partition,
                                   std::vector<int8_t> partition_field_null_list) const override {
        return nullptr;
    }
};

} // namespace starrocks::connector
