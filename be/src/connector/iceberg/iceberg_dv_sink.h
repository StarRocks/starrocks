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

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "column/chunk.h"
#include "common/status.h"
#include "connector/common/partition_chunk_writer.h"
#include "connector/common/partitioned_connector_chunk_sink.h"
#include "connector/common/utils.h"
#include "formats/puffin/iceberg_dv_writer.h"

namespace starrocks {
class FileSystem;
class RuntimeState;

namespace connector {

// Context for the Iceberg V3 Deletion Vector sink (DV sub-mode of ICEBERG_DELETE_SINK).
// Unlike IcebergDeleteSinkContext it carries no Parquet writer config: the DV sink writes a
// single Puffin file at finish() via IcebergDvWriter and holds no PartitionChunkWriters.
struct IcebergDvSinkContext : public ConnectorSinkContext {
    std::string path; // data_location (table data root)
    starrocks::TCloudConfiguration cloud_configuration;

    // Runtime state supplied by the Exec-side sink composition.
    RuntimeState* runtime_state = nullptr;

    // "_file"/"_pos" -> slot ref node, used to locate columns in incoming chunks.
    std::unordered_map<std::string, TExprNode> column_slot_map;

    // Partition metadata (empty when the table is unpartitioned).
    std::vector<std::string> partition_column_names;
    std::vector<std::string> transform_exprs;
    std::vector<std::unique_ptr<ColumnEvaluator>> partition_evaluators;

    // Tag inserted into the Puffin file-name prefix (matches the V2 delete sink: "delete").
    std::string writer_tag;
};

// Creates IcebergDvSink instances (one per sink driver).
class IcebergDvSinkProvider final : public ConnectorSinkProvider {
public:
    explicit IcebergDvSinkProvider(std::shared_ptr<IcebergDvSinkContext> ctx);
    ~IcebergDvSinkProvider() override = default;

    StatusOr<std::unique_ptr<ConnectorSink>> create_sink(int32_t driver_id) override;

private:
    std::shared_ptr<IcebergDvSinkContext> _ctx;
};

// IcebergDvSink writes Iceberg V3 Deletion Vectors. It receives delete rows (_file, _pos),
// groups them by referenced data file, accumulates positions into IcebergDvWriter, and at
// finish() writes all bitmaps as deletion-vector-v1 blobs into one Puffin file at the table
// data root, emitting one TIcebergDataFile commit info per data file.
//
// It derives from PartitionedConnectorChunkSink to reuse the ConnectorSinkOperator lifecycle,
// but owns no PartitionChunkWriters (close-time finalize; the DV bitmaps cannot be
// incrementally flushed). callback_on_commit is unused — the sink emits commit info directly
// in finish().
class IcebergDvSink final : public PartitionedConnectorChunkSink {
public:
    IcebergDvSink(std::shared_ptr<FileSystem> fs, std::shared_ptr<LocationProvider> location_provider,
                  std::unordered_map<std::string, TExprNode> column_slot_map,
                  std::vector<std::string> partition_column_names, std::vector<std::string> transform_exprs,
                  std::vector<std::unique_ptr<ColumnEvaluator>>&& partition_evaluators, RuntimeState* state);
    ~IcebergDvSink() override = default;

    Status init(formats::AsyncFlushStreamPoller* poller, RuntimeProfile* profile,
                SinkMemoryManager* sink_mem_mgr) override;
    Status add(const ChunkPtr& chunk) override;
    Status finish() override;
    bool is_finished() override { return _finished; }
    void callback_on_commit(const CommitResult& /*result*/) override {}

    // Test-only accessor for the number of distinct data files accumulated so far.
    size_t num_data_files_for_test() const { return _dv_writer.num_data_files(); }

private:
    struct PartitionInfo {
        std::string partition_path;   // <data_root>/<partition_name>
        std::string null_fingerprint; // per-field '1' if null else '0'
    };

    std::shared_ptr<FileSystem> _fs;
    std::shared_ptr<LocationProvider> _location_provider;
    std::unordered_map<std::string, TExprNode> _column_slot_map;
    std::vector<std::string> _transform_exprs;

    formats::IcebergDvWriter _dv_writer;
    // referenced_data_file -> partition (captured lazily from the first row seen for that file).
    // std::less<> enables string_view lookup without allocating a std::string per row.
    std::map<std::string, PartitionInfo, std::less<>> _partition_by_file;
    bool _finished = false;
};

} // namespace connector
} // namespace starrocks
