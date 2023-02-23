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

#include "column/chunk.h"
#include "column/column.h"
#include "column/stream_chunk.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "connector/connector.h"
#include "exec/pipeline/fragment_context.h"
#include "gen_cpp/PlanNodes_constants.h"
#include "storage/tablet.h"

namespace starrocks::connector {

class BinlogConnector final : public Connector {
public:
    ~BinlogConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    ConnectorType connector_type() const override { return ConnectorType::BINLOG; }
};

class BinlogDataSource;

class BinlogDataSourceProvider final : public DataSourceProvider {
public:
    ~BinlogDataSourceProvider() override = default;
    friend class BinlogDataSource;
    BinlogDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;

    bool insert_local_exchange_operator() const override { return false; }
    bool accept_empty_scan_ranges() const override { return false; }

    bool stream_data_source() const override { return true; }

protected:
    ConnectorScanNode* _scan_node;
    const TBinlogScanNode _binlog_scan_node;
};

using BinlogMetaFieldMap = std::unordered_map<std::string, FieldPtr>;
const std::string BINLOG_OP = g_PlanNodes_constants.BINLOG_OP_COLUMN_NAME;
const std::string BINLOG_VERSION = g_PlanNodes_constants.BINLOG_VERSION_COLUMN_NAME;
const std::string BINLOG_SEQ_ID = g_PlanNodes_constants.BINLOG_SEQ_ID_COLUMN_NAME;
const std::string BINLOG_TIMESTAMP = g_PlanNodes_constants.BINLOG_TIMESTAMP_COLUMN_NAME;

class BinlogDataSource final : public StreamDataSource {
public:
    ~BinlogDataSource() override = default;

    BinlogDataSource(const BinlogDataSourceProvider* provider, const TScanRange& scan_range);
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;

    Status set_offset(int64_t table_version, int64_t changelog_id) override;
    Status reset_status() override;

    int64_t raw_rows_read() const override;
    int64_t num_rows_read() const override;
    int64_t num_bytes_read() const override;
    int64_t cpu_time_spent() const override;

    int64_t num_rows_read_in_epoch() const override;

    // CPU time of this data source in the current epoch.
    int64_t cpu_time_spent_in_epoch() const override;

private:
    StatusOr<TabletSharedPtr> _get_tablet();
    BinlogMetaFieldMap _build_binlog_meta_fields(ColumnId start_cid);
    StatusOr<Schema> _build_binlog_schema();

    const BinlogDataSourceProvider* _provider;
    const TBinlogScanRange _scan_range;
    RuntimeState* _runtime_state = nullptr;
    TabletSharedPtr _tablet;
    // TODO this will be used by BinlogReader
    Schema _binlog_read_schema;

    int64_t _rows_read_number = 0;
    int64_t _bytes_read = 0;
    int64_t _cpu_time_ns = 0;

    int64_t _rows_read_in_epoch = 0;
    int64_t _cpu_time_spent_in_epoch = 0;

    // Mock data for testing
    Status _mock_chunk(Chunk* chunk);
    Status _mock_chunk_test(ChunkPtr* chunk);
    std::atomic<int32_t> _chunk_num = 0;

    // for binlog offset
    int64_t _table_version;
    int64_t _changelog_id;
    bool _is_stream_pipeline = false;
};

} // namespace starrocks::connector