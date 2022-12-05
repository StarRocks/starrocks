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

#include "column/column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "connector/connector.h"

namespace starrocks::connector {
class BinlogConnector final : public Connector {
public:
    ~BinlogConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(vectorized::ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    ConnectorType connector_type() const override { return ConnectorType::BINLOG; }
};

class BinlogDataSource;
class BinlogDataSourceProvider;

class BinlogDataSourceProvider final : public DataSourceProvider {
public:
    ~BinlogDataSourceProvider() override = default;
    friend class BinlogDataSource;
    BinlogDataSourceProvider(vectorized::ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;

    bool insert_local_exchange_operator() const override { return false; }
    bool accept_empty_scan_ranges() const override { return false; }

protected:
    vectorized::ConnectorScanNode* _scan_node;
    const TBinlogScanNode _binlog_scan_node;
};

class BinlogDataSource final : public DataSource {
public:
    ~BinlogDataSource() override = default;

    BinlogDataSource(const BinlogDataSourceProvider* provider, const TScanRange& scan_range);
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, vectorized::ChunkPtr* chunk) override;

    Status set_offset(int64_t table_version, int64_t changelog_id) override;

    int64_t raw_rows_read() const override;
    int64_t num_rows_read() const override;
    int64_t num_bytes_read() const override;
    int64_t cpu_time_spent() const override;

private:
    const BinlogDataSourceProvider* _provider;

    const TBinlogScanRange _scan_range;

    //std::unique_ptr<BinlogReader> _binlog_reader;

    RuntimeProfile::Counter* _read_timer = nullptr;

    int64_t _rows_read_number = 0;
    int64_t _bytes_read = 0;
    int64_t _cpu_time_ns = 0;

    int64_t _epoch_max_rows;
    int64_t _epoch_max_time;

    bool is_reached_epoch_limit = false;

};

} // namespace starrocks::connector
