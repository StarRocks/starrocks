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

#include "column/vectorized_fwd.h"
#include "connector/connector.h"
#include "exec/jdbc_scanner.h"
namespace starrocks {

class JDBCScanner;

namespace connector {

class JDBCConnector final : public Connector {
public:
    ~JDBCConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    ConnectorType connector_type() const override { return ConnectorType::JDBC; }
};

class JDBCDataSource;
class JDBCDataSourceProvider;

class JDBCDataSourceProvider final : public DataSourceProvider {
public:
    ~JDBCDataSourceProvider() override = default;
    friend class JDBCDataSource;
    JDBCDataSourceProvider(ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;

    bool insert_local_exchange_operator() const override { return true; }
    bool accept_empty_scan_ranges() const override { return false; }
    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

protected:
    ConnectorScanNode* _scan_node;
    const TJDBCScanNode _jdbc_scan_node;
};

class JDBCDataSource final : public DataSource {
public:
    ~JDBCDataSource() override = default;

    JDBCDataSource(const JDBCDataSourceProvider* provider, const TScanRange& scan_range);
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;

    int64_t raw_rows_read() const override;
    int64_t num_rows_read() const override;
    int64_t num_bytes_read() const override;
    int64_t cpu_time_spent() const override;

private:
    Status _create_scanner(RuntimeState* state);

    // ====================================
    const JDBCDataSourceProvider* _provider;
    ObjectPool _obj_pool;
    ObjectPool* _pool = &_obj_pool;
    RuntimeState* _runtime_state = nullptr;
    JDBCScanner* _scanner = nullptr;
    int64_t _rows_read = 0;
    int64_t _bytes_read = 0;
};

} // namespace connector
} // namespace starrocks
