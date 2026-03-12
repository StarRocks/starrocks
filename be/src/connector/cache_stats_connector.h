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

#include "connector/connector.h"

namespace starrocks {
class CacheStatsScanner;
} // namespace starrocks

namespace starrocks::connector {

class CacheStatsConnector final : public Connector {
public:
    ~CacheStatsConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;

    ConnectorType connector_type() const override { return ConnectorType::CACHE_STATS; }
};

class CacheStatsDataSource;
class CacheStatsDataSourceProvider final : public DataSourceProvider {
public:
    ~CacheStatsDataSourceProvider() override = default;
    friend class CacheStatsDataSource;
    explicit CacheStatsDataSourceProvider(const TPlanNode& plan_node);

    DataSourcePtr create_data_source(const TScanRange& scan_range) override;

    bool insert_local_exchange_operator() const override { return false; }
    bool accept_empty_scan_ranges() const override { return false; }

    const TupleDescriptor* tuple_descriptor(RuntimeState* state) const override;

private:
    const TCacheStatsScanNode _cache_stats_scan_node;
};

class CacheStatsDataSource final : public DataSource {
public:
    ~CacheStatsDataSource() override = default;

    CacheStatsDataSource(const CacheStatsDataSourceProvider* provider, const TScanRange& scan_range);
    std::string name() const override;
    Status open(RuntimeState* state) override;
    void close(RuntimeState* state) override;
    Status get_next(RuntimeState* state, ChunkPtr* chunk) override;

    int64_t raw_rows_read() const override;
    int64_t num_rows_read() const override;
    int64_t num_bytes_read() const override;
    int64_t cpu_time_spent() const override;

private:
    const CacheStatsDataSourceProvider* _provider;
    TInternalScanRange _scan_range;
    std::unique_ptr<starrocks::CacheStatsScanner> _scanner;
    int64_t _rows_read = 0;
    int64_t _bytes_read = 0;
};

} // namespace starrocks::connector
