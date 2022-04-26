// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "connector/connector.h"

namespace starrocks {
namespace connector {

class HiveConnector final : public Connector {
public:
    DataSourceProviderPtr create_data_source_provider(vectorized::ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;
};

class HiveDataSourceProvider;

class HiveDataSource final : public DataSource {
public:
    HiveDataSource(HiveDataSourceProvider* provider, const TScanRange& scan_range);
    Status do_open(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    Status do_get_next(RuntimeState* state, vectorized::ChunkPtr* chunk) override;

private:
    HiveDataSourceProvider* _provider;
};

class HiveDataSourceProvider final : public DataSourceProvider {
public:
    HiveDataSourceProvider(vectorized::ConnectorScanNode* scan_node, const TPlanNode& plan_node);
    Status init(RuntimeState* state) override;
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;

private:
    vectorized::ConnectorScanNode* _scan_node;
};

} // namespace connector
} // namespace starrocks