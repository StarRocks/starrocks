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

class HiveDataSource final : public DataSource {
public:
    Status do_open(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    Status do_get_next(RuntimeState* state, vectorized::ChunkPtr* chunk) override;
};

class HiveDataSourceProvider final : public DataSourceProvider {
public:
    Status init(RuntimeState* state) override;
    DataSourcePtr create_data_source(const TScanRange& scan_range) override;
};

} // namespace connector
} // namespace starrocks