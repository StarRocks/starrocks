// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "connector/connector.h"

namespace starrocks::connector {

class LakeConnector final : public Connector {
public:
    ~LakeConnector() override = default;

    DataSourceProviderPtr create_data_source_provider(vectorized::ConnectorScanNode* scan_node,
                                                      const TPlanNode& plan_node) const override;
};

} // namespace starrocks::connector