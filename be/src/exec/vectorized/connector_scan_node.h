// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <memory>

#include "connector/connector.h"
#include "env/env.h"
#include "exec/scan_node.h"

namespace starrocks::vectorized {

class ConnectorScanNode final : public starrocks::ScanNode {
public:
    ConnectorScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~ConnectorScanNode() override;

    Status init(const TPlanNode& tnode, RuntimeState* state) override;
    std::vector<std::shared_ptr<pipeline::OperatorFactory>> decompose_to_pipeline(
            pipeline::PipelineBuilderContext* context) override;
    connector::DataSourceProvider* data_source_provider() { return _data_source_provider.get(); }

private:
    RuntimeState* _runtime_state;
    connector::DataSourceProviderPtr _data_source_provider;
};
} // namespace starrocks::vectorized
