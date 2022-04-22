// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan_operator.h"

namespace starrocks {
class ScanNode;
}
namespace starrocks::pipeline {

class HdfsScanOperatorFactory final : public ScanOperatorFactory {
public:
    HdfsScanOperatorFactory(int32_t id, ScanNode* scan_node);

    ~HdfsScanOperatorFactory() override = default;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    OperatorPtr do_create(int32_t dop, int32_t driver_sequence) override;
};

class HdfsScanOperator final : public ScanOperator {
public:
    HdfsScanOperator(OperatorFactory* factory, int32_t id, ScanNode* scan_node);

    ~HdfsScanOperator() override = default;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    ChunkSourcePtr create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) override;

private:
};

} // namespace starrocks::pipeline
