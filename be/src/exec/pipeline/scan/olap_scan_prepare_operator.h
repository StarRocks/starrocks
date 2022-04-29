// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/pipeline/scan/olap_scan_context.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks {

class OlapScanNode;

namespace pipeline {

class OlapScanPrepareOperator final : public SourceOperator {
public:
    OlapScanPrepareOperator(OperatorFactory* factory, int32_t id, const string& name, int32_t plan_node_id,
                            int32_t driver_sequence, OlapScanContextPtr ctx);

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool has_output() const override;
    bool is_finished() const override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    OlapScanContextPtr _ctx;
};

class OlapScanPrepareOperatorFactory final : public SourceOperatorFactory {
public:
    OlapScanPrepareOperatorFactory(int32_t id, int32_t plan_node_id, OlapScanContextPtr ctx);
    ~OlapScanPrepareOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    OlapScanContextPtr _ctx;
};

} // namespace pipeline
} // namespace starrocks
