// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/pipeline/scan/meta_scan_context.h"
#include "exec/pipeline/scan/meta_scan_prepare_operator.h"
#include "exec/pipeline/source_operator.h"
#include "exec/vectorized/olap_meta_scan_node.h"

namespace starrocks::pipeline {

class OlapMetaScanPrepareOperator final : public MetaScanPrepareOperator {
public:
    OlapMetaScanPrepareOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                vectorized::OlapMetaScanNode* const scan_node, MetaScanContextPtr scan_ctx);
    ~OlapMetaScanPrepareOperator() override = default;

private:
    Status _prepare_scan_context(RuntimeState* state) override;

    vectorized::OlapMetaScanNode* const _scan_node;
};

class OlapMetaScanPrepareOperatorFactory final : public MetaScanPrepareOperatorFactory {
public:
    OlapMetaScanPrepareOperatorFactory(int32_t id, int32_t plan_node_id, vectorized::OlapMetaScanNode* const scan_node,
                                       std::shared_ptr<MetaScanContextFactory> scan_ctx_factory);

    ~OlapMetaScanPrepareOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    vectorized::OlapMetaScanNode* const _scan_node;
};

} // namespace starrocks::pipeline
