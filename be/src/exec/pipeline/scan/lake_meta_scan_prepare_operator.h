// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/pipeline/scan/meta_scan_prepare_operator.h"

#include "exec/pipeline/scan/meta_scan_context.h"
#include "exec/pipeline/source_operator.h"
#include "exec/vectorized/lake_meta_scan_node.h"

namespace starrocks::pipeline {

class LakeMetaScanPrepareOperator final : public MetaScanPrepareOperator {
public:
    LakeMetaScanPrepareOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                vectorized::LakeMetaScanNode* const scan_node, MetaScanContextPtr scan_ctx);
    ~LakeMetaScanPrepareOperator() = default;

private:
    Status _prepare_scan_context(RuntimeState* state) override;

    vectorized::LakeMetaScanNode* const _scan_node;
}; 

class LakeMetaScanPrepareOperatorFactory final : public MetaScanPrepareOperatorFactory {
public:
    LakeMetaScanPrepareOperatorFactory(int32_t id, int32_t plan_node_id, vectorized::LakeMetaScanNode* const scan_node,
                                       std::shared_ptr<MetaScanContextFactory> scan_ctx_factory);

    ~LakeMetaScanPrepareOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    vectorized::LakeMetaScanNode* const _scan_node;
};

} // namespace starrocks::pipeline