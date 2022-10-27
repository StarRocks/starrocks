// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/pipeline/scan/olap_meta_scan_context.h"
#include "exec/pipeline/source_operator.h"
#include "exec/vectorized/olap_meta_scan_node.h"

namespace starrocks {

namespace pipeline {

class OlapMetaScanPrepareOperator final : public SourceOperator {
public:
    OlapMetaScanPrepareOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                                vectorized::OlapMetaScanNode* const scan_node, OlapMetaScanContextPtr scan_ctx);
    ~OlapMetaScanPrepareOperator() override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool has_output() const override;
    bool is_finished() const override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    Status _prepare_scan_context(RuntimeState* state);

    vectorized::OlapMetaScanNode* const _scan_node;
    OlapMetaScanContextPtr _scan_ctx;
};

class OlapMetaScanPrepareOperatorFactory final : public SourceOperatorFactory {
public:
    OlapMetaScanPrepareOperatorFactory(int32_t id, int32_t plan_node_id, vectorized::OlapMetaScanNode* const scan_node,
                                       std::shared_ptr<OlapMetaScanContextFactory> scan_ctx_factory);

    ~OlapMetaScanPrepareOperatorFactory() override = default;

    bool with_morsels() const override { return true; }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    vectorized::OlapMetaScanNode* const _scan_node;
    std::shared_ptr<OlapMetaScanContextFactory> _scan_ctx_factory;
};

} // namespace pipeline
} // namespace starrocks