// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/pipeline/scan/meta_scan_context.h"
#include "exec/pipeline/source_operator.h"
#include "exec/vectorized/meta_scan_node.h"

namespace starrocks::pipeline {
class MetaScanPrepareOperator : public SourceOperator {
public:
    MetaScanPrepareOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                            const std::string& operator_name, MetaScanContextPtr scan_ctx);
    ~MetaScanPrepareOperator() override = default;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool has_output() const override;
    bool is_finished() const override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

protected:
    virtual Status _prepare_scan_context(RuntimeState* state) = 0;
    MetaScanContextPtr _scan_ctx;
};

class MetaScanPrepareOperatorFactory : public SourceOperatorFactory {
public:
    MetaScanPrepareOperatorFactory(int32_t id, int32_t plan_node_id, const std::string& operator_name,
                                   std::shared_ptr<MetaScanContextFactory> scan_ctx_factory);

    ~MetaScanPrepareOperatorFactory() override = default;

    bool with_morsels() const override { return true; }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

protected:
    std::shared_ptr<MetaScanContextFactory> _scan_ctx_factory;
};

} // namespace starrocks::pipeline