// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/pipeline/source_operator.h"
#include "exprs/vectorized/runtime_filter_bank.h"

namespace starrocks {
namespace vectorized {
class RuntimeFilterProbeCollector;
}
namespace pipeline {
class ScanOperator final : public SourceOperator {
public:
    ScanOperator(int32_t id, int32_t plan_node_id, const TOlapScanNode& olap_scan_node,
                 const std::vector<ExprContext*>& conjunct_ctxs,
                 const vectorized::RuntimeFilterProbeCollector& runtime_filters)
            : SourceOperator(id, "olap_scan", plan_node_id),
              _olap_scan_node(olap_scan_node),
              _conjunct_ctxs(conjunct_ctxs),
              _runtime_filters(runtime_filters) {}

    ~ScanOperator() override = default;

    Status prepare(RuntimeState* state) override;

    Status close(RuntimeState* state) override;

    bool has_output() const override;

    bool is_finished() const override;

    void finish(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    void add_morsel(Morsel* morsel) override;

private:
    bool _is_finished = false;
    const TOlapScanNode& _olap_scan_node;
    const std::vector<ExprContext*>& _conjunct_ctxs;
    const vectorized::RuntimeFilterProbeCollector& _runtime_filters;
};

class ScanOperatorFactory final : public OperatorFactory {
public:
    ScanOperatorFactory(int32_t id, int32_t plan_node_id, const TOlapScanNode& olap_scan_node,
                        std::vector<ExprContext*>&& conjunct_ctxs,
                        vectorized::RuntimeFilterProbeCollector&& runtime_filters)
            : OperatorFactory(id, plan_node_id),
              _olap_scan_node(olap_scan_node),
              _conjunct_ctxs(std::move(conjunct_ctxs)),
              _runtime_filters(std::move(runtime_filters)) {}

    ~ScanOperatorFactory() override = default;

    OperatorPtr create(int32_t driver_instance_count, int32_t driver_sequence) override {
        return std::make_shared<ScanOperator>(_id, _plan_node_id, _olap_scan_node, _conjunct_ctxs, _runtime_filters);
    }

    bool is_source() const override { return true; }

private:
    TOlapScanNode _olap_scan_node;
    std::vector<ExprContext*> _conjunct_ctxs;
    vectorized::RuntimeFilterProbeCollector _runtime_filters;
};

} // namespace pipeline
} // namespace starrocks