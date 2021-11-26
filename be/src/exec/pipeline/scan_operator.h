// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <optional>

#include "exec/pipeline/source_operator.h"
#include "exprs/vectorized/runtime_filter_bank.h"
#include "runtime/global_dicts.h"
#include "util/blocking_queue.hpp"
#include "util/priority_thread_pool.hpp"

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

    bool pending_finish() override;

    bool is_finished() const override;

    void set_finishing(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;
    void set_io_threads(PriorityThreadPool* io_threads) { _io_threads = io_threads; }

private:
    // This method is only invoked when current morsel is reached eof
    // and all cached chunk of this morsel has benn read out
    void _pickup_morsel(RuntimeState* state);
    void _trigger_next_scan(RuntimeState* state);

private:
    // TODO(hcf) ugly, remove this later
    RuntimeState* _state = nullptr;

    const size_t _batch_size = config::pipeline_io_buffer_size;
    mutable bool _is_finished = false;
    std::atomic_bool _is_io_task_active = false;
    const TOlapScanNode& _olap_scan_node;
    const std::vector<ExprContext*>& _conjunct_ctxs;
    const vectorized::RuntimeFilterProbeCollector& _runtime_filters;
    PriorityThreadPool* _io_threads = nullptr;
    std::vector<std::string> _unused_output_columns;
};

class ScanOperatorFactory final : public SourceOperatorFactory {
public:
    ScanOperatorFactory(int32_t id, int32_t plan_node_id, const TOlapScanNode& olap_scan_node,
                        std::vector<ExprContext*>&& conjunct_ctxs,
                        vectorized::RuntimeFilterProbeCollector&& runtime_filters)
            : SourceOperatorFactory(id, "olap_scan", plan_node_id),
              _olap_scan_node(olap_scan_node),
              _conjunct_ctxs(std::move(conjunct_ctxs)),
              _runtime_filters(std::move(runtime_filters)) {}

    ~ScanOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        return std::make_shared<ScanOperator>(_id, _plan_node_id, _olap_scan_node, _conjunct_ctxs, _runtime_filters);
    }

    // ScanOperator needs to attach MorselQueue.
    bool with_morsels() const override { return true; }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

private:
    const TOlapScanNode& _olap_scan_node;
    std::vector<ExprContext*> _conjunct_ctxs;
    vectorized::RuntimeFilterProbeCollector _runtime_filters;
};

} // namespace pipeline
} // namespace starrocks
