// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/pipeline/context_with_dependency.h"
#include "exec/vectorized/olap_scan_prepare.h"

namespace starrocks {

class ScanNode;

namespace vectorized {
class RuntimeFilterProbeCollector;
}

namespace pipeline {

class OlapScanContext;
using OlapScanContextPtr = std::shared_ptr<OlapScanContext>;

using namespace vectorized;

class OlapScanContext final : public ContextWithDependency {
public:
    explicit OlapScanContext(vectorized::OlapScanNode* scan_node) : _scan_node(scan_node) {}

    Status prepare(RuntimeState* state);
    void close(RuntimeState* state) override;

    void set_prepare_finished() { _is_prepare_finished.store(true, std::memory_order_release); }
    bool is_prepare_finished() const { return _is_prepare_finished.load(std::memory_order_acquire); }

    Status parse_conjuncts(RuntimeState* state, const std::vector<ExprContext*>& runtime_in_filters,
                           RuntimeFilterProbeCollector* runtime_bloom_filters);

    vectorized::OlapScanNode* scan_node() const { return _scan_node; }
    vectorized::OlapScanConjunctsManager& conjuncts_manager() { return _conjuncts_manager; }
    const std::vector<ExprContext*>& not_push_down_conjuncts() const { return _not_push_down_conjuncts; }
    const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges() const { return _key_ranges; }

    void update_avg_row_bytes(size_t added_sum_row_bytes, size_t added_num_rows);
    size_t avg_row_bytes() const { return _avg_row_bytes; }

private:
    vectorized::OlapScanNode* _scan_node;

    std::vector<ExprContext*> _conjunct_ctxs;
    vectorized::OlapScanConjunctsManager _conjuncts_manager;
    // The conjuncts couldn't push down to storage engine
    std::vector<ExprContext*> _not_push_down_conjuncts;
    std::vector<std::unique_ptr<OlapScanRange>> _key_ranges;
    vectorized::DictOptimizeParser _dict_optimize_parser;
    ObjectPool _obj_pool;

    std::atomic<bool> _is_prepare_finished{false};

    std::mutex _mutex;
    size_t _sum_row_bytes = 0;
    size_t _num_rows = 0;
    size_t _avg_row_bytes = 0;
};

} // namespace pipeline

} // namespace starrocks
