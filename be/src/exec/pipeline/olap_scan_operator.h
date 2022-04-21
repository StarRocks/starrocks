// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan_operator.h"
#include "exec/vectorized/olap_scan_prepare.h"

namespace starrocks {

class ScanNode;
class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;

}; // namespace starrocks

namespace starrocks::pipeline {

class OlapScanOperatorFactory final : public ScanOperatorFactory {
public:
    OlapScanOperatorFactory(int32_t id, ScanNode* scan_node);

    ~OlapScanOperatorFactory() override = default;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    OperatorPtr do_create(int32_t dop, int32_t driver_sequence) override;

    Status parse_conjuncts(RuntimeState* state);
    vectorized::OlapScanConjunctsManager& conjuncts_manager() { return _conjuncts_manager; }
    std::vector<ExprContext*>& not_push_down_conjuncts() { return _not_push_down_conjuncts; }
    std::vector<std::unique_ptr<OlapScanRange>>& key_ranges() { return _key_ranges; }

private:
    std::vector<ExprContext*> _conjunct_ctxs;
    vectorized::OlapScanConjunctsManager _conjuncts_manager;
    std::vector<ExprContext*> _not_push_down_conjuncts;
    std::vector<std::unique_ptr<OlapScanRange>> _key_ranges;
    vectorized::DictOptimizeParser _dict_optimize_parser;
    ObjectPool _obj_pool;
};

class OlapScanOperator final : public ScanOperator {
public:
    OlapScanOperator(OperatorFactory* factory, int32_t id, ScanNode* scan_node,
                     std::atomic<ScanOperatorFactory::SharedPhase>& shared_phase);

    ~OlapScanOperator() override = default;

    Status do_prepare(RuntimeState* state) override;
    Status do_open_shared(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    ChunkSourcePtr create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) override;

    vectorized::OlapScanConjunctsManager& conjuncts_manager() { return _get_factory()->conjuncts_manager(); }
    std::vector<ExprContext*>& not_push_down_conjuncts() { return _get_factory()->not_push_down_conjuncts(); }
    std::vector<std::unique_ptr<OlapScanRange>>& key_ranges() { return _get_factory()->key_ranges(); }

private:
    Status _capture_tablet_rowsets();
    OlapScanOperatorFactory* _get_factory() { return down_cast<OlapScanOperatorFactory*>(_factory); }

    // The row sets of tablets will become stale and be deleted, if compaction occurs
    // and these row sets aren't referenced, which will typically happen when the tablets
    // of the left table are compacted at building the right hash table. Therefore, reference
    // the row sets into _tablet_rowsets in the preparation phase to avoid the row sets being deleted.
    std::vector<std::vector<RowsetSharedPtr>> _tablet_rowsets;
};

} // namespace starrocks::pipeline
