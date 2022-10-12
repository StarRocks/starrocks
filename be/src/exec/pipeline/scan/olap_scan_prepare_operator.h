// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include "exec/pipeline/scan/olap_scan_context.h"
#include "exec/pipeline/source_operator.h"

namespace starrocks {

class OlapScanNode;

namespace pipeline {

// It does some common preparation works for OlapScan, after its local waiting set is ready
// and before OlapScanOperator::pull_chunk. That is, OlapScanOperator depends on
// it and waits until it is finished.
class OlapScanPrepareOperator final : public SourceOperator {
public:
    OlapScanPrepareOperator(OperatorFactory* factory, int32_t id, const string& name, int32_t plan_node_id,
                            int32_t driver_sequence, OlapScanContextPtr ctx);
    ~OlapScanPrepareOperator() override;

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    bool has_output() const override;
    bool is_finished() const override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    Status _capture_tablet_rowsets();

private:
    // The row sets of tablets will become stale and be deleted, if compaction occurs
    // and these row sets aren't referenced, which will typically happen when the tablets
    // of the left table are compacted at building the right hash table. Therefore, reference
    // the row sets into _tablet_rowsets in the preparation phase to avoid the row sets being deleted.
    std::vector<TabletSharedPtr> _tablets;
    std::vector<std::vector<RowsetSharedPtr>> _tablet_rowsets;

    OlapScanContextPtr _ctx;
};

class OlapScanPrepareOperatorFactory final : public SourceOperatorFactory {
public:
    OlapScanPrepareOperatorFactory(int32_t id, int32_t plan_node_id, vectorized::OlapScanNode* const scan_node,
                                   OlapScanContextFactoryPtr ctx_factory);
    ~OlapScanPrepareOperatorFactory() override = default;

    bool with_morsels() const { return true; }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    vectorized::OlapScanNode* const _scan_node;
    OlapScanContextFactoryPtr _ctx_factory;
};

} // namespace pipeline
} // namespace starrocks
