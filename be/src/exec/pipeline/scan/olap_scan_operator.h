// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan/scan_operator.h"

namespace starrocks {

class ScanNode;
class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;

namespace pipeline {

class OlapScanContext;
using OlapScanContextPtr = std::shared_ptr<OlapScanContext>;

class OlapScanOperatorFactory final : public ScanOperatorFactory {
public:
    OlapScanOperatorFactory(int32_t id, ScanNode* scan_node, OlapScanContextPtr ctx);

    ~OlapScanOperatorFactory() override = default;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    OperatorPtr do_create(int32_t dop, int32_t driver_sequence) override;

private:
    OlapScanContextPtr _ctx;
};

class OlapScanOperator final : public ScanOperator {
public:
    OlapScanOperator(OperatorFactory* factory, int32_t id, int32_t driver_sequence, ScanNode* scan_node,
                     OlapScanContextPtr ctx);

    ~OlapScanOperator() override = default;

    bool maybe_has_output() const override;
    bool must_be_finished() const override;

    Status do_prepare(RuntimeState* state) override;
    void do_close(RuntimeState* state) override;
    ChunkSourcePtr create_chunk_source(MorselPtr morsel, int32_t chunk_source_index) override;

private:
    Status _capture_tablet_rowsets();

    // The row sets of tablets will become stale and be deleted, if compaction occurs
    // and these row sets aren't referenced, which will typically happen when the tablets
    // of the left table are compacted at building the right hash table. Therefore, reference
    // the row sets into _tablet_rowsets in the preparation phase to avoid the row sets being deleted.
    std::vector<std::vector<RowsetSharedPtr>> _tablet_rowsets;

    OlapScanContextPtr _ctx;
};

} // namespace pipeline
} // namespace starrocks
