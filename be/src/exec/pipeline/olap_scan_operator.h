// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/scan_operator.h"

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
};

class OlapScanOperator final : public ScanOperator {
public:
    OlapScanOperator(OperatorFactory* factory, int32_t id, ScanNode* scan_node);

    ~OlapScanOperator() override = default;

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
};

} // namespace starrocks::pipeline
