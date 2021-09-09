// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "exec/scan_node.h"
#include "exprs/slot_ref.h"
#include "runtime/descriptors.h"

namespace starrocks {

class RuntimeState;
class Status;

// Currently, for convert chunk to row_batch.
// After all exec nodes support vectorized, could remove these class.
class ConvertScanNode : public ScanNode {
public:
    ConvertScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : ScanNode(pool, tnode, descs) {}

    Status prepare(RuntimeState* state) override;

    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;

    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) = 0;

    Status close(RuntimeState* state) override;

    // Convert old rowbatch content to vectorized chunk
    Status convert_rowbatch_to_chunk(const RowBatch& batch, vectorized::Chunk* result);

    // Ignore this, because this Node can not handle vectorized predicate
    void push_down_predicate(RuntimeState* state, std::list<ExprContext*>* expr_ctxs, bool is_vectorized) override {
        if (!is_vectorized) {
            ExecNode::push_down_predicate(state, expr_ctxs, false);
        }
    }

    void push_down_join_runtime_filter(RuntimeState* state,
                                       vectorized::RuntimeFilterProbeCollector* collector) override {}

protected:
    // Fill vectorized column by slot
    template <PrimitiveType SlotType>
    void _fill_column_with_slot(const RowBatch& batch, SlotRef* slot, vectorized::Column* result);

    template <PrimitiveType SlotType>
    void _fill_data_column_with_slot(vectorized::Column* data_column, void* slot);

    std::unique_ptr<RowBatch> _convert_row_batch;
    std::vector<SlotDescriptor*> _slot_descs;
    std::vector<SlotRef> _slots;
};

} // namespace starrocks
