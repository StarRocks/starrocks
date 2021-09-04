// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/scan_operator.h"

#include "column/chunk.h"
#include "exec/pipeline/olap_chunk_source.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

Status ScanOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);
    RowDescriptor row_desc;
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state, row_desc, get_memtracker()));
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));
    _chunk_source->prepare(state);
    return Status::OK();
}

Status ScanOperator::close(RuntimeState* state) {
    Expr::close(_conjunct_ctxs, state);
    _chunk_source->close(state);
    Operator::close(state);
    return Status::OK();
}

bool ScanOperator::has_output() const {
    return _chunk_source->has_next_chunk();
}

bool ScanOperator::is_finished() const {
    return _is_finished;
}

void ScanOperator::finish(RuntimeState* state) {
    _is_finished = true;
    _chunk_source->close(state);
}

StatusOr<vectorized::ChunkPtr> ScanOperator::pull_chunk(RuntimeState* state) {
    StatusOr<vectorized::ChunkPtr> chunk = _chunk_source->get_next_chunk();
    if (!chunk.ok()) {
        _is_finished = true;
    }
    return chunk;
}

void ScanOperator::add_morsel(Morsel* morsel) {
    _chunk_source =
            std::make_unique<OlapChunkSource>(morsel, _olap_scan_node.tuple_id, _conjunct_ctxs, _runtime_filters,
                                              _olap_scan_node.key_column_name, _olap_scan_node.is_preaggregation);
}
} // namespace starrocks::pipeline