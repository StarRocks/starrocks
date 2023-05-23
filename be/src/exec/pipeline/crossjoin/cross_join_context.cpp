// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/crossjoin/cross_join_context.h"

#include <algorithm>
#include <numeric>

#include "common/global_types.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exec/vectorized/cross_join_node.h"
#include "exprs/expr.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {

void CrossJoinContext::close(RuntimeState* state) {
    _build_chunks.clear();
}

void CrossJoinContext::incr_prober() {
    ++_num_left_probers;
}

Status CrossJoinContext::finish_one_left_prober(RuntimeState* state) {
    if (_num_left_probers == _num_finished_left_probers.fetch_add(1) + 1) {
        // All the probers have finished, so the builders can be short-circuited.
        set_finished();
    }
    return Status::OK();
}

Status CrossJoinContext::_init_runtime_filter(RuntimeState* state) {
    vectorized::ChunkPtr one_row_chunk = nullptr;
    size_t num_rows = 0;
    for (auto& chunk_ptr : _build_chunks) {
        if (chunk_ptr != nullptr) {
            if (chunk_ptr->num_rows() == 1) {
                one_row_chunk = chunk_ptr;
            }
            num_rows += chunk_ptr->num_rows();
        }
    }
    // build runtime filter for cross join
    if (num_rows == 1) {
        DCHECK(one_row_chunk != nullptr);
        auto* pool = state->obj_pool();
        ASSIGN_OR_RETURN(auto rfs, vectorized::CrossJoinNode::rewrite_runtime_filter(
                                           pool, _rf_descs, one_row_chunk.get(), _conjuncts_ctx));
        _rf_hub->set_collector(_plan_node_id,
                               std::make_unique<RuntimeFilterCollector>(std::move(rfs), RuntimeBloomFilterList{}));
    } else {
        // notify cross join left child
        _rf_hub->set_collector(_plan_node_id, std::make_unique<RuntimeFilterCollector>(RuntimeInFilterList{},
                                                                                       RuntimeBloomFilterList{}));
    }
    return Status::OK();
}
} // namespace starrocks::pipeline