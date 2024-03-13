// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/mor_processor.h"

namespace starrocks {

Status IcebergMORProcessor::init(RuntimeState* runtime_state, const MORParams& params) {
    THashJoinNode hash_join_node;
    hash_join_node.__set_join_op(TJoinOp::LEFT_ANTI_JOIN);
    hash_join_node.__set_distribution_mode(TJoinDistributionMode::PARTITIONED);
    hash_join_node.__set_is_push_down(false);
    hash_join_node.__set_build_runtime_filters_from_planner(false);
    hash_join_node.__set_output_columns(std::vector<SlotId>());

    std::set<SlotId> probe_output_slot_ids;
    for (const auto slot_desc : params.tuple_desc->slots()) {
        probe_output_slot_ids.insert(slot_desc->id());
    }

    for (const SlotDescriptor* slot_desc : params.equality_slots) {
        const auto column_ref = _pool.add(new ColumnRef(slot_desc));
        const auto expr_context = _pool.add(new ExprContext(column_ref));
        _join_exprs.emplace_back(expr_context);
    }

    RETURN_IF_ERROR(Expr::prepare(_join_exprs, runtime_state));
    RETURN_IF_ERROR(Expr::open(_join_exprs, runtime_state));

    _build_row_desc =
            std::make_unique<RowDescriptor>(runtime_state->desc_tbl().get_tuple_descriptor(params.mor_tuple_id), false);
    _probe_row_desc = std::make_unique<RowDescriptor>(params.tuple_desc, false);

    const auto param = _pool.add(
            new HashJoinerParam(&_pool, hash_join_node, 1, TPlanNodeType::HASH_JOIN_NODE,
                                std::vector<bool>(params.equality_slots.size(), false), _join_exprs, _join_exprs,
                                std::vector<ExprContext*>(), std::vector<ExprContext*>(), *_build_row_desc,
                                *_probe_row_desc, *_probe_row_desc, TPlanNodeType::HDFS_SCAN_NODE,
                                TPlanNodeType::HDFS_SCAN_NODE, true, std::list<RuntimeFilterBuildDescriptor*>(),
                                std::set<SlotId>(), probe_output_slot_ids, TJoinDistributionMode::PARTITIONED, true));

    _hash_joiner = _pool.add(new HashJoiner(*param));
    RETURN_IF_ERROR(_hash_joiner->prepare_builder(runtime_state, _runtime_profile));
    return Status::OK();
}

Status IcebergMORProcessor::build_hash_table(RuntimeState* runtime_state) {
    RETURN_IF_ERROR(_hash_joiner->build_ht(runtime_state));
    _hash_joiner->enter_probe_phase();
    return Status::OK();
}

Status IcebergMORProcessor::append_chunk_to_hashtable(ChunkPtr& chunk) {
    return _hash_joiner->append_chunk_to_ht(chunk);
}

Status IcebergMORProcessor::get_next(RuntimeState* state, ChunkPtr* chunk) {
    if ((*chunk)->is_empty()) {
        return Status::OK();
    }

    if (!_prepared_probe.load()) {
        RETURN_IF_ERROR(_hash_joiner->prepare_prober(state, _runtime_profile));
        _prepared_probe.store(true);
    }

    RETURN_IF_ERROR(_hash_joiner->push_chunk(state, std::move(*chunk)));
    *chunk = std::move(_hash_joiner->pull_chunk(state)).value();
    return Status::OK();
}

void IcebergMORProcessor::close(RuntimeState* runtime_state) {
    if (_hash_joiner) {
        _hash_joiner->enter_eos_phase();
        _hash_joiner->set_prober_finished();
        _hash_joiner->close(runtime_state);
        Expr::close(_join_exprs, runtime_state);
    }
}

} // namespace starrocks
