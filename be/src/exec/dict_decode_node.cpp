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

#include "exec/dict_decode_node.h"

#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "common/logging.h"
#include "exec/pipeline/dict_decode_operator.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks {

DictDecodeNode::DictDecodeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs) {}

Status DictDecodeNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    _init_counter();

    std::vector<SlotId> slots;
    for (const auto& [slot_id, texpr] : tnode.decode_node.string_functions) {
        ExprContext* context;
        RETURN_IF_ERROR(Expr::create_expr_tree(_pool, texpr, &context, state));
        _string_functions[slot_id] = std::make_pair(context, DictOptimizeContext{});
        _expr_ctxs.push_back(context);
        slots.emplace_back(slot_id);
    }

    DictOptimizeParser::set_output_slot_id(&_expr_ctxs, slots);
    for (const auto& [encode_id, decode_id] : tnode.decode_node.dict_id_to_string_ids) {
        _encode_column_cids.emplace_back(encode_id);
        _decode_column_cids.emplace_back(decode_id);
    }

    auto& tuple_id = this->_tuple_ids[0];
    for (const auto& dict_id : _decode_column_cids) {
        auto idx = this->row_desc().get_tuple_idx(tuple_id);
        auto& tuple = this->row_desc().tuple_descriptors()[idx];
        for (const auto& slot : tuple->slots()) {
            if (slot->id() == dict_id) {
                _decode_column_types.emplace_back(&slot->type());
                break;
            }
        }
    }

    return Status::OK();
}

void DictDecodeNode::_init_counter() {
    _decode_timer = ADD_TIMER(_runtime_profile, "DictDecodeTime");
}

Status DictDecodeNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_expr_ctxs, state));
    return Status::OK();
}

Status DictDecodeNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(Expr::open(_expr_ctxs, state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(_children[0]->open(state));

    const auto& global_dict = state->get_query_global_dict_map();
    auto* dict_optimize_parser = state->mutable_dict_optimize_parser();

    for (auto& [slot_id, v] : _string_functions) {
        auto dict_iter = global_dict.find(slot_id);
        auto dict_not_contains_cid = dict_iter == global_dict.end();
        if (dict_not_contains_cid) {
            auto& [expr_ctx, dict_ctx] = v;
            dict_optimize_parser->check_could_apply_dict_optimize(expr_ctx, &dict_ctx);
            if (!dict_ctx.could_apply_dict_optimize) {
                return Status::InternalError(fmt::format(
                        "Not found dict for function-called cid:{} it may cause by unsupported function", slot_id));
            }

            RETURN_IF_ERROR(dict_optimize_parser->eval_expression(expr_ctx, &dict_ctx, slot_id));
            auto dict_iter = global_dict.find(slot_id);
            DCHECK(dict_iter != global_dict.end());
            if (dict_iter == global_dict.end()) {
                return Status::InternalError(fmt::format("Eval Expr Error for cid:{}", slot_id));
            }
        }
    }

    DCHECK_EQ(_encode_column_cids.size(), _decode_column_cids.size());
    int need_decode_size = _decode_column_cids.size();
    for (int i = 0; i < need_decode_size; ++i) {
        int need_encode_cid = _encode_column_cids[i];
        auto dict_iter = global_dict.find(need_encode_cid);
        auto dict_not_contains_cid = dict_iter == global_dict.end();

        if (dict_not_contains_cid) {
            if (dict_optimize_parser->eval_dict_expr(need_encode_cid).ok()) {
                dict_iter = global_dict.find(need_encode_cid);
                dict_not_contains_cid = dict_iter == global_dict.end();
            }
        }

        if (dict_not_contains_cid) {
            return Status::InternalError(fmt::format("Not found dict for cid:{}", need_encode_cid));
        }
        // TODO : avoid copy dict
        GlobalDictDecoderPtr decoder = create_global_dict_decoder(dict_iter->second.second);

        _decoders.emplace_back(std::move(decoder));
    }

    return Status::OK();
}

Status DictDecodeNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);
    *eos = false;
    do {
        RETURN_IF_ERROR(_children[0]->get_next(state, chunk, eos));
    } while (!(*eos) && (*chunk)->num_rows() == 0);

    if (*eos) {
        *chunk = nullptr;
        return Status::OK();
    }

    Columns decode_columns(_encode_column_cids.size());
    for (size_t i = 0; i < _encode_column_cids.size(); i++) {
        const ColumnPtr& encode_column = (*chunk)->get_column_by_slot_id(_encode_column_cids[i]);
        TypeDescriptor desc;
        desc.type = TYPE_VARCHAR;

        decode_columns[i] = ColumnHelper::create_column(desc, encode_column->is_nullable());
        RETURN_IF_ERROR(_decoders[i]->decode_string(encode_column.get(), decode_columns[i].get()));
    }

    ChunkPtr nchunk = std::make_shared<Chunk>();
    for (const auto& [k, v] : (*chunk)->get_slot_id_to_index_map()) {
        if (std::find(_encode_column_cids.begin(), _encode_column_cids.end(), k) == _encode_column_cids.end()) {
            auto& col = (*chunk)->get_column_by_slot_id(k);
            nchunk->append_column(col, k);
        }
    }
    for (size_t i = 0; i < decode_columns.size(); i++) {
        nchunk->append_column(decode_columns[i], _decode_column_cids[i]);
    }
    *chunk = nchunk;

    DCHECK_CHUNK(*chunk);
    return Status::OK();
}

void DictDecodeNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    ExecNode::close(state);
    Expr::close(_expr_ctxs, state);
}

pipeline::OpFactories DictDecodeNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;
    OpFactories operators = _children[0]->decompose_to_pipeline(context);
    operators.emplace_back(std::make_shared<DictDecodeOperatorFactory>(
            context->next_operator_id(), id(), std::move(_encode_column_cids), std::move(_decode_column_cids),
            std::move(_decode_column_types), std::move(_expr_ctxs), std::move(_string_functions)));
    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(1, std::move(this->runtime_filter_collector()));
    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(operators.back().get(), context, rc_rf_probe_collector);
    if (limit() != -1) {
        operators.emplace_back(std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }
    return operators;
}

} // namespace starrocks
