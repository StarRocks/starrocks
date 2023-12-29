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

#include "exec/pipeline/dict_decode_operator.h"

#include "column/column_helper.h"
#include "common/logging.h"
#include "runtime/global_dict/decoder.h"

namespace starrocks::pipeline {

Status DictDecodeOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return Status::OK();
}

void DictDecodeOperator::close(RuntimeState* state) {
    _cur_chunk.reset();
    Operator::close(state);
}

StatusOr<ChunkPtr> DictDecodeOperator::pull_chunk(RuntimeState* state) {
    return std::move(_cur_chunk);
}

Status DictDecodeOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    Columns decode_columns(_encode_column_cids.size());
    for (size_t i = 0; i < _encode_column_cids.size(); i++) {
        const ColumnPtr& encode_column = chunk->get_column_by_slot_id(_encode_column_cids[i]);
        TypeDescriptor* desc = _decode_column_types[i];
        decode_columns[i] = ColumnHelper::create_column(*desc, encode_column->is_nullable());
        if (encode_column->only_null()) {
            bool res = decode_columns[i]->append_nulls(encode_column->size());
            DCHECK(res);
            continue;
        }

        if (desc->is_array_type()) {
            RETURN_IF_ERROR(_decoders[i]->decode_array(encode_column.get(), decode_columns[i].get()));
        } else {
            RETURN_IF_ERROR(_decoders[i]->decode_string(encode_column.get(), decode_columns[i].get()));
        }
    }

    _cur_chunk = std::make_shared<Chunk>();

    // The order when traversing Chunk::_slot_id_to_index may be unstable of different instance of DictDecodeOperator
    // Subsequent operator may call Chunk::append_selective which requires Chunk::_slot_id_to_index to be exactly same
    // So here we keep the output chunks with the same order as original chunks
    std::vector<std::pair<ColumnPtr, int>> columns_with_original_order(chunk->columns().size());
    const auto& slot_id_to_index_map = chunk->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        if (std::find(_encode_column_cids.begin(), _encode_column_cids.end(), slot_id) == _encode_column_cids.end()) {
            auto& col = chunk->get_column_by_slot_id(slot_id);
            columns_with_original_order[index] = std::make_pair(std::move(col), slot_id);
        }
    }
    for (size_t i = 0; i < decode_columns.size(); i++) {
        auto it = slot_id_to_index_map.find(_encode_column_cids[i]);
        DCHECK(it != slot_id_to_index_map.end());
        auto& index = it->second;
        columns_with_original_order[index] = std::make_pair(std::move(decode_columns[i]), _decode_column_cids[i]);
    }

    for (auto& item : columns_with_original_order) {
        _cur_chunk->append_column(std::move(item.first), item.second);
    }

    DCHECK_CHUNK(_cur_chunk);
    return Status::OK();
}

Status DictDecodeOperator::reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) {
    _cur_chunk = nullptr;
    _is_finished = false;
    return Status::OK();
}

Status DictDecodeOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    RETURN_IF_ERROR(Expr::prepare(_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_expr_ctxs, state));

    const auto& global_dict = state->get_query_global_dict_map();
    auto dict_optimize_parser = state->mutable_dict_optimize_parser();

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

void DictDecodeOperatorFactory::close(RuntimeState* state) {
    Expr::close(_expr_ctxs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
