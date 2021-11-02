// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/pipeline/dict_decode_operator.h"

#include "column/column_helper.h"
#include "common/logging.h"

namespace starrocks::pipeline {

Status DictDecodeOperator::prepare(RuntimeState* state) {
    Operator::prepare(state);
    return Status::OK();
}

Status DictDecodeOperator::close(RuntimeState* state) {
    Operator::close(state);
    return Status::OK();
}

StatusOr<vectorized::ChunkPtr> DictDecodeOperator::pull_chunk(RuntimeState* state) {
    return std::move(_cur_chunk);
}

Status DictDecodeOperator::push_chunk(RuntimeState* state, const vectorized::ChunkPtr& chunk) {
    Columns decode_columns(_encode_column_cids.size());
    for (size_t i = 0; i < _encode_column_cids.size(); i++) {
        const ColumnPtr& encode_column = chunk->get_column_by_slot_id(_encode_column_cids[i]);
        TypeDescriptor desc;
        desc.type = TYPE_VARCHAR;

        decode_columns[i] = vectorized::ColumnHelper::create_column(desc, encode_column->is_nullable());
        RETURN_IF_ERROR(_decoders[i]->decode(encode_column.get(), decode_columns[i].get()));
    }

    _cur_chunk = std::make_shared<vectorized::Chunk>();
    for (const auto& [k, v] : chunk->get_slot_id_to_index_map()) {
        if (std::find(_encode_column_cids.begin(), _encode_column_cids.end(), k) == _encode_column_cids.end()) {
            auto& col = chunk->get_column_by_slot_id(k);
            _cur_chunk->append_column(col, k);
        }
    }
    for (size_t i = 0; i < decode_columns.size(); i++) {
        _cur_chunk->append_column(decode_columns[i], _decode_column_cids[i]);
    }

    DCHECK_CHUNK(_cur_chunk);
    return Status::OK();
}

Status DictDecodeOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));

    RowDescriptor row_desc;
    RETURN_IF_ERROR(Expr::prepare(_expr_ctxs, state, row_desc));

    const auto& global_dict = state->get_global_dict_map();
    _dict_optimize_parser.set_mutable_dict_maps(state->mutable_global_dict_map());

    DCHECK_EQ(_encode_column_cids.size(), _decode_column_cids.size());
    int need_decode_size = _decode_column_cids.size();
    for (int i = 0; i < need_decode_size; ++i) {
        int need_encode_cid = _encode_column_cids[i];
        auto dict_iter = global_dict.find(need_encode_cid);
        auto dict_not_contains_cid = dict_iter == global_dict.end();
        auto input_has_string_function = _string_functions.find(need_encode_cid) != _string_functions.end();

        if (dict_not_contains_cid && !input_has_string_function) {
            return Status::InternalError(fmt::format("Not found dict for cid:{}", need_encode_cid));
        } else if (dict_not_contains_cid && input_has_string_function) {
            auto& [expr_ctx, dict_ctx] = _string_functions[need_encode_cid];
            DCHECK(expr_ctx->root()->fn().could_apply_dict_optimize);
            _dict_optimize_parser.check_could_apply_dict_optimize(expr_ctx, &dict_ctx);
            DCHECK(dict_ctx.could_apply_dict_optimize);
            _dict_optimize_parser.eval_expr(state, expr_ctx, &dict_ctx, need_encode_cid);
            dict_iter = global_dict.find(need_encode_cid);
            DCHECK(dict_iter != global_dict.end());
            return Status::InternalError(fmt::format("Not found dict for function-called cid:{}", need_encode_cid));
        }

        vectorized::DefaultDecoderPtr decoder = std::make_unique<vectorized::DefaultDecoder>();
        // TODO : avoid copy dict
        decoder->dict = dict_iter->second.second;
        _decoders.emplace_back(std::move(decoder));
    }

    return Status::OK();
}

void DictDecodeOperatorFactory::close(RuntimeState* state) {
    Expr::close(_expr_ctxs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
