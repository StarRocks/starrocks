// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/dict_decode_node.h"

#include "column/chunk.h"
#include "column/column_helper.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks::vectorized {

DictDecodeNode::DictDecodeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs), _decode_node(tnode.decode_node) {}

Status DictDecodeNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    _init_counter();
    return Status::OK();
}

void DictDecodeNode::_init_counter() {
    _decode_timer = ADD_TIMER(_runtime_profile, "DictDecodeTime");
}

Status DictDecodeNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));

    auto global_dict = state->get_global_dict_map();
    for (auto it : _decode_node.dict_id_to_string_ids) {
        _encode_column_cids.emplace_back(it.first);
        _decode_column_cids.emplace_back(it.second);
        auto dict_iter = global_dict.find(it.first);
        if (dict_iter == global_dict.end()) {
            return Status::InternalError("Not find dict");
        }
        DefaultDecoder decoder = std::make_unique<DictDecoder<TYPE_INT, RGlobalDictMap, TYPE_VARCHAR>>();
        decoder->dict = dict_iter->second.second;
        _decoders.emplace_back(std::move(decoder));
    }

    return Status::OK();
}

Status DictDecodeNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(_children[0]->open(state));
    return Status::OK();
}

Status DictDecodeNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
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
        RETURN_IF_ERROR(_decoders[i]->decode(encode_column.get(), decode_columns[i].get()));
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

Status DictDecodeNode::close(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::close(state));
    return Status::OK();
}

} // namespace starrocks::vectorized
