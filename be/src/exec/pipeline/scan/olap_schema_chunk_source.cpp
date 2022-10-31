// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "exec/pipeline/scan/olap_schema_chunk_source.h"

#include <boost/algorithm/string.hpp>

#include "exec/vectorized/schema_scanner.h"
#include "exec/workgroup/work_group.h"

namespace starrocks::pipeline {

OlapSchemaChunkSource::OlapSchemaChunkSource(int32_t scan_operator_id, RuntimeProfile* runtime_profile,
                                             MorselPtr&& morsel, const OlapSchemaScanContextPtr& ctx)
        : ChunkSource(scan_operator_id, runtime_profile, std::move(morsel), ctx->get_chunk_buffer()), _ctx(ctx) {}

OlapSchemaChunkSource::~OlapSchemaChunkSource() = default;

Status OlapSchemaChunkSource::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ChunkSource::prepare(state));
    _dest_tuple_desc = state->desc_tbl().get_tuple_descriptor(_ctx->tuple_id());
    if (_dest_tuple_desc == nullptr) {
        return Status::InternalError("failed to get tuple descriptor");
    }
    const auto* schema_table =
            static_cast<const SchemaTableDescriptor*>(_dest_tuple_desc->table_desc());
    if (schema_table == nullptr) {
        return Status::InternalError("Failed to get schema table descriptor");
    }

    auto param = _ctx->param();
    param->_rpc_timer = ADD_TIMER(_runtime_profile, "FERPC");
    param->_fill_chunk_timer = ADD_TIMER(_runtime_profile, "FillChunk");

    _filter_timer = ADD_TIMER(_runtime_profile, "FilterTime");

    _scanner = vectorized::SchemaScanner::create(schema_table->schema_table_type());
    if (_scanner == nullptr) {
        return Status::InternalError("schema scanner get null pointer");
    }

    RETURN_IF_ERROR(_scanner->init(param, _ctx->object_pool()));
    return _scanner->start(state);
}

void OlapSchemaChunkSource::close(RuntimeState* state) {}

Status OlapSchemaChunkSource::_read_chunk(RuntimeState* state, ChunkPtr* chunk) {
    ChunkPtr chunk_src = std::make_shared<vectorized::Chunk>();
    if (chunk_src == nullptr) {
        return Status::InternalError("Failed to allocated new chunk.");
    }
    const std::vector<SlotDescriptor*>& dest_slot_descs = _dest_tuple_desc->slots();

    ChunkPtr chunk_dst = std::make_shared<vectorized::Chunk>();
    if (chunk_dst == nullptr) {
        return Status::InternalError("Failed to allocate new chunk");
    }
    for (auto dest_slot_desc : dest_slot_descs) {
        ColumnPtr column =
                vectorized::ColumnHelper::create_column(dest_slot_desc->type(), dest_slot_desc->is_nullable());
        chunk_dst->append_column(std::move(column), dest_slot_desc->id());
    }

    bool scanner_eos = false;
    int32_t row_num = 0;
    while (!scanner_eos && chunk_dst->is_empty()) {
        while (row_num < state->chunk_size()) {
            RETURN_IF_ERROR(_scanner->get_next(&chunk_dst, &scanner_eos));
            if (scanner_eos) {
                if (row_num == 0) {
                    return Status::EndOfFile("end of file");
                }
                break;
            }
            row_num++;
        }

        {
            SCOPED_TIMER(_filter_timer);
            auto& conjunct_ctxs = _ctx->conjunct_ctxs();
            if (!conjunct_ctxs.empty()) {
                RETURN_IF_ERROR(ExecNode::eval_conjuncts(conjunct_ctxs, chunk_dst.get()));
            }
        }
        row_num = chunk_dst->num_rows();
    }
    *chunk = std::move(chunk_dst);
    return Status::OK();
}

const workgroup::WorkGroupScanSchedEntity* OlapSchemaChunkSource::_scan_sched_entity(
        const workgroup::WorkGroup* wg) const {
    DCHECK(wg != nullptr);
    return wg->scan_sched_entity();
}
} // namespace starrocks
