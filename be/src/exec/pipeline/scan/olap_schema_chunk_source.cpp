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

#include "exec/pipeline/scan/olap_schema_chunk_source.h"

#include <boost/algorithm/string.hpp>

#include "exec/schema_scanner.h"
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
    const auto* schema_table = static_cast<const SchemaTableDescriptor*>(_dest_tuple_desc->table_desc());
    if (schema_table == nullptr) {
        return Status::InternalError("Failed to get schema table descriptor");
    }

    auto param = _ctx->param();
    param->_rpc_timer = ADD_TIMER(_runtime_profile, "FERPC");
    param->_fill_chunk_timer = ADD_TIMER(_runtime_profile, "FillChunk");

    _filter_timer = ADD_TIMER(_runtime_profile, "FilterTime");

    _schema_scanner = SchemaScanner::create(schema_table->schema_table_type());
    if (_schema_scanner == nullptr) {
        return Status::InternalError("schema scanner get null pointer");
    }

    RETURN_IF_ERROR(_schema_scanner->init(param, _ctx->object_pool()));

    const std::vector<SlotDescriptor*>& src_slot_descs = _schema_scanner->get_slot_descs();
    const std::vector<SlotDescriptor*>& dest_slot_descs = _dest_tuple_desc->slots();
    int slot_num = dest_slot_descs.size();
    if (src_slot_descs.empty()) {
        slot_num = 0;
    } else {
        _index_map.resize(slot_num);
    }
    for (int i = 0; i < slot_num; ++i) {
        int j = 0;
        for (; j < src_slot_descs.size(); ++j) {
            if (boost::iequals(dest_slot_descs[i]->col_name(), src_slot_descs[j]->col_name())) {
                break;
            }
        }

        if (j >= src_slot_descs.size()) {
            LOG(WARNING) << "no match column for this column(" << dest_slot_descs[i]->col_name() << ")";
            return Status::InternalError("no match column for this column.");
        }

        if (src_slot_descs[j]->type().type != dest_slot_descs[i]->type().type) {
            LOG(WARNING) << "schema not match. input is " << src_slot_descs[j]->type() << " and output is "
                         << dest_slot_descs[i]->type();
            return Status::InternalError("schema not match.");
        }
        _index_map[i] = j;
    }

    return _schema_scanner->start(state);
}

void OlapSchemaChunkSource::close(RuntimeState* state) {}

Status OlapSchemaChunkSource::_read_chunk(RuntimeState* state, ChunkPtr* chunk) {
    const std::vector<SlotDescriptor*>& src_slot_descs = _schema_scanner->get_slot_descs();
    const std::vector<SlotDescriptor*>& dest_slot_descs = _dest_tuple_desc->slots();

    // For dummy schema scanner, the src_slot_descs is empty and the result also should be empty
    if (src_slot_descs.empty()) {
        return Status::EndOfFile("end of file");
    }

    ChunkPtr chunk_src = std::make_shared<Chunk>();
    if (chunk_src == nullptr) {
        return Status::InternalError("Failed to allocated new chunk");
    }

    for (size_t i = 0; i < dest_slot_descs.size(); ++i) {
        DCHECK(dest_slot_descs[i]->is_materialized());
        int j = _index_map[i];
        SlotDescriptor* src_slot = src_slot_descs[j];
        ColumnPtr column = ColumnHelper::create_column(src_slot->type(), src_slot->is_nullable());
        column->reserve(state->chunk_size());
        chunk_src->append_column(std::move(column), src_slot->id());
    }

    ChunkPtr chunk_dst = std::make_shared<Chunk>();
    if (chunk_dst == nullptr) {
        return Status::InternalError("Failed to allocate new chunk");
    }

    for (auto dest_slot_desc : dest_slot_descs) {
        ColumnPtr column = ColumnHelper::create_column(dest_slot_desc->type(), dest_slot_desc->is_nullable());
        chunk_dst->append_column(std::move(column), dest_slot_desc->id());
    }

    bool scanner_eos = false;
    int32_t row_num = 0;

    while (!scanner_eos && chunk_dst->is_empty()) {
        while (row_num < state->chunk_size()) {
            RETURN_IF_ERROR(_schema_scanner->get_next(&chunk_src, &scanner_eos));
            if (scanner_eos) {
                if (row_num == 0) {
                    return Status::EndOfFile("end of file");
                }
                break;
            }
            row_num++;
        }

        for (size_t i = 0; i < dest_slot_descs.size(); ++i) {
            int j = _index_map[i];
            ColumnPtr& src_column = chunk_src->get_column_by_slot_id(src_slot_descs[j]->id());
            ColumnPtr& dst_column = chunk_dst->get_column_by_slot_id(dest_slot_descs[i]->id());
            dst_column->append(*src_column);
        }

        {
            SCOPED_TIMER(_filter_timer);
            auto& conjunct_ctxs = _ctx->conjunct_ctxs();
            if (!conjunct_ctxs.empty()) {
                RETURN_IF_ERROR(ExecNode::eval_conjuncts(conjunct_ctxs, chunk_dst.get()));
            }
        }
        row_num = chunk_dst->num_rows();
        chunk_src->reset();
    }
    *chunk = std::move(chunk_dst);
    return Status::OK();
}

const workgroup::WorkGroupScanSchedEntity* OlapSchemaChunkSource::_scan_sched_entity(
        const workgroup::WorkGroup* wg) const {
    DCHECK(wg != nullptr);
    return wg->scan_sched_entity();
}
} // namespace starrocks::pipeline
