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

#include "exec/schema_scan_node.h"

#include <boost/algorithm/string.hpp>

#include "column/column_helper.h"
#include "exec/pipeline/scan/schema_scan_context.h"
#include "exec/pipeline/scan/schema_scan_operator.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "util/runtime_profile.h"

namespace starrocks {

SchemaScanNode::SchemaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _tnode(tnode),
          _is_init(false),
          _table_name(tnode.schema_scan_node.table_name),
          _tuple_id(tnode.schema_scan_node.tuple_id),
          _dest_tuple_desc(nullptr),
          _schema_scanner(nullptr) {
    _name = "schema_scan";
}

SchemaScanNode::~SchemaScanNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
}

Status SchemaScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ScanNode::init(tnode, state));
    if (tnode.schema_scan_node.__isset.db) {
        _scanner_param.db = _pool->add(new std::string(tnode.schema_scan_node.db));
    }

    if (tnode.schema_scan_node.__isset.table) {
        _scanner_param.table = _pool->add(new std::string(tnode.schema_scan_node.table));
    }

    if (tnode.schema_scan_node.__isset.wild) {
        _scanner_param.wild = _pool->add(new std::string(tnode.schema_scan_node.wild));
    }

    if (tnode.schema_scan_node.__isset.current_user_ident) {
        _scanner_param.current_user_ident = _pool->add(new TUserIdentity(tnode.schema_scan_node.current_user_ident));
    } else {
        if (tnode.schema_scan_node.__isset.user) {
            _scanner_param.user = _pool->add(new std::string(tnode.schema_scan_node.user));
        }
        if (tnode.schema_scan_node.__isset.user_ip) {
            _scanner_param.user_ip = _pool->add(new std::string(tnode.schema_scan_node.user_ip));
        }
    }

    if (tnode.schema_scan_node.__isset.ip) {
        _scanner_param.ip = _pool->add(new std::string(tnode.schema_scan_node.ip));
    }
    if (tnode.schema_scan_node.__isset.port) {
        _scanner_param.port = tnode.schema_scan_node.port;
    }

    if (tnode.schema_scan_node.__isset.thread_id) {
        _scanner_param.thread_id = tnode.schema_scan_node.thread_id;
    }

    // only for no predicate and limit parameter is set
    if (tnode.conjuncts.empty() && tnode.limit > 0) {
        _scanner_param.without_db_table = true;
        _scanner_param.limit = tnode.limit;
    }
    return Status::OK();
}

Status SchemaScanNode::prepare(RuntimeState* state) {
    if (_is_init) {
        return Status::OK();
    }

    if (nullptr == state) {
        return Status::InternalError("input pointer is nullptr.");
    }

    RETURN_IF_ERROR(ScanNode::prepare(state));

    // get dest tuple desc
    _dest_tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);

    if (nullptr == _dest_tuple_desc) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    const auto* schema_table = static_cast<const SchemaTableDescriptor*>(_dest_tuple_desc->table_desc());

    if (nullptr == schema_table) {
        return Status::InternalError("Failed to get schema table descriptor.");
    }

    _scanner_param._rpc_timer = ADD_TIMER(_runtime_profile, "FERPC");
    _scanner_param._fill_chunk_timer = ADD_TIMER(_runtime_profile, "FillChunk");
    _filter_timer = ADD_TIMER(_runtime_profile, "FilterTime");

    // new one scanner
    _schema_scanner = SchemaScanner::create(schema_table->schema_table_type());

    if (nullptr == _schema_scanner) {
        return Status::InternalError("schema scanner get nullptr pointer.");
    }

    RETURN_IF_ERROR(_schema_scanner->init(&_scanner_param, _pool));

    // check whether we have requested columns in src_slot_descs.
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

    _is_init = true;

    return Status::OK();
}

Status SchemaScanNode::open(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("Open before Init.");
    }

    if (nullptr == state) {
        return Status::InternalError("input pointer is nullptr.");
    }

    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(ExecNode::open(state));

    if (_scanner_param.user) {
        TSetSessionParams param;
        param.__set_user(*_scanner_param.user);
    }

    return _schema_scanner->start(state);
}

Status SchemaScanNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    VLOG(1) << "SchemaScanNode::GetNext";

    DCHECK(state != nullptr && chunk != nullptr && eos != nullptr);
    DCHECK(_is_init);

    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    const std::vector<SlotDescriptor*>& src_slot_descs = _schema_scanner->get_slot_descs();
    // For dummy schema scanner, the src_slot_descs is empty and the result also should be empty
    if (src_slot_descs.empty() || reached_limit() || _is_finished) {
        *eos = true;
        return Status::OK();
    }

    const std::vector<SlotDescriptor*>& dest_slot_descs = _dest_tuple_desc->slots();

    bool scanner_eos = false;
    int row_num = 0;

    ChunkPtr chunk_src = std::make_shared<Chunk>();
    if (nullptr == chunk_src.get()) {
        return Status::InternalError("Failed to allocate new chunk.");
    }

    for (size_t i = 0; i < dest_slot_descs.size(); ++i) {
        DCHECK(dest_slot_descs[i]->is_materialized());
        int j = _index_map[i];
        SlotDescriptor* src_slot = src_slot_descs[j];
        ColumnPtr column = ColumnHelper::create_column(src_slot->type(), src_slot->is_nullable());
        column->reserve(state->chunk_size());
        chunk_src->append_column(std::move(column), src_slot->id());
    }

    // convert src chunk format to dest chunk format to process where clause
    ChunkPtr chunk_dst = std::make_shared<Chunk>();
    if (nullptr == chunk_dst.get()) {
        return Status::InternalError("Failed to allocate new chunk.");
    }

    for (auto dest_slot_desc : dest_slot_descs) {
        ColumnPtr column = ColumnHelper::create_column(dest_slot_desc->type(), dest_slot_desc->is_nullable());
        chunk_dst->append_column(std::move(column), dest_slot_desc->id());
    }

    while (!scanner_eos && chunk_dst->is_empty()) {
        while (row_num < state->chunk_size()) {
            RETURN_IF_ERROR(_schema_scanner->get_next(&chunk_src, &scanner_eos));

            if (scanner_eos) {
                _is_finished = true;
                // if row_num is greater than 0, in this call, eos = false, and eos will be set to true
                // in the next call
                if (row_num == 0) {
                    *eos = true;
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
            if (!_conjunct_ctxs.empty()) {
                RETURN_IF_ERROR(ExecNode::eval_conjuncts(_conjunct_ctxs, chunk_dst.get()));
            }
        }
        row_num = chunk_dst->num_rows();
        chunk_src->reset();
    }

    _num_rows_returned += chunk_dst->num_rows();
    if (reached_limit()) {
        int64_t num_rows_over = _num_rows_returned - _limit;
        chunk_dst->set_num_rows(chunk_dst->num_rows() - num_rows_over);
        COUNTER_SET(_rows_returned_counter, _limit);
    } else {
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    }
    *chunk = std::move(chunk_dst);

    return Status::OK();
}

void SchemaScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        Status::OK();
    }
    exec_debug_action(TExecNodePhase::CLOSE);
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    ScanNode::close(state);
}

void SchemaScanNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "SchemaScanNode(tupleid=" << _tuple_id << " table=" << _table_name;
    *out << ")" << std::endl;

    for (auto i : _children) {
        i->debug_string(indentation_level + 1, out);
    }
}

Status SchemaScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    return Status::OK();
}

std::vector<std::shared_ptr<pipeline::OperatorFactory>> SchemaScanNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
    // the dop of SchemaScanOperator should always be 1.
    size_t dop = 1;

    size_t buffer_capacity = pipeline::ScanOperator::max_buffer_capacity() * dop;
    pipeline::ChunkBufferLimiterPtr buffer_limiter = std::make_unique<pipeline::DynamicChunkBufferLimiter>(
            buffer_capacity, buffer_capacity, _mem_limit, runtime_state()->chunk_size());

    auto scan_op = std::make_shared<pipeline::SchemaScanOperatorFactory>(context->next_operator_id(), this, dop, _tnode,
                                                                         std::move(buffer_limiter));
    return pipeline::decompose_scan_node_to_pipeline(scan_op, this, context);
}

} // namespace starrocks
