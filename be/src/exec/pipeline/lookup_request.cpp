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

#include "exec/pipeline/lookup_request.h"
#include <brpc/controller.h>

#include "column/chunk.h"
#include "column/vectorized_fwd.h"
#include "serde/column_array_serde.h"
#include "storage/range.h"
#include "util/raw_container.h"
#include "connector/hive_connector.h"
#include "runtime/descriptors.h"
#include "util/logging.h"
#include "exec/pipeline/scan/glm_manager.h"
#include "exec/pipeline/query_context.h"
#include "util/runtime_profile.h"
#include "exec/sorting/sorting.h"
#include "exec/pipeline/lookup_operator.h"

namespace starrocks::pipeline {

Status LocalLookUpRequestContext::collect_input_columns(ChunkPtr chunk) {
    // put all related columns into chunk, include source_id column and other related columns
    size_t num_rows = fetch_ctx->request_chunk->num_rows();
    for (const auto& [slot_id, idx] : fetch_ctx->request_chunk->get_slot_id_to_index_map()) {
        auto src_col = fetch_ctx->request_chunk->get_column_by_index(idx);
        auto dst_col = chunk->get_column_by_slot_id(slot_id);
        dst_col->append(*src_col, 0, num_rows);
    }
    chunk->check_or_die();
    return Status::OK();
}
StatusOr<size_t> LocalLookUpRequestContext::fill_response(const ChunkPtr& result_chunk, SlotId source_id_slot,
                                                 const std::vector<SlotDescriptor*>& slots, size_t start_offset) {
    size_t num_rows = fetch_ctx->request_chunk->num_rows();
    for (const auto& slot : slots) {
        auto src_col = result_chunk->get_column_by_slot_id(slot->id());
        auto dst_col = src_col->clone_empty();
        dst_col->append(*src_col, start_offset, num_rows);
        DCHECK(!fetch_ctx->response_columns.contains(slot->id())) << "slot id: " << slot->id()
                                                      << " already exists in response columns";
        fetch_ctx->response_columns[slot->id()] = std::move(dst_col);
    }
    return num_rows;
}

void LocalLookUpRequestContext::callback(const Status& status) {
    // @TODO call back function
    fetch_ctx->unit->finished_request_num++;
}

Status RemoteLookUpRequestContext::collect_input_columns(ChunkPtr chunk) {
    request_chunk = std::make_shared<Chunk>();
    for (size_t i = 0;i < request->request_columns_size(); i++) {
        const auto& pcolumn = request->request_columns(i);
        SlotId slot_id = pcolumn.slot_id();
        int64_t data_size = pcolumn.data_size();
        // @TODO we should know slot desc
        auto dst_col = chunk->get_column_by_slot_id(slot_id);
        auto col = dst_col->clone_empty();
        DLOG(INFO) << "deserialize column, slot_id: " << slot_id << ", data_size: " << data_size << ", column: " << col->get_name();
        const uint8_t* buff = reinterpret_cast<const uint8_t*>(pcolumn.data().data());
        auto ret = serde::ColumnArraySerde::deserialize(buff, col.get());
        if (ret == nullptr) {
            auto msg = fmt::format("deserialize column failed, slot_id: {}, data_size: {}", slot_id, data_size);
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
        dst_col->append(*col, 0, col->size());
        request_chunk->append_column(std::move(col), slot_id);
    }
    chunk->check_or_die();
    DLOG(INFO) << "RemoteLookUpRequestContext collect input columns: " << chunk->debug_columns();
    return Status::OK();
}

StatusOr<size_t> RemoteLookUpRequestContext::fill_response(const ChunkPtr& result_chunk, SlotId source_id_slot,
                                                   const std::vector<SlotDescriptor*>& slots, size_t start_offset) {
    size_t num_rows = request_chunk->num_rows();
    DLOG(INFO) << "RemoteLookUpRequestContext fill response, num_rows: " << num_rows << ", slots: " << slots.size();
    std::vector<ColumnPtr> columns;
    size_t max_serialized_size = 0;
    for (const auto& slot : slots) {
        auto src_col = result_chunk->get_column_by_slot_id(slot->id());
        auto dst_col = src_col->clone_empty();
        dst_col->append(*src_col, start_offset, num_rows);
        max_serialized_size += serde::ColumnArraySerde::max_serialized_size(*dst_col);
        columns.emplace_back(std::move(dst_col));
    }
    // @TODO reuse serialize buffer
    raw::RawString serialize_buffer;
    serialize_buffer.resize(max_serialized_size);
    uint8_t* buff = reinterpret_cast<uint8_t*>(serialize_buffer.data());
    uint8_t* begin = buff;
    for (size_t i = 0;i < slots.size();i++) {
        auto column = columns[i];
        auto pcolumn = response->add_columns();
        pcolumn->set_slot_id(slots[i]->id());
        uint8_t* start = buff;
        buff = serde::ColumnArraySerde::serialize(*column, buff);
        pcolumn->set_data_size(buff - start);
        DLOG(INFO) << "serialize column: " << slots[i]->id() << ", " << column->get_name() << ", data_size: " << (buff - start);
    }
    size_t actual_serialize_size = buff - begin;
    auto* brpc_cntl = static_cast<brpc::Controller*>(cntl);
    brpc_cntl->response_attachment().append(serialize_buffer.data(), actual_serialize_size);

    return num_rows;
}

void RemoteLookUpRequestContext::callback(const Status& status) {
    DLOG(INFO) << "RemoteLookUpRequestContext callback: " << status.to_string();
    status.to_protobuf(response->mutable_status());
    // @TODO fill response
    done->Run();
}

StatusOr<ChunkPtr> LookUpTask::_sort_chunk(RuntimeState* state, const ChunkPtr& chunk,
                                               const Columns& order_by_columns) {
    // @TODO(silverbullet233): reuse sort descs
    SortDescs sort_descs;
    sort_descs.descs.reserve(order_by_columns.size());
    for (size_t i = 0; i < order_by_columns.size(); i++) {
        sort_descs.descs.emplace_back(true, true);
    }
    _ctx->permutation.resize(0);

    RETURN_IF_ERROR(sort_and_tie_columns(state->cancelled_ref(), order_by_columns, sort_descs, &_ctx->permutation));
    auto sorted_chunk = chunk->clone_empty_with_slot(chunk->num_rows());
    materialize_by_permutation(sorted_chunk.get(), {chunk}, _ctx->permutation);

    return sorted_chunk;
}


StatusOr<ChunkPtr> IcebergV3LookUpTask::_calculate_row_id_range(RuntimeState* state, const ChunkPtr& request_chunk,
    phmap::flat_hash_map<int32_t, std::shared_ptr<SparseRange<int64_t>>>* row_id_ranges,
    Buffer<uint32_t>* replicated_offsets) {
    SCOPED_TIMER(_ctx->parent->_calculate_row_id_range_timer);
    // 1. add position column
    UInt32Column::Ptr position_column = UInt32Column::create();
    position_column->resize_uninitialized(request_chunk->num_rows());
    auto& position_data = position_column->get_data();
    for (size_t i = 0;i < request_chunk->num_rows();i++) {
        position_data[i] = i;
    }
    request_chunk->append_column(std::move(position_column), Chunk::SORT_ORDINAL_COLUMN_SLOT_ID);
    request_chunk->check_or_die();
    // 2. sort by _row_id
    // @TODO should use fetch ref slots
    auto scan_range_id_column = request_chunk->get_column_by_slot_id(_ctx->fetch_ref_slot_ids[0]);
    auto row_id_column = request_chunk->get_column_by_slot_id(_ctx->fetch_ref_slot_ids[1]);


    ASSIGN_OR_RETURN(auto sorted_chunk, _sort_chunk(state, request_chunk, {scan_range_id_column, row_id_column}));


    // 3. calculate row_id range and replicated_offsets
    const auto& nullable_scan_range_id_column = 
        down_cast<NullableColumn*>(sorted_chunk->get_column_by_slot_id(_ctx->fetch_ref_slot_ids[0]).get());
    DCHECK(!nullable_scan_range_id_column->has_null()) << "scan_range_id column should not have null";
    auto ordered_scan_range_id_column = Int32Column::static_pointer_cast(nullable_scan_range_id_column->data_column());
    const auto& ordered_scan_range_ids = ordered_scan_range_id_column->get_data();

    const auto& nullable_row_id_column = 
        down_cast<NullableColumn*>(sorted_chunk->get_column_by_slot_id(_ctx->fetch_ref_slot_ids[1]).get());
    DCHECK(!nullable_row_id_column->has_null()) << "row_id column should not have null";
    auto ordered_row_id_column = Int64Column::static_pointer_cast(nullable_row_id_column->data_column());
    const auto& ordered_row_ids = ordered_row_id_column->get_data();

    size_t num_rows = ordered_scan_range_id_column->size();

    int32_t cur_scan_range_id = ordered_scan_range_ids[0];
    int64_t cur_row_id = ordered_row_ids[0];
    Range<int64_t> cur_range(cur_row_id, cur_row_id + 1);

    replicated_offsets->emplace_back(0);
    replicated_offsets->emplace_back(1);

    bool has_duplicated_row = false;
    for (size_t i = 1;i < num_rows;i++) {
        int32_t scan_range_id = ordered_scan_range_ids[i];
        int64_t row_id = ordered_row_ids[i];
        if (scan_range_id == cur_scan_range_id) {
            // same scan range, check if need add a new range
            if (row_id == cur_range.end() - 1) {
                // duplicated row_ids, do nothing, we should mark which idx is duplicated
                replicated_offsets->back()++;
                has_duplicated_row = true;
                continue;
            }
            if (row_id == cur_range.end()) {
                // continous range, just expand current range
                cur_range.expand(1);
            } else {
                // not continous, add the old one into seg_ranges
                auto [iter, _] = row_id_ranges->try_emplace(cur_scan_range_id, std::make_shared<SparseRange<int64_t>>());
                iter->second->add(cur_range);
                cur_range = Range<int64_t>(row_id, row_id + 1);
            }
        } else {
            // move to next scan range, we should add the old range into seg_ranges
            auto [iter, _] = row_id_ranges->try_emplace(cur_scan_range_id, std::make_shared<SparseRange<int64_t>>());
            iter->second->add(cur_range);
            // reset all
            cur_scan_range_id = scan_range_id;
            cur_range = Range<int64_t>(row_id, row_id + 1);
        }
        replicated_offsets->emplace_back(replicated_offsets->back() + 1);
    }
    // handle the last one
    auto [iter, _] = row_id_ranges->try_emplace(cur_scan_range_id, std::make_shared<SparseRange<int64_t>>());
    iter->second->add(cur_range);
    for (const auto& [scan_range_id, range] : *row_id_ranges) {
        DLOG(INFO) << "scan_range_id: " << scan_range_id << ", range: " << range->to_string();
    }

    if (!has_duplicated_row) {
        replicated_offsets->clear();
    }
    return sorted_chunk;
}


TExpr create_between_expr(int32_t slot_id, int64_t start, int64_t end) {
    TExpr expr;
    std::vector<TExprNode>& nodes = expr.nodes;
    
    // 1. 根节点：COMPOUND_AND
    TExprNode and_node;
    and_node.node_type = TExprNodeType::COMPOUND_PRED;
    and_node.opcode = TExprOpcode::COMPOUND_AND;
    and_node.__isset.opcode = true;
    and_node.num_children = 2;
    and_node.is_nullable = true;  // 因为a是nullable bigint
    
    // 设置返回类型为BOOLEAN
    TTypeDesc bool_type;
    TTypeNode bool_type_node;
    bool_type_node.type = TTypeNodeType::SCALAR;
    bool_type_node.__isset.scalar_type = true;
    bool_type_node.scalar_type.type = TPrimitiveType::BOOLEAN;
    bool_type.types.push_back(bool_type_node);
    and_node.type = bool_type;
    
    nodes.push_back(and_node);
    
    // 2. 左子节点：a >= 1
    TExprNode ge_node;
    ge_node.node_type = TExprNodeType::BINARY_PRED;
    ge_node.opcode = TExprOpcode::GE;
    ge_node.__isset.opcode = true;
    ge_node.num_children = 2;
    ge_node.is_nullable = true;
    ge_node.child_type = TPrimitiveType::BIGINT;
    ge_node.__isset.child_type = true;
    ge_node.type = bool_type;  // 返回BOOLEAN
    
    nodes.push_back(ge_node);
    
    // 3. SlotRef: a (slot_id=5)
    TExprNode slot_ref_1;
    slot_ref_1.node_type = TExprNodeType::SLOT_REF;
    slot_ref_1.num_children = 0;
    slot_ref_1.is_nullable = true;
    
    // 设置SlotRef类型为nullable bigint
    TTypeDesc bigint_type;
    TTypeNode bigint_type_node;
    bigint_type_node.type = TTypeNodeType::SCALAR;
    bigint_type_node.__isset.scalar_type = true;
    bigint_type_node.scalar_type.type = TPrimitiveType::BIGINT;
    bigint_type.types.push_back(bigint_type_node);
    slot_ref_1.type = bigint_type;
    
    // 设置slot信息
    TSlotRef slot_ref_info_1;
    slot_ref_info_1.slot_id = slot_id;  // 5
    slot_ref_info_1.tuple_id = 0;
    slot_ref_1.slot_ref = slot_ref_info_1;
    slot_ref_1.__isset.slot_ref = true;
    
    nodes.push_back(slot_ref_1);
    
    // 4. 字面量: 1
    TExprNode literal_1;
    literal_1.node_type = TExprNodeType::INT_LITERAL;
    literal_1.num_children = 0;
    literal_1.is_nullable = false;
    
    // 设置字面量类型为bigint（与a的类型匹配）
    literal_1.type = bigint_type;
    
    TIntLiteral int_literal_1;
    int_literal_1.value = start;
    literal_1.int_literal = int_literal_1;
    literal_1.__isset.int_literal = true;
    
    nodes.push_back(literal_1);
    
    // 5. 右子节点：a <= 10
    TExprNode le_node;
    le_node.node_type = TExprNodeType::BINARY_PRED;
    le_node.opcode = TExprOpcode::LT;
    le_node.__isset.opcode = true;
    le_node.num_children = 2;
    le_node.is_nullable = true;
    le_node.child_type = TPrimitiveType::BIGINT;
    le_node.__isset.child_type = true;
    le_node.type = bool_type;
    
    nodes.push_back(le_node);
    
    // 6. SlotRef: a (再次引用同一个slot)
    TExprNode slot_ref_2;
    slot_ref_2.node_type = TExprNodeType::SLOT_REF;
    slot_ref_2.num_children = 0;
    slot_ref_2.is_nullable = true;
    slot_ref_2.type = bigint_type;
    
    TSlotRef slot_ref_info_2;
    slot_ref_info_2.slot_id = slot_id;  // 5
    slot_ref_info_2.tuple_id = 0;
    slot_ref_2.slot_ref = slot_ref_info_2;
    slot_ref_2.__isset.slot_ref = true;
    
    nodes.push_back(slot_ref_2);
    
    // 7. 字面量: 10
    TExprNode literal_10;
    literal_10.node_type = TExprNodeType::INT_LITERAL;
    literal_10.num_children = 0;
    literal_10.is_nullable = false;
    literal_10.type = bigint_type;
    
    TIntLiteral int_literal_10;
    int_literal_10.value = end;
    literal_10.int_literal = int_literal_10;
    literal_10.__isset.int_literal = true;
    
    nodes.push_back(literal_10);
    
    return expr;
}

TExpr IcebergV3LookUpTask::create_row_id_filter_expr(SlotId slot_id, const SparseRange<int64_t>& row_id_range) {
    SCOPED_TIMER(_ctx->parent->_build_row_id_filter_timer);
    TExpr expr;
    std::vector<TExprNode>& nodes = expr.nodes;
    
    if (row_id_range.empty()) {
        // Return a false literal if no ranges
        TExprNode false_node;
        false_node.node_type = TExprNodeType::BOOL_LITERAL;
        false_node.num_children = 0;
        false_node.is_nullable = false;
        
        TTypeDesc bool_type;
        TTypeNode bool_type_node;
        bool_type_node.type = TTypeNodeType::SCALAR;
        bool_type_node.__isset.scalar_type = true;
        bool_type_node.scalar_type.type = TPrimitiveType::BOOLEAN;
        bool_type.types.push_back(bool_type_node);
        false_node.type = bool_type;
        
        TBoolLiteral bool_literal;
        bool_literal.value = false;
        false_node.bool_literal = bool_literal;
        false_node.__isset.bool_literal = true;
        
        nodes.push_back(false_node);
        return expr;
    }
    
    if (row_id_range.size() == 1) {
        // Single range: slot_id >= begin AND slot_id < end
        return create_between_expr(slot_id, row_id_range[0].begin(), row_id_range[0].end()); // Reuse existing function for single range
    }
    
    // Multiple ranges: create OR expression
    TExprNode or_node;
    or_node.node_type = TExprNodeType::COMPOUND_PRED;
    or_node.opcode = TExprOpcode::COMPOUND_OR;
    or_node.__isset.opcode = true;
    or_node.num_children = row_id_range.size();
    or_node.is_nullable = true;
    
    TTypeDesc bool_type;
    TTypeNode bool_type_node;
    bool_type_node.type = TTypeNodeType::SCALAR;
    bool_type_node.__isset.scalar_type = true;
    bool_type_node.scalar_type.type = TPrimitiveType::BOOLEAN;
    bool_type.types.push_back(bool_type_node);
    or_node.type = bool_type;
    
    nodes.push_back(or_node);
    
    // Create AND expression for each range
    for (size_t i = 0; i < row_id_range.size(); i++) {
        const auto& range = row_id_range[i];
        
        // AND node for this range
        TExprNode and_node;
        and_node.node_type = TExprNodeType::COMPOUND_PRED;
        and_node.opcode = TExprOpcode::COMPOUND_AND;
        and_node.__isset.opcode = true;
        and_node.num_children = 2;
        and_node.is_nullable = true;
        and_node.type = bool_type;
        nodes.push_back(and_node);
        
        // GE node: slot_id >= range.begin
        TExprNode ge_node;
        ge_node.node_type = TExprNodeType::BINARY_PRED;
        ge_node.opcode = TExprOpcode::GE;
        ge_node.__isset.opcode = true;
        ge_node.num_children = 2;
        ge_node.is_nullable = true;
        ge_node.child_type = TPrimitiveType::BIGINT;
        ge_node.__isset.child_type = true;
        ge_node.type = bool_type;
        nodes.push_back(ge_node);
        
        // SlotRef for GE
        TExprNode slot_ref_ge;
        slot_ref_ge.node_type = TExprNodeType::SLOT_REF;
        slot_ref_ge.num_children = 0;
        slot_ref_ge.is_nullable = true;
        
        TTypeDesc bigint_type;
        TTypeNode bigint_type_node;
        bigint_type_node.type = TTypeNodeType::SCALAR;
        bigint_type_node.__isset.scalar_type = true;
        bigint_type_node.scalar_type.type = TPrimitiveType::BIGINT;
        bigint_type.types.push_back(bigint_type_node);
        slot_ref_ge.type = bigint_type;
        
        TSlotRef slot_ref_info_ge;
        slot_ref_info_ge.slot_id = slot_id;
        slot_ref_info_ge.tuple_id = 0;
        slot_ref_ge.slot_ref = slot_ref_info_ge;
        slot_ref_ge.__isset.slot_ref = true;
        nodes.push_back(slot_ref_ge);
        
        // Literal for range.begin
        TExprNode literal_begin;
        literal_begin.node_type = TExprNodeType::INT_LITERAL;
        literal_begin.num_children = 0;
        literal_begin.is_nullable = false;
        literal_begin.type = bigint_type;
        
        TIntLiteral int_literal_begin;
        int_literal_begin.value = range.begin();
        literal_begin.int_literal = int_literal_begin;
        literal_begin.__isset.int_literal = true;
        nodes.push_back(literal_begin);
        
        // LT node: slot_id < range.end
        TExprNode lt_node;
        lt_node.node_type = TExprNodeType::BINARY_PRED;
        lt_node.opcode = TExprOpcode::LT;
        lt_node.__isset.opcode = true;
        lt_node.num_children = 2;
        lt_node.is_nullable = true;
        lt_node.child_type = TPrimitiveType::BIGINT;
        lt_node.__isset.child_type = true;
        lt_node.type = bool_type;
        nodes.push_back(lt_node);
        
        // SlotRef for LT
        TExprNode slot_ref_lt;
        slot_ref_lt.node_type = TExprNodeType::SLOT_REF;
        slot_ref_lt.num_children = 0;
        slot_ref_lt.is_nullable = true;
        slot_ref_lt.type = bigint_type;
        
        TSlotRef slot_ref_info_lt;
        slot_ref_info_lt.slot_id = slot_id;
        slot_ref_info_lt.tuple_id = 0;
        slot_ref_lt.slot_ref = slot_ref_info_lt;
        slot_ref_lt.__isset.slot_ref = true;
        nodes.push_back(slot_ref_lt);
        
        // Literal for range.end
        TExprNode literal_end;
        literal_end.node_type = TExprNodeType::INT_LITERAL;
        literal_end.num_children = 0;
        literal_end.is_nullable = false;
        literal_end.type = bigint_type;
        
        TIntLiteral int_literal_end;
        int_literal_end.value = range.end();
        literal_end.int_literal = int_literal_end;
        literal_end.__isset.int_literal = true;
        nodes.push_back(literal_end);
    }
    
    return expr;
}


StatusOr<ChunkPtr> IcebergV3LookUpTask::_get_data_from_storage(RuntimeState* state, const std::vector<SlotDescriptor*>& slots,
    const phmap::flat_hash_map<int32_t, std::shared_ptr<SparseRange<int64_t>>>& row_id_ranges) {
    // @TODO
    SCOPED_TIMER(_ctx->parent->_get_data_from_storage_timer);
    ChunkPtr result_chunk;
    for (const auto& [scan_range_id, row_id_range] : row_id_ranges) {

        ObjectPool obj_pool;
        // @TODO should know _row_id slot id
        // TExpr expr = create_between_expr(_ctx->lookup_ref_slot_ids[0], 0);
        TExpr expr = create_row_id_filter_expr(_ctx->lookup_ref_slot_ids[1], *row_id_range);
        DLOG(INFO) << "create row_id filter expr: " << apache::thrift::ThriftDebugString(expr);

        ExprContext* expr_ctx = nullptr;
        RETURN_IF_ERROR(Expr::create_expr_tree(&obj_pool, expr, &expr_ctx, state, false));
        std::vector<ExprContext*> conjunct_ctxs{expr_ctx};

        RETURN_IF_ERROR(Expr::prepare(conjunct_ctxs, state));
        RETURN_IF_ERROR(Expr::open(conjunct_ctxs, state));

        // create an or preidcate, _row_id >= a and _row_id <= b or xxx
        // build hive data source

        // @TODO should know row_source_slot_id
        auto glm_ctx = down_cast<pipeline::IcebergGlobalLateMaterilizationContext*>(
            state->query_ctx()->global_late_materialization_ctx_mgr()->get_ctx(_ctx->row_source_slot_id));
        auto hdfs_scan_node = glm_ctx->hdfs_scan_node;
        hdfs_scan_node.tuple_id = _ctx->request_tuple_id;
        {
            [[maybe_unused]] std::ostringstream oss;
            oss << "request tuple id: "<< _ctx->request_tuple_id << ", slots: [";
            for (const auto& slot : state->desc_tbl().get_tuple_descriptor(_ctx->request_tuple_id)->slots()) {
                oss << slot->id() << ", ";
            }
            oss << "]";
            DLOG(INFO) << oss.str();
        }
        // @TODO add a _row_id slot into request tuple...

        auto provider = std::make_unique<connector::HiveDataSourceProvider>(nullptr, hdfs_scan_node);
        const auto& scan_range = glm_ctx->get_hdfs_scan_range(scan_range_id);
        auto data_source = std::make_shared<connector::HiveDataSource>(provider.get(), scan_range);
        // RuntimeProfile mock_profile("mock");
        // data_source->set_runtime_profile(&mock_profile);
        data_source->set_runtime_profile(_ctx->profile);
        data_source->set_predicates(conjunct_ctxs);
        // @TODO we should set a specific predicate to filter by row_id
        // @TODO set row_id
        
        RETURN_IF_ERROR(data_source->open(state));
        
        // 读取数据
        // ChunkPtr result_chunk = std::make_shared<Chunk>();
        // @TODO create a chunk that contain all columns
        do {
            ChunkPtr chunk = std::make_shared<Chunk>();
            auto status = data_source->get_next(state, &chunk);
            DLOG(INFO) << "get next chunk, status: " << status.to_string();
            if (status.is_end_of_file()) {
                DLOG(INFO) << "get end of file, break";
                break;
            }
            RETURN_IF_ERROR(status);
            if (chunk->num_rows() == 0) {
                DLOG(INFO) << "get empty chunk, break";
                break;
            }
            DLOG(INFO) << "get chunk: " << chunk->debug_columns();
            // result_chunk->append(*chunk);
            // @TODO result chunk
            if (result_chunk == nullptr) {
                result_chunk = std::make_shared<Chunk>();
                for (const auto& [slot_id, idx] : chunk->get_slot_id_to_index_map()) {
                    if (slot_id == _ctx->lookup_ref_slot_ids[0] || slot_id == _ctx->lookup_ref_slot_ids[1]) {
                        continue;
                    }
                    auto src_col = chunk->get_column_by_index(idx);
                    DLOG(INFO) << "append column: " << slot_id << ", " << src_col->get_name();
                    result_chunk->append_column(std::move(src_col), slot_id);
                }
            } else {
                for (const auto& [slot_id, idx] : chunk->get_slot_id_to_index_map()) {
                    if (slot_id == _ctx->lookup_ref_slot_ids[0] || slot_id == _ctx->lookup_ref_slot_ids[1]) {
                        continue;
                    }
                    auto src_col = chunk->get_column_by_index(idx);
                    auto dst_col = result_chunk->get_column_by_slot_id(slot_id);
                    dst_col->append(*src_col, 0, chunk->num_rows());
                    DLOG(INFO) << "append column: " << slot_id << ", " << dst_col->get_name();
                }
            }
            // @TODO 
        } while (true);
        data_source->close(state);
    }
    return result_chunk;
}

Status IcebergV3LookUpTask::process(RuntimeState* state, const ChunkPtr& request_chunk) {
    // 获取请求上下文中的信息
    DLOG(INFO) << "IcebergV3LookUpTask process, request_ctxs size: " << _ctx->request_ctxs.size();
    if (_ctx->request_ctxs.empty()) {
        return Status::OK();
    }
    // @TODO calculate row_id range
    // @TODO fetch data

    // create rowid expr 

    phmap::flat_hash_map<int32_t, std::shared_ptr<SparseRange<int64_t>>> row_id_ranges;
    Buffer<uint32_t> replicated_offsets;
    // @TODO request_chunk use fetch_ref_slots, we should change it to lookup_ref_slots
    ASSIGN_OR_RETURN(auto sorted_chunk, _calculate_row_id_range(state, request_chunk, &row_id_ranges, &replicated_offsets));
    // LOG(INFO) << "IcebergV3LookUpTask calculate row_id_range: " << row_id_ranges.to_string();
    ASSIGN_OR_RETURN(auto result_chunk, _get_data_from_storage(state, {}, row_id_ranges));
    {
        auto unordered_position_column = sorted_chunk->get_column_by_slot_id(Chunk::SORT_ORDINAL_COLUMN_SLOT_ID);
        if (!replicated_offsets.empty()) {
            // if there are duplicated rows, we need to replicate the data
            for (const auto& [slot_id, _] : result_chunk->get_slot_id_to_index_map()) {
                auto old_column = result_chunk->get_column_by_slot_id(slot_id);
                ASSIGN_OR_RETURN(auto new_column, old_column->replicate(replicated_offsets));
                result_chunk->append_or_update_column(std::move(new_column), slot_id);
            }
            result_chunk->check_or_die();
        }
        result_chunk->append_column(unordered_position_column, Chunk::SORT_ORDINAL_COLUMN_SLOT_ID);
        result_chunk->check_or_die();
        ASSIGN_OR_RETURN(auto sorted_result_chunk, _sort_chunk(state, result_chunk, {unordered_position_column}));
        result_chunk = sorted_result_chunk;
    }
    // @TODO resort and replicate
    // @TODO fill response
    DLOG(INFO) << "IcebergV3LookUpTask fill response, result_chunk: " << result_chunk->debug_columns();

    // insersection
    auto tuple_desc = state->desc_tbl().get_tuple_descriptor(_ctx->request_tuple_id);
    std::vector<SlotDescriptor*> slots;
    // @TODO this slot may contains row_id, we don't need it
    {
        [[maybe_unused]] std::ostringstream oss;
        oss << "IcebergV3LookUpTask fill response, slots: ";
        for (const auto& slot : tuple_desc->slots()) {
            if (slot->id() == _ctx->lookup_ref_slot_ids[0] || slot->id() == _ctx->lookup_ref_slot_ids[1]) {
                continue;
            }
            slots.emplace_back(slot);
            oss << slot->id() << ", ";
        }
        DLOG(INFO) << oss.str();
    }
    {
        SCOPED_TIMER(_ctx->parent->_fill_response_timer);
        size_t start_offset = 0;
        for (const auto& request_ctx : _ctx->request_ctxs) {
            ASSIGN_OR_RETURN(auto num_rows, request_ctx->fill_response(result_chunk, 0, slots, start_offset));
            start_offset += num_rows;
        }
    }

    // for (const auto& request_ctx : _ctx->request_ctxs) {
    //     auto status = request_ctx->fill_response(result_chunk, _ctx->source_id_slot, _ctx->slots, 0);
    //     RETURN_IF_ERROR(status);
    // }

    
    return Status::OK();
}

}