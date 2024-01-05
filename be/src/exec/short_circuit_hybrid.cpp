// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "exec/short_circuit_hybrid.h"

#include "column/column_helper.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/scan_node.h"
#include "exprs/expr.h"
#include "runtime/exec_env.h"
#include "runtime/memory_scratch_sink.h"
#include "storage/chunk_helper.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"
#include "util/thrift_util.h"

namespace starrocks {
Status ShortCircuitHybridScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    return Status::OK();
}

Status ShortCircuitHybridScanNode::open(RuntimeState* state) {
    _t_desc_tbl = &_common_request.desc_tbl;
    _key_literal_exprs = &_common_request.key_literal_exprs;
    _versions.swap(_common_request.versions);

    // get tablet
    for (auto tablet_id : _common_request.tablet_ids) {
        auto tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id);
        if (tablet == nullptr) {
            return Status::NotFound(fmt::format("tablet {} not exist", tablet_id));
        }
        _tablets.emplace_back(std::move(tablet));
    }

    SCOPED_TIMER(_runtime_profile->total_time_counter());

    _num_rows = _key_literal_exprs->size();
    //init tuple
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    DCHECK(_tuple_desc != nullptr);

    // skips runtime filters in ScanNode::open
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));
    return Status::OK();
}

Status ShortCircuitHybridScanNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    if (*eos) {
        return Status::OK();
    }
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    std::vector<bool> found(_num_rows, false);
    Buffer<uint8_t> selections;

    RETURN_IF_ERROR(_process_key_chunk());
    RETURN_IF_ERROR(_process_value_chunk(found));
    size_t result_size = 0;
    for (int i = 0; i < found.size(); ++i) {
        if (found[i]) {
            result_size++;
            selections.emplace_back(1);
        } else {
            selections.emplace_back(0);
        }
    }

    auto tablet_schema = _tablets[0]->tablet_schema()->schema();
    auto column_ids = tablet_schema->field_column_ids();
    auto tablet_schema_without_rowstore = std::make_unique<Schema>(tablet_schema, column_ids);
    auto result_chunk = ChunkHelper::new_chunk(*_tuple_desc, result_size);

    //idx is column id, value is slot id
    if (result_size > 0) {
        _key_chunk->filter(selections);
        for (auto slot_desc : _tuple_desc->slots()) {
            auto field = tablet_schema_without_rowstore->get_field_by_name(slot_desc->col_name());
            if (field->is_key()) {
                result_chunk->get_column_by_slot_id(slot_desc->id())
                        ->append(*(_key_chunk->get_column_by_name(field->name().data()).get()));
            }
        }

        for (auto slot_desc : _tuple_desc->slots()) {
            auto field = tablet_schema_without_rowstore->get_field_by_name(slot_desc->col_name());
            if (!field->is_key()) {
                result_chunk->get_column_by_slot_id(slot_desc->id())
                        ->append(*(_value_chunk->get_column_by_name(field->name().data()).get()));
            }
        }
        RETURN_IF_ERROR(ExecNode::eval_conjuncts(_conjunct_ctxs, result_chunk.get()));
    }
    *eos = true;
    *chunk = std::move(result_chunk);
    return Status::OK();
}

Status ShortCircuitHybridScanNode::_process_key_chunk() {
    DCHECK(_tablets.size() > 0);
    _tablet_schema = _tablets[0]->tablet_schema();
    auto& key_column_cids = _tablet_schema->sort_key_idxes();
    auto key_schema = ChunkHelper::convert_schema(_tablet_schema, key_column_cids);

    _key_chunk = ChunkHelper::new_chunk(key_schema, _num_rows);
    _key_chunk->reset();

    for (int i = 0; i < _num_rows; ++i) {
        // TODO (jkj) if expr is k1=1 and k2 in (3, 4), we need bind tablet with expr,
        // tablet 1  <---> k1 =1, k2 =3
        // tablet 2  <---> k1 =1, k2 =4
        // this prune need happen in fe
        auto keys_literal_expr = (*_key_literal_exprs)[i].literal_exprs;
        size_t num_pk_filters = keys_literal_expr.size();
        // must all columns
        if (UNLIKELY(num_pk_filters != _tablet_schema->num_key_columns())) {
            return Status::Corruption("short circuit only support all key predicate");
        }
        for (int j = 0; j < num_pk_filters; ++j) {
            // init expr context
            std::vector<ExprContext*> expr_ctxs;
            std::vector<TExpr> key_literal_expr{keys_literal_expr[j]};
            // prepare
            RETURN_IF_ERROR(Expr::create_expr_trees(runtime_state()->obj_pool(), key_literal_expr, &expr_ctxs,
                                                    runtime_state()));
            RETURN_IF_ERROR(Expr::prepare(expr_ctxs, runtime_state()));
            RETURN_IF_ERROR(Expr::open(expr_ctxs, runtime_state()));
            auto& iteral_expr_ctx = expr_ctxs[0];
            ASSIGN_OR_RETURN(ColumnPtr value, iteral_expr_ctx->root()->evaluate_const(iteral_expr_ctx));
            // add const column to chunk
            auto const_column = ColumnHelper::get_data_column(value.get());
            _key_chunk->get_column_by_index(j)->append(*const_column);
        }
    }

    return Status::OK();
}

// params: std::vector<int>& found
// found vector value
Status ShortCircuitHybridScanNode::_process_value_chunk(std::vector<bool>& found) {
    std::vector<string> value_field_names;
    vector<starrocks::ColumnId> value_column_ids;
    for (auto slot_desc : _tuple_desc->slots()) {
        auto field = _tablet_schema->schema()->get_field_by_name(slot_desc->col_name());
        if (field != nullptr && !field->is_key()) {
            value_field_names.emplace_back(field->name());
            value_column_ids.emplace_back(field->id());
        }
    }
    auto value_schema = std::make_unique<Schema>(_tablet_schema->schema(), value_column_ids);
    // tmp value_chunk, order not match key_chunk
    ChunkPtr value_chunk = ChunkHelper::new_chunk(*(value_schema), _num_rows);
    // final value_chunk, order match key_chunk
    _value_chunk = ChunkHelper::new_chunk(*(value_schema), _num_rows);

    std::vector<int> key_idx_to_value_idx(_num_rows, -1);
    int value_chunk_idx = 0;

    for (int i = 0; i < _tablets.size(); ++i) {
        LocalTableReaderParams params;
        params.version = std::stoi(_versions[i]);
        params.tablet_id = _tablets[i]->get_tablet_info().tablet_id;
        _table_reader = std::make_shared<TableReader>();
        RETURN_IF_ERROR(_table_reader->init(params));

        auto current_chunk = ChunkHelper::new_chunk(*(value_schema), _num_rows);
        // current tablet will return all key_chunk mapping whether has value
        // true , means vector idx of key_chunk have value
        std::vector<bool> curent_found;
        Status status =
                _table_reader->multi_get(*(_key_chunk.get()), value_field_names, curent_found, *(current_chunk.get()));
        if (!status.ok()) {
            // todo retry
            LOG(WARNING) << "fail to execute multi get: " << status.detailed_message();
        }

        // merge all tablet result
        bool has_found_value = false;
        for (int key_idx = 0; key_idx < curent_found.size(); ++key_idx) {
            // 1 tablet will have many value hit predicate, so here need foreach end
            if (curent_found[key_idx] && key_idx_to_value_idx[key_idx] == -1) {
                // make sure found order is same between key_chunk and value_chunk
                key_idx_to_value_idx[key_idx] = value_chunk_idx;
                value_chunk_idx++;
                if (UNLIKELY(found[key_idx])) {
                    return Status::Corruption(
                            fmt::format("one key can't be found twice in short circuit, tablet_id: {}, key_idx: {}",
                                        params.tablet_id, key_idx));
                }
                found[key_idx] = true;
                has_found_value = true;
            }
        }
        if (has_found_value) {
            value_chunk->append(*(current_chunk.get()));
        }
    }

    // transform  value
    for (int key_idx = 0; key_idx < key_idx_to_value_idx.size(); ++key_idx) {
        if (key_idx_to_value_idx[key_idx] != -1) {
            _value_chunk->append(*(value_chunk.get()), key_idx_to_value_idx[key_idx], 1);
        }
    }

    return Status::OK();
}

} // namespace starrocks
