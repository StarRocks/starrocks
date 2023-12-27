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

#include "exprs/dict_query_expr.h"

#include "agent/master_info.h"
#include "column/chunk.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exec/tablet_info.h"
#include "gutil/casts.h"
#include "runtime/client_cache.h"
#include "storage/chunk_helper.h"
#include "storage/table_reader.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

DictQueryExpr::DictQueryExpr(const TExprNode& node) : Expr(node), _dict_query_expr(node.dict_query_expr) {}

StatusOr<ColumnPtr> DictQueryExpr::evaluate_checked(ExprContext* context, Chunk* ptr) {
    Columns columns(children().size());
    size_t size = ptr != nullptr ? ptr->num_rows() : 1;
    for (int i = 0; i < _children.size(); ++i) {
        columns[i] = _children[i]->evaluate(context, ptr);
    }

    ColumnPtr res;
    for (auto& column : columns) {
        if (column->is_constant()) {
            column = ColumnHelper::unpack_and_duplicate_const_column(size, column);
        }
    }
    ChunkPtr key_chunk = ChunkHelper::new_chunk(_key_schema, size);
    key_chunk->reset();
    for (int i = 0; i < _dict_query_expr.key_fields.size(); ++i) {
        ColumnPtr key_column = columns[1 + i];
        key_chunk->update_column_by_index(key_column, i);
    }
    for (size_t i = 0; i < key_chunk->num_columns(); ++i) {
        key_chunk->set_slot_id_to_index(_key_slot_ids[i], i);
    }

    for (auto& column : key_chunk->columns()) {
        if (column->is_nullable()) {
            column = ColumnHelper::update_column_nullable(false, column, column->size());
        }
    }

    std::vector<bool> found;
    ChunkPtr value_chunk = ChunkHelper::new_chunk(_value_schema, key_chunk->num_rows());
    value_chunk->set_slot_id_to_index(_value_slot_id, 0);

    Status status = _table_reader->multi_get(*key_chunk, {_dict_query_expr.value_field}, found, *value_chunk);
    if (!status.ok()) {
        // todo retry
        LOG(WARNING) << "fail to execute multi get: " << status.detailed_message();
        return status;
    }
    res = value_chunk->get_column_by_index(0)->clone_empty();
    if (!res->is_nullable()) {
        auto null_column = UInt8Column::create(0, 0);
        res = NullableColumn::create(res, null_column);
    }

    int res_idx = 0;
    for (int idx = 0; idx < size; ++idx) {
        if (found[idx]) {
            res->append_datum(value_chunk->get_column_by_index(0)->get(res_idx));
            res_idx++;
        } else {
            if (_dict_query_expr.strict_mode) {
                return Status::NotFound("In strict mode, query failed if record not exist in dict table.");
            }
            res->append_nulls(1);
        }
    }

    return res;
}

Status DictQueryExpr::prepare(RuntimeState* state, ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));
    _runtime_state = state;
    return Status::OK();
}

Status DictQueryExpr::open(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) {
    // init parent open
    RETURN_IF_ERROR(Expr::open(state, context, scope));

    TGetDictQueryParamRequest request;
    request.__set_db_name(_dict_query_expr.db_name);
    request.__set_table_name(_dict_query_expr.tbl_name);
    TGetDictQueryParamResponse response;

    TNetworkAddress master_addr = get_master_address();
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &response](FrontendServiceConnection& client) { client->getDictQueryParam(response, request); },
            30000));

    TableReaderParams params;
    params.schema = response.schema;
    params.partition_param = response.partition;
    params.location_param = response.location;
    params.nodes_info = response.nodes_info;
    params.partition_versions = _dict_query_expr.partition_version;
    params.timeout_ms = 30000;

    _table_reader = std::make_shared<TableReader>();
    RETURN_IF_ERROR(_table_reader->init(params));

    auto schema_param = std::make_shared<OlapTableSchemaParam>();
    RETURN_IF_ERROR(schema_param->init(params.schema));
    const auto& tcolumns = schema_param->indexes()[0]->column_param->columns;
    for (int i = 0; i < _dict_query_expr.key_fields.size(); ++i) {
        for (const auto& tcolumn : tcolumns) {
            if (tcolumn->name() == _dict_query_expr.key_fields[i]) {
                auto f = std::make_shared<Field>(ChunkHelper::convert_field(i, *tcolumn));
                _key_schema.append(f);
            }
        }
    }

    for (const auto& tcolumn : tcolumns) {
        if (tcolumn->name() == _dict_query_expr.value_field) {
            auto f = std::make_shared<Field>(ChunkHelper::convert_field(0, *tcolumn));
            _value_schema.append(f);
            break; /* only single column */
        }
    }

    for (size_t i = 0; i < params.schema.slot_descs.size(); ++i) {
        const auto& slot = params.schema.slot_descs[i];
        for (const auto& field : _key_schema.fields()) {
            if (field->name() == slot.colName) {
                _key_slot_ids.emplace_back(slot.id);
                break;
            }
        }
        if (_dict_query_expr.value_field == slot.colName) {
            _value_slot_id = slot.id;
        }
    }
    DCHECK(_key_slot_ids.size() == _key_schema.fields().size());

    return Status::OK();
}

void DictQueryExpr::close(RuntimeState* state, ExprContext* context, FunctionContext::FunctionStateScope scope) {
    Expr::close(state, context, scope);
}

} // namespace starrocks
