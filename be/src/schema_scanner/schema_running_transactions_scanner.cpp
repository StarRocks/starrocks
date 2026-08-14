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

#include "schema_scanner/schema_running_transactions_scanner.h"

#include "column/nullable_column.h"
#include "runtime/runtime_state.h"
#include "schema_scanner/schema_helper.h"
#include "types/datetime_value.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaRunningTransactionsScanner::_s_tbls_columns[] = {
        //   name,                 type,                                                size,                  is_null
        {"TXN_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"GLOBAL_TXN_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"LABEL", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"DATABASE_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"DATABASE_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"TABLE_IDS", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"TABLE_NAMES", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"STATE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"COORDINATOR", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"SOURCE_TYPE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"WAREHOUSE_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"PREPARE_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"PREPARED_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"COMMIT_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"PUBLISH_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"FINISH_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"PENDING_PUBLISH_MS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"TIMEOUT_MS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"PREPARED_TIMEOUT_MS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"ERROR_REPLICA_NUM", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"REASON", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"ERROR_MSG", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"IS_NO_OP_PUBLISH", TypeDescriptor::from_logical_type(TYPE_BOOLEAN), sizeof(bool), false},
        {"NO_OP_PUBLISH_REASON", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true}};

SchemaRunningTransactionsScanner::SchemaRunningTransactionsScanner()
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaRunningTransactionsScanner::~SchemaRunningTransactionsScanner() = default;

Status SchemaRunningTransactionsScanner::start(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanner::start(state));
    TGetRunningTxnsParams txn_params;
    if (nullptr != _param->db) {
        txn_params.__set_db(*(_param->db));
    } else if (std::string db_name; _parse_expr_predicate("DATABASE_NAME", db_name)) {
        txn_params.__set_db(db_name);
    }
    if (nullptr != _param->label) {
        txn_params.__set_label(*(_param->label));
    } else if (std::string label; _parse_expr_predicate("LABEL", label)) {
        txn_params.__set_label(label);
    }
    // NOTE: no TXN_ID pushdown. _parse_expr_predicate only extracts SLOT_REF == STRING_LITERAL, and TXN_ID
    // is a BIGINT column, so a `WHERE TXN_ID = <n>` predicate never matches here; the BE applies it as a
    // residual filter on the returned chunk instead. (Only the db/label VARCHAR predicates are pushed down
    // through this helper.)

    // Forward the querying user so the leader FE can filter rows by database privilege.
    if (nullptr != _param->current_user_ident) {
        txn_params.__set_current_user_ident(*(_param->current_user_ident));
    }

    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    RETURN_IF_ERROR(SchemaHelper::get_running_transactions(_ss_state, txn_params, &_result));
    _cur_idx = 0;
    return Status::OK();
}

void SchemaRunningTransactionsScanner::_fill_datetime_column_from_ms(Column* column, bool is_set,
                                                                     int64_t epoch_ms) const {
    // These DATETIME columns are is_null=true, so the source chunk column is nullable and append_nulls is safe.
    if (!is_set || epoch_ms <= 0) {
        down_cast<NullableColumn*>(column)->append_nulls(1);
        return;
    }
    DateTimeValue t;
    t.from_unixtime(epoch_ms / 1000, (epoch_ms % 1000) * 1000, _runtime_state->timezone_obj());
    fill_column_with_slot<TYPE_DATETIME>(column, (void*)&t);
}

Status SchemaRunningTransactionsScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (; _cur_idx < _result.txns.size(); _cur_idx++) {
        auto& info = _result.txns[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 24) {
                return Status::InternalError(fmt::format("invalid slot id:{}", slot_id));
            }
            auto* column = (*chunk)->get_column_raw_ptr_by_slot_id(slot_id);
            switch (slot_id) {
            case 1: {
                // TXN_ID
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.txn_id);
                break;
            }
            case 2: {
                // GLOBAL_TXN_ID
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.global_txn_id);
                break;
            }
            case 3: {
                // LABEL (always set by the FE; is_null=false, so fill unconditionally like loads)
                Slice s = Slice(info.label);
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&s);
                break;
            }
            case 4: {
                // DATABASE_ID
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.database_id);
                break;
            }
            case 5: {
                // DATABASE_NAME
                if (info.__isset.database_name) {
                    Slice s = Slice(info.database_name);
                    fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&s);
                } else {
                    down_cast<NullableColumn*>(column)->append_nulls(1);
                }
                break;
            }
            case 6: {
                // TABLE_IDS
                if (info.__isset.table_ids) {
                    Slice s = Slice(info.table_ids);
                    fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&s);
                } else {
                    down_cast<NullableColumn*>(column)->append_nulls(1);
                }
                break;
            }
            case 7: {
                // TABLE_NAMES
                if (info.__isset.table_names) {
                    Slice s = Slice(info.table_names);
                    fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&s);
                } else {
                    down_cast<NullableColumn*>(column)->append_nulls(1);
                }
                break;
            }
            case 8: {
                // STATE (always set by the FE; is_null=false, so fill unconditionally like loads)
                Slice s = Slice(info.state);
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&s);
                break;
            }
            case 9: {
                // COORDINATOR
                if (info.__isset.coordinator) {
                    Slice s = Slice(info.coordinator);
                    fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&s);
                } else {
                    down_cast<NullableColumn*>(column)->append_nulls(1);
                }
                break;
            }
            case 10: {
                // SOURCE_TYPE
                if (info.__isset.source_type) {
                    Slice s = Slice(info.source_type);
                    fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&s);
                } else {
                    down_cast<NullableColumn*>(column)->append_nulls(1);
                }
                break;
            }
            case 11: {
                // WAREHOUSE_ID
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.warehouse_id);
                break;
            }
            case 12: {
                // PREPARE_TIME
                _fill_datetime_column_from_ms(column, info.__isset.prepare_time_ms, info.prepare_time_ms);
                break;
            }
            case 13: {
                // PREPARED_TIME
                _fill_datetime_column_from_ms(column, info.__isset.prepared_time_ms, info.prepared_time_ms);
                break;
            }
            case 14: {
                // COMMIT_TIME
                _fill_datetime_column_from_ms(column, info.__isset.commit_time_ms, info.commit_time_ms);
                break;
            }
            case 15: {
                // PUBLISH_TIME
                _fill_datetime_column_from_ms(column, info.__isset.publish_time_ms, info.publish_time_ms);
                break;
            }
            case 16: {
                // FINISH_TIME
                _fill_datetime_column_from_ms(column, info.__isset.finish_time_ms, info.finish_time_ms);
                break;
            }
            case 17: {
                // PENDING_PUBLISH_MS
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.pending_publish_ms);
                break;
            }
            case 18: {
                // TIMEOUT_MS
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.timeout_ms);
                break;
            }
            case 19: {
                // PREPARED_TIMEOUT_MS
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.prepared_timeout_ms);
                break;
            }
            case 20: {
                // ERROR_REPLICA_NUM
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.error_replica_num);
                break;
            }
            case 21: {
                // REASON
                if (info.__isset.reason) {
                    Slice s = Slice(info.reason);
                    fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&s);
                } else {
                    down_cast<NullableColumn*>(column)->append_nulls(1);
                }
                break;
            }
            case 22: {
                // ERROR_MSG
                if (info.__isset.error_msg) {
                    Slice s = Slice(info.error_msg);
                    fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&s);
                } else {
                    down_cast<NullableColumn*>(column)->append_nulls(1);
                }
                break;
            }
            case 23: {
                // IS_NO_OP_PUBLISH
                fill_column_with_slot<TYPE_BOOLEAN>(column, (void*)&info.is_no_op_publish);
                break;
            }
            case 24: {
                // NO_OP_PUBLISH_REASON
                if (info.__isset.no_op_publish_reason) {
                    Slice s = Slice(info.no_op_publish_reason);
                    fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&s);
                } else {
                    down_cast<NullableColumn*>(column)->append_nulls(1);
                }
                break;
            }
            default:
                break;
            }
        }
    }
    return Status::OK();
}

Status SchemaRunningTransactionsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_cur_idx >= _result.txns.size()) {
        *eos = true;
        return Status::OK();
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("invalid parameter.");
    }
    *eos = false;
    return fill_chunk(chunk);
}

} // namespace starrocks
