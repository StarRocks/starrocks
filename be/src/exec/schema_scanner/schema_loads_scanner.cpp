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

#include "exec/schema_scanner/schema_loads_scanner.h"

#include <boost/algorithm/string.hpp>

#include "exec/schema_scanner/schema_helper.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/literal.h"
#include "http/http_client.h"
#include "runtime/runtime_state.h"

namespace starrocks {

namespace {
bool _extract_literal_datetime_value(Expr* expr, std::string& result) {
    auto* literal = dynamic_cast<VectorizedLiteral*>(expr);
    if (literal == nullptr) {
        return false;
    }

    auto literal_col_status = literal->evaluate_checked(nullptr, nullptr);
    if (!literal_col_status.ok()) {
        return false;
    }

    const auto literal_col = literal_col_status.value();
    if (literal_col->is_null(0)) {
        return false;
    }

    const auto datum = literal_col->get(0);
    result = datum.get_timestamp().to_string(true);

    return true;
}

bool _try_parse_datetime_range_predicate(const SchemaScannerParam* param, Expr* conjunct, const std::string& col_name,
                                         std::string& literal_value, bool& is_lower_bound) {
    const TExprNodeType::type& node_type = conjunct->node_type();
    const TExprOpcode::type& op_type = conjunct->op();
    if (node_type != TExprNodeType::BINARY_PRED || (op_type != TExprOpcode::GE && op_type != TExprOpcode::GT &&
                                                    op_type != TExprOpcode::LE && op_type != TExprOpcode::LT)) {
        return false;
    }

    Expr* slot_child = conjunct->get_child(0);
    Expr* value_child = conjunct->get_child(1);
    if (value_child->node_type() == TExprNodeType::SLOT_REF) {
        std::swap(slot_child, value_child);
        is_lower_bound = (op_type == TExprOpcode::LE || op_type == TExprOpcode::LT);
    } else {
        is_lower_bound = (op_type == TExprOpcode::GE || op_type == TExprOpcode::GT);
    }

    if (slot_child->node_type() != TExprNodeType::SLOT_REF) {
        return false;
    }

    const SlotId slot_id = down_cast<const ColumnRef*>(slot_child)->slot_id();
    auto& slot_id_mapping = param->slot_id_mapping;
    if (const auto it = slot_id_mapping.find(slot_id);
        it == slot_id_mapping.end() || !boost::iequals(it->second->col_name(), col_name)) {
        return false;
    }

    if (!_extract_literal_datetime_value(value_child, literal_value)) {
        return false;
    }

    return true;
}

void _update_range_bound(std::string& bound, bool& has_bound, const std::string& candidate, bool is_lower_bound) {
    if (!has_bound) {
        bound = candidate;
        has_bound = true;
        return;
    }

    if ((is_lower_bound && candidate > bound) || (!is_lower_bound && candidate < bound)) {
        bound = candidate;
    }
}

// Collect datetime range predicates on one column from schema scan conjuncts.
// Usage:
// 1. Pass a target column name, e.g. "LOAD_START_TIME".
// 2. This function scans conjuncts and picks predicates with operators in {>, >=, <, <=}.
// 3. It normalizes them into [lower_bound, upper_bound] (both inclusive for FE-side filtering).
// Purpose: convert BE-side scan predicates into FE RPC parameters to reduce rows returned by getLoads().
void _parse_datetime_range_predicates(const SchemaScannerParam* param, const std::string& col_name,
                                      std::string& lower_bound, bool& has_lower_bound, std::string& upper_bound,
                                      bool& has_upper_bound) {
    if (param->expr_contexts == nullptr) {
        return;
    }

    for (auto* expr_context : *(param->expr_contexts)) {
        std::string literal_value;
        bool is_lower_bound = false;
        if (!_try_parse_datetime_range_predicate(param, expr_context->root(), col_name, literal_value,
                                                 is_lower_bound)) {
            continue;
        }

        if (is_lower_bound) {
            _update_range_bound(lower_bound, has_lower_bound, literal_value, true);
        } else {
            _update_range_bound(upper_bound, has_upper_bound, literal_value, false);
        }
    }
}
} // namespace

SchemaScanner::ColumnDesc SchemaLoadsScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"LABEL", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"PROFILE_ID", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"DB_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"TABLE_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"USER", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"WAREHOUSE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"STATE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"PROGRESS", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"TYPE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"PRIORITY", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"SCAN_ROWS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"SCAN_BYTES", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"FILTERED_ROWS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"UNSELECTED_ROWS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"SINK_ROWS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"RUNTIME_DETAILS", TypeDescriptor::from_logical_type(TYPE_JSON), kJsonDefaultSize, true},
        {"CREATE_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"LOAD_START_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"LOAD_COMMIT_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"LOAD_FINISH_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"PROPERTIES", TypeDescriptor::from_logical_type(TYPE_JSON), kJsonDefaultSize, true},
        {"ERROR_MSG", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"TRACKING_SQL", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"REJECTED_RECORD_PATH", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"JOB_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false}};

SchemaLoadsScanner::SchemaLoadsScanner()
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaLoadsScanner::~SchemaLoadsScanner() = default;

Status SchemaLoadsScanner::start(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanner::start(state));
    TGetLoadsParams load_params;
    if (nullptr != _param->db) {
        load_params.__set_db(*(_param->db));
    } else if (std::string db_name; _parse_expr_predicate("DB_NAME", db_name)) {
        load_params.__set_db(db_name);
    }

    if (std::string table_name; _parse_expr_predicate("TABLE_NAME", table_name)) {
        load_params.__set_table_name(table_name);
    }
    if (std::string user; _parse_expr_predicate("USER", user)) {
        load_params.__set_user(user);
    }
    if (std::string state_str; _parse_expr_predicate("STATE", state_str)) {
        load_params.__set_state(state_str);
    }

    std::string load_start_time_from;
    std::string load_start_time_to;
    bool has_load_start_time_from = false;
    bool has_load_start_time_to = false;
    _parse_datetime_range_predicates(_param, "LOAD_START_TIME", load_start_time_from, has_load_start_time_from,
                                     load_start_time_to, has_load_start_time_to);
    if (has_load_start_time_from) {
        load_params.__set_load_start_time_from(load_start_time_from);
    }
    if (has_load_start_time_to) {
        load_params.__set_load_start_time_to(load_start_time_to);
    }

    std::string load_finish_time_from;
    std::string load_finish_time_to;
    bool has_load_finish_time_from = false;
    bool has_load_finish_time_to = false;
    _parse_datetime_range_predicates(_param, "LOAD_FINISH_TIME", load_finish_time_from, has_load_finish_time_from,
                                     load_finish_time_to, has_load_finish_time_to);
    if (has_load_finish_time_from) {
        load_params.__set_load_finish_time_from(load_finish_time_from);
    }
    if (has_load_finish_time_to) {
        load_params.__set_load_finish_time_to(load_finish_time_to);
    }

    std::string create_time_from;
    std::string create_time_to;
    bool has_create_time_from = false;
    bool has_create_time_to = false;
    _parse_datetime_range_predicates(_param, "CREATE_TIME", create_time_from, has_create_time_from, create_time_to,
                                     has_create_time_to);
    if (has_create_time_from) {
        load_params.__set_create_time_from(create_time_from);
    }
    if (has_create_time_to) {
        load_params.__set_create_time_to(create_time_to);
    }

    if (nullptr != _param->label) {
        load_params.__set_label(*(_param->label));
    }
    if (_param->job_id != -1) {
        load_params.__set_job_id(_param->job_id);
    }

    // init schema scanner state
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    RETURN_IF_ERROR(SchemaHelper::get_loads(_ss_state, load_params, &_result));
    _cur_idx = 0;
    return Status::OK();
}

Status SchemaLoadsScanner::fill_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (; _cur_idx < _result.loads.size(); _cur_idx++) {
        auto& info = _result.loads[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 26) {
                return Status::InternalError(fmt::format("invalid slot id:{}", slot_id));
            }
            auto* column = (*chunk)->get_column_raw_ptr_by_slot_id(slot_id);
            switch (slot_id) {
            case 1: {
                // id
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.job_id);
                break;
            }
            case 2: {
                // label
                Slice label = Slice(info.label);
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&label);
                break;
            }
            case 3: {
                // profile_id
                if (info.__isset.profile_id) {
                    Slice profile_id = Slice(info.profile_id);
                    fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&profile_id);
                } else {
                    down_cast<NullableColumn*>(column)->append_nulls(1);
                }
                break;
            }
            case 4: {
                // database
                Slice db = Slice(info.db);
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&db);
                break;
            }
            case 5: {
                // table
                Slice table = Slice(info.table);
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&table);
                break;
            }
            case 6: {
                // user
                Slice user = Slice(info.user);
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&user);
                break;
            }
            case 7: {
                // warehouse
                if (info.__isset.warehouse) {
                    Slice warehouse = Slice(info.warehouse);
                    fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&warehouse);
                } else {
                    down_cast<NullableColumn*>(column)->append_nulls(1);
                }
                break;
            }
            case 8: {
                // state
                Slice state = Slice(info.state);
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&state);
                break;
            }
            case 9: {
                // progress
                Slice progress = Slice(info.progress);
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&progress);
                break;
            }
            case 10: {
                // type
                Slice type = Slice(info.type);
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&type);
                break;
            }
            case 11: {
                // priority
                Slice priority = Slice(info.priority);
                fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&priority);
                break;
            }
            case 12: {
                // scan_rows
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.num_scan_rows);
                break;
            }
            case 13: {
                // scan_bytes
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.num_scan_bytes);
                break;
            }
            case 14: {
                // filtered_rows
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.num_filtered_rows);
                break;
            }
            case 15: {
                // unselected_rows
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.num_unselected_rows);
                break;
            }
            case 16: {
                // sink_rows
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.num_sink_rows);
                break;
            }
            case 17: {
                // runtime details
                Slice runtime_details = Slice(info.runtime_details);
                JsonValue json_value;
                JsonValue* json_value_ptr = &json_value;
                Status s = JsonValue::parse(runtime_details, &json_value);
                if (!s.ok()) {
                    LOG(WARNING) << "parse runtime details failed. runtime details:" << runtime_details.to_string()
                                 << " error:" << s;
                    down_cast<NullableColumn*>(column)->append_nulls(1);
                } else {
                    fill_column_with_slot<TYPE_JSON>(column, (void*)&json_value_ptr);
                }
                break;
            }
            case 18: {
                // create time
                DateTimeValue t;
                if (info.__isset.create_time) {
                    if (t.from_date_str(info.create_time.data(), info.create_time.size())) {
                        fill_column_with_slot<TYPE_DATETIME>(column, (void*)&t);
                        break;
                    }
                }
                down_cast<NullableColumn*>(column)->append_nulls(1);
                break;
            }
            case 19: {
                // load start time
                DateTimeValue t;
                if (info.__isset.load_start_time) {
                    if (t.from_date_str(info.load_start_time.data(), info.load_start_time.size())) {
                        fill_column_with_slot<TYPE_DATETIME>(column, (void*)&t);
                        break;
                    }
                }
                down_cast<NullableColumn*>(column)->append_nulls(1);
                break;
            }
            case 20: {
                // load commit time
                DateTimeValue t;
                if (info.__isset.load_commit_time) {
                    if (t.from_date_str(info.load_commit_time.data(), info.load_commit_time.size())) {
                        fill_column_with_slot<TYPE_DATETIME>(column, (void*)&t);
                        break;
                    }
                }
                down_cast<NullableColumn*>(column)->append_nulls(1);
                break;
            }
            case 21: {
                // load finish time
                DateTimeValue t;
                if (info.__isset.load_finish_time) {
                    if (t.from_date_str(info.load_finish_time.data(), info.load_finish_time.size())) {
                        fill_column_with_slot<TYPE_DATETIME>(column, (void*)&t);
                        break;
                    }
                }
                down_cast<NullableColumn*>(column)->append_nulls(1);
                break;
            }
            case 22: {
                // properties
                Slice properties = Slice(info.properties);
                JsonValue json_value;
                JsonValue* json_value_ptr = &json_value;
                Status s = JsonValue::parse(properties, &json_value);
                if (!s.ok()) {
                    LOG(WARNING) << "parse properties failed. properties:" << properties.to_string() << " error:" << s;
                    down_cast<NullableColumn*>(column)->append_nulls(1);
                } else {
                    fill_column_with_slot<TYPE_JSON>(column, (void*)&json_value_ptr);
                }
                break;
            }
            case 23: {
                // error_msg
                if (info.__isset.error_msg) {
                    Slice error_msg = Slice(info.error_msg);
                    fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&error_msg);
                } else {
                    down_cast<NullableColumn*>(column)->append_nulls(1);
                }
                break;
            }
            case 24: {
                // tracking sql
                if (info.__isset.tracking_sql) {
                    Slice sql = Slice(info.tracking_sql);
                    fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&sql);
                } else {
                    down_cast<NullableColumn*>(column)->append_nulls(1);
                }
                break;
            }
            case 25: {
                // rejected record path
                if (info.__isset.rejected_record_path) {
                    Slice path = Slice(info.rejected_record_path);
                    fill_column_with_slot<TYPE_VARCHAR>(column, (void*)&path);
                } else {
                    down_cast<NullableColumn*>(column)->append_nulls(1);
                }
                break;
            }
            case 26: {
                // job id
                fill_column_with_slot<TYPE_BIGINT>(column, (void*)&info.job_id);
                break;
            }
            default:
                break;
            }
        }
    }
    return Status::OK();
}

Status SchemaLoadsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_cur_idx >= _result.loads.size()) {
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
