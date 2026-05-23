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

#include <algorithm>
#include <boost/algorithm/string.hpp>

#include "cctz/civil_time.h"
#include "cctz/time_zone.h"
#include "exec/schema_scanner/schema_helper.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/literal.h"
#include "http/http_client.h"
#include "runtime/runtime_state.h"

namespace starrocks {

namespace {
// Bound for a datetime range predicate extracted from BE conjuncts. Carries
// both the legacy wall-clock string (for old FEs that still parse strings) and
// the unambiguous UTC epoch ms (for new FEs that compare epochs directly).
struct DateTimeRangeBound {
    std::string str_value;
    int64_t epoch_ms = 0;
    bool has_value = false;
};

bool _extract_literal_datetime_value(Expr* expr, const cctz::time_zone& session_tz, bool is_lower_bound,
                                     DateTimeRangeBound& result) {
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
    const auto ts = datum.get_timestamp();
    result.str_value = ts.to_string(true);
    int year, month, day, hour, minute, second, usec;
    ts.to_timestamp(&year, &month, &day, &hour, &minute, &second, &usec);
    // See literal_to_epoch_ms for the DST contract; the sub-second part of
    // the literal is carried through to FE so the prefilter matches BE's
    // ms-precision materialized column.
    result.epoch_ms = SchemaLoadsScanner::literal_to_epoch_ms(
                             session_tz, cctz::civil_second(year, month, day, hour, minute, second), is_lower_bound) +
                      usec / 1000;

    return true;
}

bool _try_parse_datetime_range_predicate(const SchemaScannerParam* param, Expr* conjunct, const std::string& col_name,
                                         const cctz::time_zone& session_tz, DateTimeRangeBound& bound,
                                         bool& is_lower_bound) {
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

    if (!_extract_literal_datetime_value(value_child, session_tz, is_lower_bound, bound)) {
        return false;
    }
    bound.has_value = true;

    return true;
}

void _update_range_bound(DateTimeRangeBound& bound, const DateTimeRangeBound& candidate, bool is_lower_bound) {
    if (!bound.has_value) {
        bound = candidate;
        return;
    }

    // Lower bound: keep the largest candidate (tightest >= literal).
    // Upper bound: keep the smallest candidate (tightest <= literal).
    if ((is_lower_bound && candidate.epoch_ms > bound.epoch_ms) ||
        (!is_lower_bound && candidate.epoch_ms < bound.epoch_ms)) {
        bound = candidate;
    }
}

// Collect datetime range predicates on one column from schema scan conjuncts.
// Usage:
// 1. Pass a target column name, e.g. "LOAD_START_TIME".
// 2. This function scans conjuncts and picks predicates with operators in {>, >=, <, <=}.
// 3. It normalizes them into [lower_bound, upper_bound] (both inclusive for FE-side filtering).
// Purpose: convert BE-side scan predicates into FE RPC parameters to reduce rows returned by getLoads().
//
// session_tz is used to compute the epoch-ms representation of each literal; the
// wall-clock string is preserved for FEs that still parse the string field.
void _parse_datetime_range_predicates(const SchemaScannerParam* param, const std::string& col_name,
                                      const cctz::time_zone& session_tz, DateTimeRangeBound& lower,
                                      DateTimeRangeBound& upper) {
    if (param->expr_contexts == nullptr) {
        return;
    }

    for (auto* expr_context : *(param->expr_contexts)) {
        DateTimeRangeBound candidate;
        bool is_lower_bound = false;
        if (!_try_parse_datetime_range_predicate(param, expr_context->root(), col_name, session_tz, candidate,
                                                 is_lower_bound)) {
            continue;
        }

        if (is_lower_bound) {
            _update_range_bound(lower, candidate, true);
        } else {
            _update_range_bound(upper, candidate, false);
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

int64_t SchemaLoadsScanner::literal_to_epoch_ms(const cctz::time_zone& session_tz, cctz::civil_second cs,
                                                bool is_lower_bound) {
    // DST makes the civil-to-absolute mapping non-unique. cctz returns two
    // candidates named by which transition offset was applied, NOT by temporal
    // order:
    //   - REPEATED (fall-back, e.g. America/New_York 2026-11-01 01:30):
    //       lookup.pre is the earlier instant, lookup.post the later.
    //   - SKIPPED  (spring-forward gap, e.g. America/New_York 2026-03-08 02:30):
    //       senses SWAPPED - lookup.pre applies the pre-transition offset and
    //       is the LATER instant, lookup.post the EARLIER. For UNIQUE civil
    //       times pre == post.
    // BE re-evaluates the predicate after materializing the column in
    // session_tz, so this prefilter must be no-false-negative against the
    // post-filter. Picking min/max(pre, post) widens for both DST cases.
    //
    // Returns second-aligned ms; the caller adds the sub-second remainder
    // from the literal.
    const auto lookup = session_tz.lookup(cs);
    const auto tp = is_lower_bound ? std::min(lookup.pre, lookup.post) : std::max(lookup.pre, lookup.post);
    return tp.time_since_epoch().count() * 1000;
}

bool SchemaLoadsScanner::_fill_datetime_column_from_ms(Column* column, bool is_set, int64_t epoch_ms) const {
    if (!is_set) {
        return false;
    }
    if (epoch_ms <= 0) {
        // Defensive: FE writers leave the field unset for unknown timestamps,
        // so an explicit sentinel here only happens on malformed input.
        down_cast<NullableColumn*>(column)->append_nulls(1);
        return true;
    }
    DateTimeValue t;
    t.from_unixtime(epoch_ms / 1000, (epoch_ms % 1000) * 1000, _runtime_state->timezone_obj());
    fill_column_with_slot<TYPE_DATETIME>(column, (void*)&t);
    return true;
}

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

    // BE evaluates the SQL literal in the session timezone (a wall-clock value
    // with no zone marker). The new *_ms fields carry the unambiguous UTC epoch
    // ms derived from that wall-clock via the session zone. The legacy *_from /
    // *_to string fields are still populated for old FEs.
    const auto& session_tz = state->timezone_obj();

    DateTimeRangeBound load_start_time_from;
    DateTimeRangeBound load_start_time_to;
    _parse_datetime_range_predicates(_param, "LOAD_START_TIME", session_tz, load_start_time_from, load_start_time_to);
    if (load_start_time_from.has_value) {
        load_params.__set_load_start_time_from(load_start_time_from.str_value);
        load_params.__set_load_start_time_from_ms(load_start_time_from.epoch_ms);
    }
    if (load_start_time_to.has_value) {
        load_params.__set_load_start_time_to(load_start_time_to.str_value);
        load_params.__set_load_start_time_to_ms(load_start_time_to.epoch_ms);
    }

    DateTimeRangeBound load_finish_time_from;
    DateTimeRangeBound load_finish_time_to;
    _parse_datetime_range_predicates(_param, "LOAD_FINISH_TIME", session_tz, load_finish_time_from,
                                     load_finish_time_to);
    if (load_finish_time_from.has_value) {
        load_params.__set_load_finish_time_from(load_finish_time_from.str_value);
        load_params.__set_load_finish_time_from_ms(load_finish_time_from.epoch_ms);
    }
    if (load_finish_time_to.has_value) {
        load_params.__set_load_finish_time_to(load_finish_time_to.str_value);
        load_params.__set_load_finish_time_to_ms(load_finish_time_to.epoch_ms);
    }

    DateTimeRangeBound create_time_from;
    DateTimeRangeBound create_time_to;
    _parse_datetime_range_predicates(_param, "CREATE_TIME", session_tz, create_time_from, create_time_to);
    if (create_time_from.has_value) {
        load_params.__set_create_time_from(create_time_from.str_value);
        load_params.__set_create_time_from_ms(create_time_from.epoch_ms);
    }
    if (create_time_to.has_value) {
        load_params.__set_create_time_to(create_time_to.str_value);
        load_params.__set_create_time_to_ms(create_time_to.epoch_ms);
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
                if (_fill_datetime_column_from_ms(column, info.__isset.create_time_ms, info.create_time_ms)) {
                    break;
                }
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
                if (_fill_datetime_column_from_ms(column, info.__isset.load_start_time_ms, info.load_start_time_ms)) {
                    break;
                }
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
                if (_fill_datetime_column_from_ms(column, info.__isset.load_commit_time_ms, info.load_commit_time_ms)) {
                    break;
                }
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
                if (_fill_datetime_column_from_ms(column, info.__isset.load_finish_time_ms, info.load_finish_time_ms)) {
                    break;
                }
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
