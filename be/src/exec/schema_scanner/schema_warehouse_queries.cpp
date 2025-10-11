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

#include "exec/schema_scanner/schema_warehouse_queries.h"

#include <fmt/format.h>

#include "common/logging.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc WarehouseQueriesScanner::_s_columns[] = {
        //   name,       type,          size,     is_null
        {"WAREHOUSE_ID", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"WAREHOUSE_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"QUERY_ID", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"STATE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"EST_COSTS_SLOTS", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"ALLOCATE_SLOTS", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"QUEUED_WAIT_SECONDS", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"QUERY", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"QUERY_START_TIME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"QUERY_END_TIME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"QUERY_DURATION", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"EXTRA_MESSAGE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
};

WarehouseQueriesScanner::WarehouseQueriesScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

Status WarehouseQueriesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    TAuthInfo auth_info;
    if (nullptr != _param->db) {
        auth_info.__set_pattern(*(_param->db));
    }
    if (nullptr != _param->current_user_ident) {
        auth_info.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (nullptr != _param->user) {
            auth_info.__set_user(*(_param->user));
        }
        if (nullptr != _param->user_ip) {
            auth_info.__set_user_ip(*(_param->user_ip));
        }
    }
    // init schema scanner state
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    TGetWarehouseQueriesRequest req;
    req.__set_auth_info(auth_info);
    RETURN_IF_ERROR(SchemaHelper::get_warehouse_queries(_ss_state, req, &_response));
    return Status::OK();
}

Status WarehouseQueriesScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    if (_idx >= _response.queries.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return fill_chunk(chunk);
}

Status WarehouseQueriesScanner::fill_chunk(ChunkPtr* chunk) {
    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    const TGetWarehouseQueriesResponseItem& item = _response.queries[_idx++];
    DatumArray datum_array{Slice(item.warehouse_id),
                           Slice(item.warehouse_name),
                           Slice(item.query_id),
                           Slice(item.state),
                           Slice(item.est_costs_slots),
                           Slice(item.allocate_slots),
                           Slice(item.queued_wait_seconds),
                           Slice(item.query),
                           Slice(item.query_start_time),
                           Slice(item.query_end_time),
                           Slice(item.query_duration),
                           Slice(item.extra_message)};
    for (const auto& [slot_id, index] : slot_id_map) {
        auto column = (*chunk)->get_mutable_column_by_slot_id(slot_id).get();
        column->append_datum(datum_array[slot_id - 1]);
    }
    return {};
}
} // namespace starrocks