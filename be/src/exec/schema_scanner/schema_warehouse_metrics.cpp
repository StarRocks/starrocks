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

#include "exec/schema_scanner/schema_warehouse_metrics.h"

#include <fmt/format.h>

#include "common/logging.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc WarehouseMetricsScanner::_s_columns[] = {
        //   name,       type,          size,     is_null
        {"WAREHOUSE_ID", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"WAREHOUSE_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"QUEUE_PENDING_LENGTH", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"QUEUE_RUNNING_LENGTH", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"MAX_PENDING_LENGTH", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"MAX_PENDING_TIME_SECOND", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"EARLIEST_QUERY_WAIT_TIME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"MAX_REQUIRED_SLOTS", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"SUM_REQUIRED_SLOTS", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"REMAIN_SLOTS", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"MAX_SLOTS", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
        {"EXTRA_MESSAGE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), false},
};

WarehouseMetricsScanner::WarehouseMetricsScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

Status WarehouseMetricsScanner::start(RuntimeState* state) {
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
    TGetWarehouseMetricsRequest req;
    req.__set_auth_info(auth_info);
    RETURN_IF_ERROR(SchemaHelper::get_warehouse_metrics(_ss_state, req, &_response));
    return Status::OK();
}

Status WarehouseMetricsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    if (_idx >= _response.metrics.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return fill_chunk(chunk);
}

Status WarehouseMetricsScanner::fill_chunk(ChunkPtr* chunk) {
    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    const TGetWarehouseMetricsResponeItem& item = _response.metrics[_idx++];
    DatumArray datum_array{Slice(item.warehouse_id),
                           Slice(item.warehouse_name),
                           Slice(item.queue_pending_length),
                           Slice(item.queue_running_length),
                           Slice(item.max_pending_length),
                           Slice(item.max_pending_time_second),
                           Slice(item.earliest_query_wait_time),
                           Slice(item.max_required_slots),
                           Slice(item.sum_required_slots),
                           Slice(item.remain_slots),
                           Slice(item.max_slots),
                           Slice(item.extra_message)};
    for (const auto& [slot_id, index] : slot_id_map) {
        auto column = (*chunk)->get_mutable_column_by_slot_id(slot_id).get();
        column->append_datum(datum_array[slot_id - 1]);
    }
    return {};
}
} // namespace starrocks