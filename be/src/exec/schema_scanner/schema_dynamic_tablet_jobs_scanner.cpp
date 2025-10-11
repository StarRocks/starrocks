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

#include "exec/schema_scanner/schema_dynamic_tablet_jobs_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "gen_cpp/FrontendService_types.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaDynamicTabletJobsScanner::_s_columns[] = {
        {"JOB_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(long), true},
        {"DB_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(long), true},
        {"DB_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"TABLE_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(long), true},
        {"TABLE_NAME", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"JOB_TYPE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"JOB_STATE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
        {"TRANSACTION_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(long), true},
        {"PARALLEL_PARTITIONS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(long), true},
        {"PARALLEL_TABLETS", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(long), true},
        {"CREATED_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"FINISHED_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"ERROR_MESSAGE", TypeDescriptor::create_varchar_type(sizeof(Slice)), sizeof(Slice), true},
};

SchemaDynamicTabletJobsScanner::SchemaDynamicTabletJobsScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaDynamicTabletJobsScanner::~SchemaDynamicTabletJobsScanner() = default;

Status SchemaDynamicTabletJobsScanner::start(RuntimeState* state) {
    RETURN_IF(!_is_init, Status::InternalError("used before initialized."));
    RETURN_IF(!_param->ip || !_param->port, Status::InternalError("IP or port not exists"));

    RETURN_IF_ERROR(SchemaScanner::start(state));
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));

    TDynamicTabletJobsRequest request;
    return SchemaHelper::get_dynamic_tablet_jobs_info(_ss_state, request, &_result);
}

Status SchemaDynamicTabletJobsScanner::_fill_chunk(ChunkPtr* chunk) {
    if (_result.status.status_code != TStatusCode::OK) {
        return Status::InternalError(
                fmt::format("get dynamic tablet jobs infos error: {}", _result.status.error_msgs[0]));
    }

    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    const TDynamicTabletJobsItem& info = _result.items[_index];
    DatumArray datum_array{
            info.job_id,
            info.db_id,
            Slice(info.db_name),
            info.table_id,
            Slice(info.table_name),

            Slice(info.job_type),
            Slice(info.job_state),
            info.transaction_id,
            info.parallel_partitions,
            info.parallel_tablets,

            TimestampValue::create_from_unixtime(info.created_time, _runtime_state->timezone_obj()),

            info.finished_time > 0
                    ? TimestampValue::create_from_unixtime(info.finished_time, _runtime_state->timezone_obj())
                    : kNullDatum,

            Slice(info.error_message),
    };

    for (const auto& [slot_id, index] : slot_id_map) {
        auto column = (*chunk)->get_mutable_column_by_slot_id(slot_id).get();
        column->append_datum(datum_array[slot_id - 1]);
    }
    _index++;
    return {};
}

Status SchemaDynamicTabletJobsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    RETURN_IF(!_is_init, Status::InternalError("Used before initialized."));
    RETURN_IF((nullptr == chunk || nullptr == eos), Status::InternalError("input pointer is nullptr."));

    if (_index >= _result.items.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return _fill_chunk(chunk);
}

} // namespace starrocks