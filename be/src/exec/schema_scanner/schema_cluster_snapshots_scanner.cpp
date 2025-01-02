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

#include "exec/schema_scanner/schema_cluster_snapshots_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "gen_cpp/FrontendService_types.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaClusterSnapshotsScanner::_s_columns[] = {
        {"SNAPSHOT_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"SNAPSHOT_TYPE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"CREATED_TIME", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(long), true},
        {"FINISHED_TIME", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(long), true},
        {"FE_JOURNAL_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(long), true},
        {"STARMGR_JOURNAL_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(long), true},
        {"PROPERTIES", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"STORAGE_VOLUME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
        {"STORAGE_PATH", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), true},
};

SchemaClusterSnapshotsScanner::SchemaClusterSnapshotsScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaClusterSnapshotsScanner::~SchemaClusterSnapshotsScanner() = default;

Status SchemaClusterSnapshotsScanner::start(RuntimeState* state) {
    RETURN_IF(!_is_init, Status::InternalError("used before initialized."));
    RETURN_IF(!_param->ip || !_param->port, Status::InternalError("IP or port not exists"));

    RETURN_IF_ERROR(SchemaScanner::start(state));
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));

    TClusterSnapshotsRequest request;
    return SchemaHelper::get_cluster_snapshots_info(_ss_state, request, &_result);
}

Status SchemaClusterSnapshotsScanner::_fill_chunk(ChunkPtr* chunk) {
    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    const TClusterSnapshotsItem& info = _result.items[_index];
    DatumArray datum_array{
            Slice(info.snapshot_name), Slice(info.snapshot_type), info.created_time,      info.finished_time,

            info.fe_jouranl_id,        info.starmgr_jouranl_id,   Slice(info.properties), Slice(info.storage_volume),

            Slice(info.storage_path),
    };
    for (const auto& [slot_id, index] : slot_id_map) {
        Column* column = (*chunk)->get_column_by_slot_id(slot_id).get();
        column->append_datum(datum_array[slot_id - 1]);
    }
    _index++;
    return {};
}

Status SchemaClusterSnapshotsScanner::get_next(ChunkPtr* chunk, bool* eos) {
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