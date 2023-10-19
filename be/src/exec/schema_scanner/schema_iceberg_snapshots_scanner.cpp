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

#include "exec/schema_scanner/schema_iceberg_snapshots_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaIcebergSnapshotsScanner::_iceberg_snapshots_columns[] = {
        //   name,       type,          size,     is_null
        {"committed_at", TYPE_VARCHAR, sizeof(StringValue), false},
        {"snapshot_id", TYPE_BIGINT, sizeof(int64_t), false},
        {"parent_id", TYPE_BIGINT, sizeof(int64_t), true},
        {"operation", TYPE_VARCHAR, sizeof(StringValue), true},
        {"manifest_list", TYPE_VARCHAR, sizeof(StringValue), true},
        {"summary", TYPE_VARCHAR, sizeof(StringValue), true}};

SchemaIcebergSnapshotsScanner::SchemaIcebergSnapshotsScanner()
        : SchemaScanner(_iceberg_snapshots_columns,
                        sizeof(_iceberg_snapshots_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaIcebergSnapshotsScanner::~SchemaIcebergSnapshotsScanner() = default;

Status SchemaIcebergSnapshotsScanner::start(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanner::start(state));
    TGetIcebergSnapshotRequest request;
    if (nullptr != _param->catalog) {
        request.__set_catalog_name(*(_param->catalog));
    }
    if (nullptr != _param->origin_db) {
        request.__set_db_name(*(_param->origin_db));
    }
    if (nullptr != _param->origin_table) {
        request.__set_table_name(*(_param->origin_table));
    }
    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_iceberg_snapshots(*(_param->ip), _param->port, request, &_snapshots_res));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    _snapshot_index = 0;
    return Status::OK();
}

Status SchemaIcebergSnapshotsScanner::fill_chunk(ChunkPtr* chunk) {
    const TIcebergSnapshot& snapshot = _snapshots_res.iceberg_snapshots[_snapshot_index];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);
        switch (slot_id) {
        case 1: {
            // committed_at
            {
                const std::string* str = &snapshot.committed_at;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 2: {
            // snapshot_id
            { fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&snapshot.snapshot_id); }
            break;
        }
        case 3: {
            // parent_id
            { fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&snapshot.parent_id); }
            break;
        }
        case 4: {
            // operation
            {
                const std::string* operation = &snapshot.operation;
                Slice value(operation->c_str(), operation->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 5: {
            // manifest_list
            {
                const std::string* manifest_list = &snapshot.manifest_list;
                Slice value(manifest_list->c_str(), manifest_list->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 6: {
            // summary
            {
                const std::string* summary = &snapshot.summary;
                Slice value(summary->c_str(), summary->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        default:
            break;
        }
    }
    _snapshot_index++;
    return Status::OK();
}

Status SchemaIcebergSnapshotsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (_snapshot_index >= _snapshots_res.iceberg_snapshots.size()) {
        *eos = true;
        return Status::OK();
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    *eos = false;
    return fill_chunk(chunk);
}

} // namespace starrocks
