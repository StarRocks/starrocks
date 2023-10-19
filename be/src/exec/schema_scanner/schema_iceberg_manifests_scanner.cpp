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

#include "exec/schema_scanner/schema_iceberg_manifests_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaIcebergManifestsScanner::_iceberg_manifests_columns[] = {
        //   name,       type,          size,     is_null
        {"path", TYPE_VARCHAR, sizeof(StringValue), false},
        {"length", TYPE_BIGINT, sizeof(int64_t), false},
        {"partition_spec_id", TYPE_INT, sizeof(int32_t), false},
        {"added_snapshot_id", TYPE_BIGINT, sizeof(int64_t), false},
        {"added_data_files_count", TYPE_INT, sizeof(int32_t), false},
        {"added_rows_count", TYPE_BIGINT, sizeof(int64_t), false},
        {"existing_data_files_count", TYPE_INT, sizeof(int32_t), false},
        {"existing_rows_count", TYPE_BIGINT, sizeof(int64_t), false},
        {"deleted_data_files_count", TYPE_INT, sizeof(int32_t), false},
        {"deleted_rows_count", TYPE_BIGINT, sizeof(int64_t), false},
        {"partition_summaries", TYPE_VARCHAR, sizeof(StringValue), true}};

SchemaIcebergManifestsScanner::SchemaIcebergManifestsScanner()
        : SchemaScanner(_iceberg_manifests_columns,
                        sizeof(_iceberg_manifests_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaIcebergManifestsScanner::~SchemaIcebergManifestsScanner() = default;

Status SchemaIcebergManifestsScanner::start(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanner::start(state));
    TGetIcebergManifestsRequest request;
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
        RETURN_IF_ERROR(SchemaHelper::get_iceberg_manifests(*(_param->ip), _param->port, request, &_manifests_res));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    _manifest_index = 0;
    return Status::OK();
}

Status SchemaIcebergManifestsScanner::fill_chunk(ChunkPtr* chunk) {
    const TIcebergManifest& manifest = _manifests_res.iceberg_manifests[_manifest_index];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);
        switch (slot_id) {
        case 1: {
            // path
            {
                const std::string* str = &manifest.path;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 2: {
            // length
            { fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&manifest.length); }
            break;
        }
        case 3: {
            // partition_spec_id
            { fill_column_with_slot<TYPE_INT>(column.get(), (void*)&manifest.partition_spec_id); }
            break;
        }
        case 4: {
            // added_snapshot_id
            { fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&manifest.added_snapshot_id); }
            break;
        }
        case 5: {
            // added_data_files_count
            { fill_column_with_slot<TYPE_INT>(column.get(), (void*)&manifest.added_data_files_count); }
            break;
        }
        case 6: {
            // added_rows_count
            { fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&manifest.added_rows_count); }
            break;
        }
        case 7: {
            // existing_data_files_count
            { fill_column_with_slot<TYPE_INT>(column.get(), (void*)&manifest.existing_data_files_count); }
            break;
        }
        case 8: {
            // existing_rows_count
            { fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&manifest.existing_rows_count); }
            break;
        }
        case 9: {
            // deleted_data_files_count
            { fill_column_with_slot<TYPE_INT>(column.get(), (void*)&manifest.deleted_data_files_count); }
            break;
        }
        case 10: {
            // deleted_rows_count
            { fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&manifest.deleted_rows_count); }
            break;
        }
        case 11: {
            // partition_summaries
            {
                const std::string* str = &manifest.partition_summaries;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        default:
            break;
        }
    }
    _manifest_index++;
    return Status::OK();
}

Status SchemaIcebergManifestsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (_manifest_index >= _manifests_res.iceberg_manifests.size()) {
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
