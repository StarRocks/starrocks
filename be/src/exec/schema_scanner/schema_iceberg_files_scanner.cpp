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

#include "exec/schema_scanner/schema_iceberg_files_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaIcebergFilesScanner::_iceberg_files_columns[] = {
        //   name,       type,          size,     is_null
        {"content", TYPE_INT, sizeof(int32_t), false},
        {"file_path", TYPE_VARCHAR, sizeof(StringValue), false},
        {"file_format", TYPE_VARCHAR, sizeof(StringValue), false},
        {"spec_id", TYPE_INT, sizeof(int32_t), false},
        {"record_count", TYPE_BIGINT, sizeof(int64_t), false},
        {"file_size_in_bytes", TYPE_BIGINT, sizeof(int64_t), false},
        {"column_sizes", TYPE_VARCHAR, sizeof(StringValue), true},
        {"value_counts", TYPE_VARCHAR, sizeof(StringValue), true},
        {"null_value_counts", TYPE_VARCHAR, sizeof(StringValue), true},
        {"nan_value_counts", TYPE_VARCHAR, sizeof(StringValue), true},
        {"lower_bounds", TYPE_VARCHAR, sizeof(StringValue), true},
        {"upper_bounds", TYPE_VARCHAR, sizeof(StringValue), true},
        {"split_offsets", TYPE_VARCHAR, sizeof(StringValue), true},
        {"equality_ids", TYPE_VARCHAR, sizeof(StringValue), true}};

SchemaIcebergFilesScanner::SchemaIcebergFilesScanner()
        : SchemaScanner(_iceberg_files_columns, sizeof(_iceberg_files_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaIcebergFilesScanner::~SchemaIcebergFilesScanner() = default;

Status SchemaIcebergFilesScanner::start(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanner::start(state));
    TGetIcebergFilesRequest request;
    if (nullptr != _param->catalog) {
        request.__set_catalog_name(*(_param->catalog));
    }
    if (nullptr != _param->origin_db) {
        request.__set_db_name(*(_param->origin_db));
    }
    if (nullptr != _param->origin_table) {
        request.__set_table_name(*(_param->origin_table));
    }
    request.__set_limit(_param->limit);
    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_iceberg_files(*(_param->ip), _param->port, request, &_files_res));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    _files_index = 0;
    return Status::OK();
}

Status SchemaIcebergFilesScanner::fill_chunk(ChunkPtr* chunk) {
    const TIcebergFile& file = _files_res.iceberg_files[_files_index];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);
        switch (slot_id) {
        case 1: {
            // content
            { fill_column_with_slot<TYPE_INT>(column.get(), (void*)&file.content); }
            break;
        }
        case 2: {
            // file_path
            {
                const std::string* str = &file.file_path;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 3: {
            // file_format
            {
                const std::string* str = &file.file_format;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 4: {
            // spec_id
            { fill_column_with_slot<TYPE_INT>(column.get(), (void*)&file.spec_id); }
            break;
        }
        case 5: {
            // record_count
            { fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&file.record_count); }
            break;
        }
        case 6: {
            // file_size_in_bytes
            { fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&file.file_size_in_bytes); }
            break;
        }
        case 7: {
            // column_sizes
            {
                const std::string* str = &file.column_sizes;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 8: {
            // value_counts
            {
                const std::string* str = &file.value_counts;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 9: {
            // null_value_counts
            {
                const std::string* str = &file.null_value_counts;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 10: {
            // nan_value_counts
            {
                const std::string* str = &file.nan_value_counts;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 11: {
            // lower_bounds
            {
                const std::string* str = &file.lower_bounds;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 12: {
            // upper_bounds
            {
                const std::string* str = &file.upper_bounds;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 13: {
            // split_offsets
            {
                const std::string* str = &file.split_offsets;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 14: {
            // equality_ids
            {
                const std::string* str = &file.equality_ids;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        default:
            break;
        }
    }
    _files_index++;
    return Status::OK();
}

Status SchemaIcebergFilesScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (_files_index >= _files_res.iceberg_files.size()) {
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
