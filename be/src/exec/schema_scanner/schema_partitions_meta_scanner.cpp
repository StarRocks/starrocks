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

#include "exec/schema_scanner/schema_partitions_meta_scanner.h"

#include <fmt/format.h>

#include "common/logging.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaPartitionsMetaScanner::_s_columns[] = {
        //   name,       type,          size,     is_null
        {"DB_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"PARTITION_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"PARTITION_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"COMPACT_VERSION", TYPE_BIGINT, sizeof(int64_t), false},
        {"VISIBLE_VERSION", TYPE_BIGINT, sizeof(int64_t), false},
        {"VISIBLE_VERSION_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"NEXT_VERSION", TYPE_BIGINT, sizeof(int64_t), false},
        {"PARTITION_KEY", TYPE_VARCHAR, sizeof(StringValue), false},
        {"PARTITION_VALUE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DISTRIBUTION_KEY", TYPE_VARCHAR, sizeof(StringValue), false},
        {"BUCKETS", TYPE_INT, sizeof(int), false},
        {"REPLICATION_NUM", TYPE_INT, sizeof(int), false},
        {"STORAGE_MEDIUM", TYPE_VARCHAR, sizeof(StringValue), false},
        {"COOLDOWN_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"LAST_CONSISTENCY_CHECK_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"IS_IN_MEMORY", TYPE_BOOLEAN, sizeof(bool), false},
        {"IS_TEMP", TYPE_BOOLEAN, sizeof(bool), false},
        {"DATA_SIZE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ROW_COUNT", TYPE_BIGINT, sizeof(int64_t), false},
        {"ENABLE_DATACACHE", TYPE_BOOLEAN, sizeof(bool), false},
        {"AVG_CS", TYPE_DOUBLE, sizeof(double), false},
        {"P50_CS", TYPE_DOUBLE, sizeof(double), false},
        {"MAX_CS", TYPE_DOUBLE, sizeof(double), false},
        {"STORAGE_PATH", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaPartitionsMetaScanner::SchemaPartitionsMetaScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

SchemaPartitionsMetaScanner::~SchemaPartitionsMetaScanner() = default;

Status SchemaPartitionsMetaScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    TAuthInfo auth_info;
    if (nullptr != _param->db) {
        auth_info.__set_pattern(*(_param->db));
    }
    if (nullptr != _param->table) {
        auth_info.__set_pattern(*(_param->table));
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
    TGetPartitionsMetaRequest partitions_meta_req;
    partitions_meta_req.__set_auth_info(auth_info);

    if (nullptr != _param->ip && 0 != _param->port) {
        int timeout_ms = state->query_options().query_timeout * 1000;
        RETURN_IF_ERROR(SchemaHelper::get_partitions_meta(*(_param->ip), _param->port, partitions_meta_req,
                                                          &_partitions_meta_response, timeout_ms));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }
    _ctz = state->timezone_obj();
    _partitions_meta_index = 0;
    return Status::OK();
}

Status SchemaPartitionsMetaScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    if (_partitions_meta_index >= _partitions_meta_response.partitions_meta_infos.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return fill_chunk(chunk);
}

Status SchemaPartitionsMetaScanner::fill_chunk(ChunkPtr* chunk) {
    const TPartitionMetaInfo& info = _partitions_meta_response.partitions_meta_infos[_partitions_meta_index];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        if (slot_id < 1 || slot_id > 25) {
            return Status::InternalError(fmt::format("invalid slot id:{}", slot_id));
        }
        ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);

        switch (slot_id) {
        case 1: {
            // DB_NAME
            Slice db_name = Slice(info.db_name);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&db_name);
            break;
        }
        case 2: {
            // TABLE_NAME
            Slice table_name = Slice(info.table_name);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&table_name);
            break;
        }
        case 3: {
            // PARTITION_NAME
            Slice partition_name = Slice(info.partition_name);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&partition_name);
            break;
        }
        case 4: {
            // PARTITION_ID
            fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.partition_id);
            break;
        }
        case 5: {
            // COMPACT_VERSION
            fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.compact_version);
            break;
        }
        case 6: {
            // VISIBLE_VERSION
            fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.visible_version);
            break;
        }
        case 7: {
            // VISIBLE_VERSION_TIME
            if (info.visible_version_time > 0) {
                DateTimeValue ts;
                ts.from_unixtime(info.visible_version_time, _ctz);
                fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&ts);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 8: {
            // NEXT_VERSION
            fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.next_version);
            break;
        }
        case 9: {
            // PARTITION_KEY
            Slice partition_key = Slice(info.partition_key);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&partition_key);
            break;
        }
        case 10: {
            // PARTITION_VALUE
            Slice partition_value = Slice(info.partition_value);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&partition_value);
            break;
        }
        case 11: {
            // DISTRIBUTION_KEY
            Slice distribution_key = Slice(info.distribution_key);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&distribution_key);
            break;
        }
        case 12: {
            // BUCKETS
            fill_column_with_slot<TYPE_INT>(column.get(), (void*)&info.buckets);
            break;
        }
        case 13: {
            // REPLICATION_NUM
            fill_column_with_slot<TYPE_INT>(column.get(), (void*)&info.replication_num);
            break;
        }
        case 14: {
            // STORAGE_MEDIUM
            Slice storage_medium = Slice(info.storage_medium);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&storage_medium);
            break;
        }
        case 15: {
            // COOLDOWN_TIME
            if (info.cooldown_time > 0) {
                DateTimeValue ts;
                ts.from_unixtime(info.cooldown_time, _ctz);
                fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&ts);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 16: {
            // LAST_CONSISTENCY_CHECK_TIME
            if (info.last_consistency_check_time > 0) {
                DateTimeValue ts;
                ts.from_unixtime(info.last_consistency_check_time, _ctz);
                fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&ts);
            } else {
                fill_data_column_with_null(column.get());
            }
            break;
        }
        case 17: {
            // IS_IN_MEMORY
            fill_column_with_slot<TYPE_BOOLEAN>(column.get(), (void*)&info.is_in_memory);
            break;
        }
        case 18: {
            // IS_TEMP
            fill_column_with_slot<TYPE_BOOLEAN>(column.get(), (void*)&info.is_temp);
            break;
        }
        case 19: {
            // DATA_SIZE
            Slice data_size = Slice(info.data_size);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&data_size);
            break;
        }
        case 20: {
            // ROW_COUNT
            fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.row_count);
            break;
        }
        case 21: {
            // ENABLE_DATACACHE
            fill_column_with_slot<TYPE_BOOLEAN>(column.get(), (void*)&info.enable_datacache);
            break;
        }
        case 22: {
            // AVG_CS
            fill_column_with_slot<TYPE_DOUBLE>(column.get(), (void*)&info.avg_cs);
            break;
        }
        case 23: {
            // P50_CS
            fill_column_with_slot<TYPE_DOUBLE>(column.get(), (void*)&info.p50_cs);
            break;
        }
        case 24: {
            // MAX_CS
            fill_column_with_slot<TYPE_DOUBLE>(column.get(), (void*)&info.max_cs);
            break;
        }
        case 25: {
            // STORAGE_PATH
            Slice storage_path = Slice(info.storage_path);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&storage_path);
            break;
        }
        default:
            break;
        }
    }
    _partitions_meta_index++;
    return Status::OK();
}

} // namespace starrocks
