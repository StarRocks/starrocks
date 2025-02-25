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
        {"DB_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"TABLE_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"PARTITION_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"PARTITION_ID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"COMPACT_VERSION", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"VISIBLE_VERSION", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"VISIBLE_VERSION_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"NEXT_VERSION", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"DATA_VERSION", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"VERSION_EPOCH", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"VERSION_TXN_TYPE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"PARTITION_KEY", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"PARTITION_VALUE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"DISTRIBUTION_KEY", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"BUCKETS", TypeDescriptor::from_logical_type(TYPE_INT), sizeof(int), false},
        {"REPLICATION_NUM", TypeDescriptor::from_logical_type(TYPE_INT), sizeof(int), false},
        {"STORAGE_MEDIUM", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"COOLDOWN_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"LAST_CONSISTENCY_CHECK_TIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), true},
        {"IS_IN_MEMORY", TypeDescriptor::from_logical_type(TYPE_BOOLEAN), sizeof(bool), false},
        {"IS_TEMP", TypeDescriptor::from_logical_type(TYPE_BOOLEAN), sizeof(bool), false},
        {"DATA_SIZE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"ROW_COUNT", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), false},
        {"ENABLE_DATACACHE", TypeDescriptor::from_logical_type(TYPE_BOOLEAN), sizeof(bool), false},
        {"AVG_CS", TypeDescriptor::from_logical_type(TYPE_DOUBLE), sizeof(double), false},
        {"P50_CS", TypeDescriptor::from_logical_type(TYPE_DOUBLE), sizeof(double), false},
        {"MAX_CS", TypeDescriptor::from_logical_type(TYPE_DOUBLE), sizeof(double), false},
        {"STORAGE_PATH", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
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
    int64_t table_id_offset = 0;
    while (true) {
        TGetPartitionsMetaRequest partitions_meta_req;
        partitions_meta_req.__set_auth_info(auth_info);
        partitions_meta_req.__set_start_table_id_offset(table_id_offset);
        TGetPartitionsMetaResponse partitions_meta_response;
        RETURN_IF_ERROR(SchemaHelper::get_partitions_meta(_ss_state, partitions_meta_req, &partitions_meta_response));
        _partitions_meta_vec.insert(_partitions_meta_vec.end(), partitions_meta_response.partitions_meta_infos.begin(),
                                    partitions_meta_response.partitions_meta_infos.end());
        table_id_offset = partitions_meta_response.next_table_id_offset;
        if (!table_id_offset) {
            break;
        }
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
    if (_partitions_meta_index >= _partitions_meta_vec.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return fill_chunk(chunk);
}

Status SchemaPartitionsMetaScanner::fill_chunk(ChunkPtr* chunk) {
    const TPartitionMetaInfo& info = _partitions_meta_vec[_partitions_meta_index];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        if (slot_id < 1 || slot_id > _column_num) {
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
            // DATA_VERSION
            fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.data_version);
            break;
        }
        case 10: {
            // VERSION_EPOCH
            fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.version_epoch);
            break;
        }
        case 11: {
            // VERSION_TXN_TYPE
            std::string version_txn_type_str = to_string(info.version_txn_type);
            Slice version_txn_type = Slice(version_txn_type_str);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&version_txn_type);
            break;
        }
        case 12: {
            // PARTITION_KEY
            Slice partition_key = Slice(info.partition_key);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&partition_key);
            break;
        }
        case 13: {
            // PARTITION_VALUE
            Slice partition_value = Slice(info.partition_value);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&partition_value);
            break;
        }
        case 14: {
            // DISTRIBUTION_KEY
            Slice distribution_key = Slice(info.distribution_key);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&distribution_key);
            break;
        }
        case 15: {
            // BUCKETS
            fill_column_with_slot<TYPE_INT>(column.get(), (void*)&info.buckets);
            break;
        }
        case 16: {
            // REPLICATION_NUM
            fill_column_with_slot<TYPE_INT>(column.get(), (void*)&info.replication_num);
            break;
        }
        case 17: {
            // STORAGE_MEDIUM
            Slice storage_medium = Slice(info.storage_medium);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&storage_medium);
            break;
        }
        case 18: {
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
        case 19: {
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
        case 20: {
            // IS_IN_MEMORY
            fill_column_with_slot<TYPE_BOOLEAN>(column.get(), (void*)&info.is_in_memory);
            break;
        }
        case 21: {
            // IS_TEMP
            fill_column_with_slot<TYPE_BOOLEAN>(column.get(), (void*)&info.is_temp);
            break;
        }
        case 22: {
            // DATA_SIZE
            Slice data_size = Slice(info.data_size);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&data_size);
            break;
        }
        case 23: {
            // ROW_COUNT
            fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.row_count);
            break;
        }
        case 24: {
            // ENABLE_DATACACHE
            fill_column_with_slot<TYPE_BOOLEAN>(column.get(), (void*)&info.enable_datacache);
            break;
        }
        case 25: {
            // AVG_CS
            fill_column_with_slot<TYPE_DOUBLE>(column.get(), (void*)&info.avg_cs);
            break;
        }
        case 26: {
            // P50_CS
            fill_column_with_slot<TYPE_DOUBLE>(column.get(), (void*)&info.p50_cs);
            break;
        }
        case 27: {
            // MAX_CS
            fill_column_with_slot<TYPE_DOUBLE>(column.get(), (void*)&info.max_cs);
            break;
        }
        case 28: {
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
