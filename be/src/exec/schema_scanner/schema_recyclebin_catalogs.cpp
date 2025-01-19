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

#include "exec/schema_scanner/schema_recyclebin_catalogs.h"

#include "exec/schema_scanner.h"
#include "exec/schema_scanner/schema_helper.h"
#include "gen_cpp/FrontendService_types.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaRecycleBinCatalogs::_s_columns[] = {
        {"TYPE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"DBID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"TABLEID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"PARTID", TypeDescriptor::from_logical_type(TYPE_BIGINT), sizeof(int64_t), true},
        {"DROPTIME", TypeDescriptor::from_logical_type(TYPE_DATETIME), sizeof(DateTimeValue), false},
};

SchemaRecycleBinCatalogs::SchemaRecycleBinCatalogs()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

Status SchemaRecycleBinCatalogs::start(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    return SchemaScanner::start(state);
}

Status SchemaRecycleBinCatalogs::_listRecycleBinCatalogs() {
    RETURN_IF(_param->ip == nullptr || _param->port == 0, Status::InternalError("unknown frontend address"));

    TListRecycleBinCatalogsParams params;
    if (_param->current_user_ident) {
        params.__set_user_ident(*_param->current_user_ident);
    }
    return SchemaHelper::listRecycleBinCatalogs(_ss_state, params, &_recyclebin_catalogs_result);
}

Status SchemaRecycleBinCatalogs::get_next(ChunkPtr* chunk, bool* eos) {
    while (_cur_row >= _recyclebin_catalogs_result.recyclebin_catalogs.size()) {
        if (!_fetched) {
            _fetched = true;
            RETURN_IF_ERROR(_listRecycleBinCatalogs());
        } else {
            *eos = true;
            return Status::OK();
        }
    }
    *eos = false;
    return _fill_chunk(chunk);
}

Status SchemaRecycleBinCatalogs::_fill_chunk(ChunkPtr* chunk) {
    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    auto& info = _recyclebin_catalogs_result.recyclebin_catalogs.at(_cur_row++);
    for (const auto& [slot_id, index] : slot_id_map) {
        switch (slot_id) {
        case 1: {
            // type
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(1);
                const std::string* str = &info.type;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 2: {
            // name
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(2);
                const std::string* str = &info.name;
                Slice value(str->c_str(), str->length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 3: {
            // dbid
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(3);
                if (info.__isset.dbid) {
                    fill_data_column_with_null(column.get());
                } else {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.dbid);
                }
            }
            break;
        }
        case 4: {
            // tableid
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(4);
                if (info.__isset.tableid) {
                    fill_data_column_with_null(column.get());
                } else {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.tableid);
                }
            }
            break;
        }
        case 5: {
            // partitionid
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(5);
                if (info.__isset.partitionid) {
                    fill_data_column_with_null(column.get());
                } else {
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), (void*)&info.partitionid);
                }
            }
            break;
        }
        case 6: {
            // droptime
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(6);
                DateTimeValue t;
                t.from_unixtime(info.droptime, _runtime_state->timezone_obj());
                fill_column_with_slot<TYPE_DATETIME>(column.get(), (void*)&t);
            }
            break;
        }
        default:
            break;
        }
    }
    return {};
}

} // namespace starrocks
