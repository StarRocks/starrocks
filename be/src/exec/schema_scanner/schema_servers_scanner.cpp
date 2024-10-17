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

#include "exec/schema_scanner/schema_servers_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaServersScanner::_s_tbls_columns[] = {
    //   name,       type,          size,     is_null
    {"BE_ID", TYPE_BIGINT, sizeof(int64_t), false},
    {"VERSION", TYPE_VARCHAR, sizeof(StringValue), false},
    {"IP", TYPE_VARCHAR, sizeof(StringValue), false},
    {"HEARTBEAT_PORT", TYPE_INT, sizeof(int32_t), false},
    {"BE_PORT", TYPE_INT, sizeof(int32_t), false},
    {"HTTP_PORT", TYPE_INT, sizeof(int32_t), false},
    {"BRPC_PORT", TYPE_INT, sizeof(int32_t), false},
    {"ALIVE", TYPE_BOOLEAN, sizeof(bool), false},
    {"DECOMMISSIONED", TYPE_VARCHAR, sizeof(StringValue), false},
    {"DATA_USED_CAPACITY", TYPE_BIGINT, sizeof(int64_t), false},
    {"AVAIL_CAPACITY", TYPE_BIGINT, sizeof(int64_t), false},
    {"TOTAL_CAPACITY", TYPE_BIGINT, sizeof(int64_t), false},
    {"DATA_TOTAL_CAPACITY", TYPE_BIGINT, sizeof(int64_t), false},
    {"CPU_CORES", TYPE_INT, sizeof(int32_t), false}};

SchemaServersScanner::SchemaServersScanner(TServerType::type type)
    : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)),
    _type(type){}

SchemaServersScanner::~SchemaServersScanner() = default;

Status SchemaServersScanner::start(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanner::start(state));
    TGetServersParams params;

    params.__set_type(_type);
    int32_t timeout = static_cast<int32_t>(std::min(state->query_options().query_timeout * 1000 / 2, INT_MAX));
    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_servers(*(_param->ip), _param->port, params, &_result, timeout));
    } else {
        return Status::InternalError("IP or port doesn't exists");
    }

    _cur_idx = 0;
    return Status::OK();
}

Status SchemaServersScanner::fill_be_chunk(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (; _cur_idx < _result.backends.size(); _cur_idx++) {
        auto& info = _result.backends[_cur_idx];
        for (const auto& [slot_id, index] : slot_id_to_index_map) {
            if (slot_id < 1 || slot_id > 23) {
                return Status::InternalError(fmt::format("invalid slot id:{}", slot_id));
            }

            ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);
            switch (slot_id) {
                case 1: {
                    // be_id
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), &info.id);
                    break;
                }
                case 2: {
                    // Version
                    Slice version = Slice(info.version);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), &version);
                    break;
                }
                case 3: {
                    // IP
                    Slice ip = Slice(info.ip);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), &ip);
                    break;
                }
                case 4: {
                    // heartbeat_port
                    fill_column_with_slot<TYPE_INT>(column.get(), &info.heartbeat_port);
                    break;
                }
                case 5: {
                    // be_port
                    fill_column_with_slot<TYPE_INT>(column.get(), &info.be_port);
                    break;
                }
                case 6: {
                    // http_port
                    fill_column_with_slot<TYPE_INT>(column.get(), &info.http_port);
                    break;
                }
                case 7: {
                    // brpc_port
                    fill_column_with_slot<TYPE_INT>(column.get(), &info.brpc_port);
                    break;
                }
                case 8: {
                    // alive
                    fill_column_with_slot<TYPE_BOOLEAN>(column.get(), &info.alive);
                    break;
                }
                case 9: {
                    // decommissioned
                    Slice decommissioned = Slice(info.decommissioned);
                    fill_column_with_slot<TYPE_VARCHAR>(column.get(), &decommissioned);
                    break;
                }
                case 10: {
                    // data_used_capacity
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), &info.data_used_capacity);
                    break;
                }
                case 11: {
                    // avail_capacity
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), &info.avail_capacity);
                    break;
                }
                case 12: {
                    // total_capacity
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), &info.total_capacity);
                    break;
                }
                case 13: {
                    // data_total_capacity
                    fill_column_with_slot<TYPE_BIGINT>(column.get(), &info.data_total_capacity);
                    break;
                }
                case 14: {
                    // cpu_cores
                    fill_column_with_slot<TYPE_INT>(column.get(), &info.cpu_cores);
                    break;
                }
                default:
                    break;
            }
        }
    }
    return Status::OK();
}

Status SchemaServersScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("invalid parameter.");
    }
    if (_type == TServerType::BACKEND) {
        if (_cur_idx >= _result.backends.size()) {
            *eos = true;
            return Status::OK();
        }
        *eos = false;
        return fill_be_chunk(chunk);
    }
    return Status::InternalError("invalid parameter.");
}

} // namespace starrocks
