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

#include "exec/schema_scanner/schema_keywords_scanner.h"

#include <fmt/format.h>

#include "common/logging.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaKeywordsScanner::_s_columns[] = {
        //   name,       type,          size,     is_null
        {"KEYWORD", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"RESERVED", TypeDescriptor::from_logical_type(TYPE_BOOLEAN), sizeof(bool), false},
};

SchemaKeywordsScanner::SchemaKeywordsScanner()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

Status SchemaKeywordsScanner::start(RuntimeState* state) {
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
        TGetKeywordsRequest keywords_req;
        keywords_req.__set_auth_info(auth_info);
        keywords_req.__set_start_table_id_offset(table_id_offset);
        TGetKeywordsResponse keywords_response;
        RETURN_IF_ERROR(SchemaHelper::get_keywords(_ss_state, keywords_req, &keywords_response));
        _keywords_vec.insert(_keywords_vec.end(), keywords_response.keywords.begin(), keywords_response.keywords.end());
        table_id_offset = keywords_response.next_table_id_offset;
        if (!table_id_offset) {
            break;
        }
    }
    _ctz = state->timezone_obj();
    _keywords_index = 0;
    return Status::OK();
}

Status SchemaKeywordsScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }
    if (_keywords_index >= _keywords_vec.size()) {
        *eos = true;
        return Status::OK();
    }
    *eos = false;
    return fill_chunk(chunk);
}

Status SchemaKeywordsScanner::fill_chunk(ChunkPtr* chunk) {
    const TKeywordInfo& info = _keywords_vec[_keywords_index];
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        if (slot_id < 1 || slot_id > _column_num) {
            return Status::InternalError(fmt::format("invalid slot id:{}", slot_id));
        }
        ColumnPtr column = (*chunk)->get_column_by_slot_id(slot_id);

        switch (slot_id) {
        case 1: {
            // KEYWORD
            Slice keyword = Slice(info.keyword);
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&keyword);
            break;
        }
        case 2: {
            // IS_RESERVED
            fill_column_with_slot<TYPE_BOOLEAN>(column.get(), (void*)&info.reserved);
            break;
        }
        default:
            break;
        }
    }
    _keywords_index++;
    return Status::OK();
}
} // namespace starrocks