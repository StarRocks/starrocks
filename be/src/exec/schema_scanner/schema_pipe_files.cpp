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

#include "exec/schema_scanner/schema_pipe_files.h"

#include "exec/schema_scanner.h"
#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaTablePipeFiles::_s_columns[] = {
        {"DATABASE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"PIPE_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"PIPE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"FILE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ROW_COUNT", TYPE_BIGINT, sizeof(int64_t), false},
        {"FILE_SIZE", TYPE_BIGINT, sizeof(int64_t), false},
        {"LOAD_STATE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ERROR_MSG", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ERROR_LINE_NUMBER", TYPE_BIGINT, sizeof(int64_t), false},
        {"ERROR_COLUMN", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaTablePipeFiles::SchemaTablePipeFiles()
    :SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) ){
}

Status SchemaTablePipeFiles::start(RuntimeState* state) {
    RETURN_IF_ERROR(SchemaScanner::start(state));

    return {};
}

Status SchemaTablePipeFiles::get_next(ChunkPtr* chunk, bool* eos) {
    if (_cur_idx > 0) {
        *eos = true;
        return {};
    }
    fill_chunk(chunk);
    *eos = false;
    return {};
}

Status SchemaTablePipeFiles::fill_chunk(ChunkPtr* chunk) {
    _cur_idx++;
    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_map) {
        Column* column = (*chunk)->get_column_by_slot_id(slot_id).get();
        if (column->is_binary()) {
            Slice fake = "fake";
            fill_column_with_slot<TYPE_VARCHAR>(column, &fake);
        } else if (column->is_numeric()) {
            int64_t fake = 1024;
            fill_column_with_slot<TYPE_BIGINT>(column, &fake);
        }
    }
    return {};
}

} // namespace starrocks