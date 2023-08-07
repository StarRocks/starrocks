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
#include "gen_cpp/FrontendService_types.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"

namespace starrocks {

SchemaScanner::ColumnDesc SchemaTablePipeFiles::_s_columns[] = {
        {"DATABASE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"PIPE_ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"PIPE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},

        {"FILE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"FILE_VERSION", TYPE_VARCHAR, sizeof(StringValue), false},
        {"FILE_ROWS", TYPE_BIGINT, sizeof(int64_t), false},
        {"FILE_SIZE", TYPE_BIGINT, sizeof(int64_t), false},
        {"LAST_MODIFIED", TYPE_VARCHAR, sizeof(StringValue), false},

        {"LOAD_STATE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"STAGED_TIME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"START_LOAD_TIME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"FINISH_LOAD_TIME", TYPE_VARCHAR, sizeof(StringValue), false},

        {"ERROR_MSG", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ERROR_COUNT", TYPE_BIGINT, sizeof(int64_t), false},
        {"ERROR_LINE", TYPE_BIGINT, sizeof(int64_t), false},
};

SchemaTablePipeFiles::SchemaTablePipeFiles()
        : SchemaScanner(_s_columns, sizeof(_s_columns) / sizeof(SchemaScanner::ColumnDesc)) {}

Status SchemaTablePipeFiles::start(RuntimeState* state) {
    return SchemaScanner::start(state);
}

Status SchemaTablePipeFiles::_list_pipe_files() {
    RETURN_IF(_param->ip == nullptr || _param->port == 0, Status::InternalError("unknown frontend address"));

    TListPipeFilesParams params;
    if (_param->current_user_ident) {
        params.__set_user_ident(*_param->current_user_ident);
    }
    return SchemaHelper::list_pipe_files(*(_param->ip), _param->port, params, &_pipe_files_result);
}

Status SchemaTablePipeFiles::get_next(ChunkPtr* chunk, bool* eos) {
    while (_cur_row >= _pipe_files_result.pipe_files.size()) {
        if (!_fetched) {
            // send RPC
            _fetched = true;
            RETURN_IF_ERROR(_list_pipe_files());
        } else {
            *eos = true;
            return Status::OK();
        }
    }
    *eos = false;
    return _fill_chunk(chunk);
}

DatumArray SchemaTablePipeFiles::_build_row() {
    auto& pipe_file = _pipe_files_result.pipe_files.at(_cur_row++);
    return {
            Slice(pipe_file.database_name),
            pipe_file.pipe_id,
            Slice(pipe_file.pipe_name),

            Slice(pipe_file.file_name),
            Slice(pipe_file.file_version),
            pipe_file.file_rows,
            pipe_file.file_size,
            Slice(pipe_file.last_modified),

            Slice(pipe_file.state),
            Slice(pipe_file.staged_time),
            Slice(pipe_file.start_load),
            Slice(pipe_file.finish_load),

            Slice(pipe_file.first_error_msg),
            (int64_t)pipe_file.error_count,
            (int64_t)pipe_file.error_line,
    };
}

Status SchemaTablePipeFiles::_fill_chunk(ChunkPtr* chunk) {
    auto& slot_id_map = (*chunk)->get_slot_id_to_index_map();
    auto datum_array = _build_row();
    for (const auto& [slot_id, index] : slot_id_map) {
        Column* column = (*chunk)->get_column_by_slot_id(slot_id).get();
        column->append_datum(datum_array[slot_id - 1]);
    }
    return {};
}

} // namespace starrocks