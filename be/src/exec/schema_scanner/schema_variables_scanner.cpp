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

#include "exec/schema_scanner/schema_variables_scanner.h"

#include "exec/schema_scanner/schema_helper.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "types/logical_type.h"
#include "util/failpoint/fail_point.h"

namespace starrocks {
DEFINE_FAIL_POINT(schema_scan_rpc_failed);

SchemaScanner::ColumnDesc SchemaVariablesScanner::_s_vars_columns[] = {
        //   name,       type,          size
        {"VARIABLE_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"VARIABLE_VALUE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
};

SchemaScanner::ColumnDesc SchemaVariablesScanner::_s_verbose_vars_columns[] = {
        //   name,       type,          size
        {"VARIABLE_NAME", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"VARIABLE_VALUE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"DEFAULT_VALUE", TypeDescriptor::create_varchar_type(sizeof(StringValue)), sizeof(StringValue), false},
        {"IS_CHANGED", TypeDescriptor::from_logical_type(TYPE_BOOLEAN), 1, false},
};

SchemaVariablesScanner::SchemaVariablesScanner(TVarType::type type)
        : SchemaScanner(type == TVarType::VERBOSE ? _s_verbose_vars_columns : _s_vars_columns,
                        type == TVarType::VERBOSE ? 4 : 2),
          _type(type) {}

SchemaVariablesScanner::~SchemaVariablesScanner() = default;

Status SchemaVariablesScanner::start(RuntimeState* state) {
    TShowVariableRequest var_params;
    // Use db to save type
    if (_param->db != nullptr) {
        if (strcmp(_param->db->c_str(), "GLOBAL") == 0) {
            var_params.__set_varType(TVarType::GLOBAL);
        } else {
            var_params.__set_varType(TVarType::SESSION);
        }
    } else {
        var_params.__set_varType(_type);
    }
    var_params.__set_threadId(_param->thread_id);

    // init schema scanner state
    FAIL_POINT_TRIGGER_RETURN_ERROR(schema_scan_rpc_failed);
    RETURN_IF_ERROR(SchemaScanner::init_schema_scanner_state(state));
    RETURN_IF_ERROR(SchemaHelper::show_variables(_ss_state, var_params, &_var_result));

    if (_type != TVarType::VERBOSE) {
        _begin = _var_result.variables.begin();
    } else {
        if (!_var_result.__isset.verbose_variables) {
            return Status::InternalError("invalid verbose show variables result");
        }
        _verbose_iter = _var_result.verbose_variables.begin();
    }
    return Status::OK();
}

Status SchemaVariablesScanner::fill_chunk(ChunkPtr* chunk) {
    if (_type == TVarType::VERBOSE) {
        return _fill_chunk_for_verbose(chunk);
    }
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        switch (slot_id) {
        case 1: {
            // variables names
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(1);
                Slice value(_begin->first.c_str(), _begin->first.length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 2: {
            // value
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(2);
                Slice value(_begin->second.c_str(), _begin->second.length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        default:
            break;
        }
    }
    ++_begin;
    return Status::OK();
}

Status SchemaVariablesScanner::_fill_chunk_for_verbose(ChunkPtr* chunk) {
    const auto& slot_id_to_index_map = (*chunk)->get_slot_id_to_index_map();
    for (const auto& [slot_id, index] : slot_id_to_index_map) {
        switch (slot_id) {
        case 1: {
            // variables names
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(1);
                Slice value(_verbose_iter->variable_name.c_str(), _verbose_iter->variable_name.length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 2: {
            // value
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(2);
                Slice value(_verbose_iter->value.c_str(), _verbose_iter->value.length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 3: {
            // default_value
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(3);
                Slice value(_verbose_iter->default_value.c_str(), _verbose_iter->default_value.length());
                fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
            }
            break;
        }
        case 4: {
            // is_changed
            {
                ColumnPtr column = (*chunk)->get_column_by_slot_id(4);
                bool is_changed = _verbose_iter->is_changed;
                fill_column_with_slot<TYPE_BOOLEAN>(column.get(), (void*)&is_changed);
            }
            break;
        }
        default:
            break;
        }
    }
    ++_verbose_iter;
    return Status::OK();
}

Status SchemaVariablesScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_type != TVarType::VERBOSE && _begin == _var_result.variables.end()) {
        *eos = true;
        return Status::OK();
    }
    if (_type == TVarType::VERBOSE && _verbose_iter == _var_result.verbose_variables.end()) {
        *eos = true;
        return Status::OK();
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("invalid parameter.");
    }
    *eos = false;
    return fill_chunk(chunk);
}

} // namespace starrocks
