// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "exec/vectorized/schema_scanner/schema_variables_scanner.h"

#include "column/chunk.h"
#include "exec/vectorized/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"

namespace starrocks::vectorized {

SchemaScanner::ColumnDesc SchemaVariablesScanner::_s_vars_columns[] = {
        //   name,       type,          size
        {"VARIABLE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"VARIABLE_VALUE", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaVariablesScanner::SchemaVariablesScanner(TVarType::type type)
        : SchemaScanner(_s_vars_columns, sizeof(_s_vars_columns) / sizeof(SchemaScanner::ColumnDesc)), _type(type) {}

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

    if (nullptr != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::show_varialbes(*(_param->ip), _param->port, var_params, &_var_result));
    } else {
        return Status::InternalError("IP or port dosn't exists");
    }
    _begin = _var_result.variables.begin();
    return Status::OK();
}

Status SchemaVariablesScanner::fill_chunk(ChunkPtr* chunk) {
    // variables names
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[0]->id());
        Slice value(_begin->first.c_str(), _begin->first.length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    // value
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[1]->id());
        Slice value(_begin->second.c_str(), _begin->second.length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void*)&value);
    }
    ++_begin;
    return Status::OK();
}

Status SchemaVariablesScanner::get_next(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (_begin == _var_result.variables.end()) {
        *eos = true;
        return Status::OK();
    }
    if (nullptr == chunk || nullptr == eos) {
        return Status::InternalError("invalid parameter.");
    }
    *eos = false;
    return fill_chunk(chunk);
}

} // namespace starrocks::vectorized
