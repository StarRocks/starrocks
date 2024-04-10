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

#include "table_function_operator.h"

namespace starrocks::pipeline {

void TableFunctionOperator::close(RuntimeState* state) {
    if (_table_function != nullptr && _table_function_state != nullptr) {
        (void)_table_function->close(state, _table_function_state);
        _table_function_state = nullptr;
    }
    Operator::close(state);
}

bool TableFunctionOperator::has_output() const {
    if (!_table_function_result.first.empty() && _next_output_row < _table_function_result.first[0]->size()) {
        return true;
    }
    if (_input_chunk != nullptr && _table_function_state != nullptr &&
        _table_function_state->processed_rows() < _input_chunk->num_rows()) {
        return true;
    }
    return false;
}

bool TableFunctionOperator::need_input() const {
    return !has_output();
}

bool TableFunctionOperator::is_finished() const {
    return _is_finished && !has_output();
}

Status TableFunctionOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

Status TableFunctionOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    if (_tnode.table_function_node.__isset.param_columns) {
        _param_slots.insert(_param_slots.end(), _tnode.table_function_node.param_columns.begin(),
                            _tnode.table_function_node.param_columns.end());
    } else {
        return Status::InternalError("param slots not set in table function node");
    }

    if (_tnode.table_function_node.__isset.outer_columns) {
        _outer_slots.insert(_outer_slots.end(), _tnode.table_function_node.outer_columns.begin(),
                            _tnode.table_function_node.outer_columns.end());
    } else {
        return Status::InternalError("outer slots not set in table function node");
    }

    if (_tnode.table_function_node.__isset.fn_result_columns) {
        _fn_result_slots.insert(_fn_result_slots.end(), _tnode.table_function_node.fn_result_columns.begin(),
                                _tnode.table_function_node.fn_result_columns.end());
    } else {
        return Status::InternalError("fn result slots not set in table function node");
    }

    // Get table function from TableFunctionResolver
    TFunction table_fn = _tnode.table_function_node.table_function.nodes[0].fn;
    std::string table_function_name = table_fn.name.function_name;
    std::vector<LogicalType> arg_types;
    for (const TTypeDesc& ttype_desc : table_fn.arg_types) {
        TypeDescriptor arg_type = TypeDescriptor::from_thrift(ttype_desc);
        arg_types.emplace_back(arg_type.type);
    }

    std::vector<LogicalType> return_types;
    for (const TTypeDesc& ttype_desc : table_fn.table_fn.ret_types) {
        TypeDescriptor return_type = TypeDescriptor::from_thrift(ttype_desc);
        return_types.emplace_back(return_type.type);
    }

    if (table_function_name == "unnest" && arg_types.size() > 1) {
        _table_function = get_table_function(table_function_name, {}, {}, table_fn.binary_type);
    } else {
        _table_function = get_table_function(table_function_name, arg_types, return_types, table_fn.binary_type);
    }

    if (_table_function == nullptr) {
        return Status::InternalError("can't find table function " + table_function_name);
    }
    RETURN_IF_ERROR(_table_function->init(table_fn, &_table_function_state));

    _table_function_exec_timer = ADD_TIMER(_unique_metrics, "TableFunctionExecTime");
    _table_function_exec_counter = ADD_COUNTER(_unique_metrics, "TableFunctionExecCount", TUnit::UNIT);
    RETURN_IF_ERROR(_table_function->prepare(_table_function_state));
    return _table_function->open(state, _table_function_state);
}

StatusOr<ChunkPtr> TableFunctionOperator::pull_chunk(RuntimeState* state) {
    DCHECK(_input_chunk != nullptr);
    size_t max_chunk_size = state->chunk_size();
    std::vector<ColumnPtr> output_columns;

    if (_table_function_result.second == nullptr) {
        RETURN_IF_ERROR(_process_table_function(state));
    }

    output_columns.reserve(_outer_slots.size());
    for (int _outer_slot : _outer_slots) {
        output_columns.emplace_back(_input_chunk->get_column_by_slot_id(_outer_slot)->clone_empty());
    }
    for (size_t i = 0; i < _fn_result_slots.size(); ++i) {
        output_columns.emplace_back(_table_function_result.first[i]->clone_empty());
    }

    while (output_columns[0]->size() < max_chunk_size) {
        if (!_table_function_result.first.empty() && _next_output_row < _table_function_result.first[0]->size()) {
            _copy_result(output_columns, max_chunk_size);
        } else if (_table_function_state->processed_rows() < _input_chunk->num_rows()) {
            RETURN_IF_ERROR(_process_table_function(state));
        } else {
            DCHECK(!has_output());
            DCHECK(need_input());
            break;
        }
    }

    // Just return the chunk whether its full or not in order to keep the semantics of pipeline
    return _build_chunk(output_columns);
}

Status TableFunctionOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    DCHECK(_input_chunk == nullptr || _table_function_state->processed_rows() >= _input_chunk->num_rows());
    _input_chunk = chunk;
    _input_index_of_first_result = 0;
    _next_output_row = 0;
    _next_output_row_offset = 0;
    Columns table_function_params;
    for (SlotId slotId : _param_slots) {
        table_function_params.emplace_back(_input_chunk->get_column_by_slot_id(slotId));
    }

    _table_function_state->set_params(table_function_params);
    _table_function_result.first.clear();
    _table_function_result.second = nullptr;
    return Status::OK();
}

ChunkPtr TableFunctionOperator::_build_chunk(const std::vector<ColumnPtr>& columns) {
    ChunkPtr chunk = std::make_shared<Chunk>();

    for (size_t i = 0; i < _outer_slots.size(); ++i) {
        chunk->append_column(columns[i], _outer_slots[i]);
    }
    for (size_t i = 0; i < _fn_result_slots.size(); ++i) {
        chunk->append_column(columns[_outer_slots.size() + i], _fn_result_slots[i]);
    }

    return chunk;
}

Status TableFunctionOperator::_process_table_function(RuntimeState* state) {
    SCOPED_TIMER(_table_function_exec_timer);
    COUNTER_UPDATE(_table_function_exec_counter, 1);
    _input_index_of_first_result = _table_function_state->processed_rows();
    _next_output_row = 0;
    _next_output_row_offset = 0;

    _table_function_result = _table_function->process(state, _table_function_state);
    return _table_function_state->status();
}

Status TableFunctionOperator::reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) {
    _input_chunk.reset();
    _input_index_of_first_result = 0;
    _next_output_row_offset = 0;
    _next_output_row = 0;
    _is_finished = false;
    _table_function_result.first.clear();
    _table_function_result.second = nullptr;
    if (_table_function_state != nullptr) {
        _table_function_state->set_params(Columns{});
    }
    return Status::OK();
}

void TableFunctionOperator::_copy_result(const std::vector<ColumnPtr>& columns, uint32_t max_output_size) {
    DCHECK_LE(_next_output_row, _table_function_result.first[0]->size());
    DCHECK_LT(_next_output_row_offset, _table_function_result.second->size());
    uint32_t curr_output_size = columns[0]->size();
    const auto& fn_result_cols = _table_function_result.first;
    const auto& offsets_col = _table_function_result.second;
    while (curr_output_size < max_output_size && _next_output_row < fn_result_cols[0]->size()) {
        uint32_t start = _next_output_row;
        uint32_t end = offsets_col->get_data()[_next_output_row_offset + 1];
        DCHECK_GE(start, offsets_col->get_data()[_next_output_row_offset]);
        DCHECK_LE(start, end);
        uint32_t copy_rows = std::min(end - start, max_output_size - curr_output_size);
        VLOG(2) << "_next_output_row=" << _next_output_row << " start=" << start << " end=" << end
                << " copy_rows=" << copy_rows << " input_size=" << fn_result_cols[0]->size()
                << " _next_output_row_offset=" << _next_output_row_offset
                << " _input_index_of_first_result=" << _input_index_of_first_result;

        if (copy_rows > 0) {
            // Build outer data, repeat multiple times
            for (size_t i = 0; i < _outer_slots.size(); ++i) {
                ColumnPtr& input_column_ptr = _input_chunk->get_column_by_slot_id(_outer_slots[i]);
                Datum value = input_column_ptr->get(_input_index_of_first_result + _next_output_row_offset);
                if (value.is_null()) {
                    DCHECK(columns[i]->is_nullable());
                    down_cast<NullableColumn*>(columns[i].get())->append_nulls(copy_rows);
                } else {
                    columns[i]->append_value_multiple_times(&value, copy_rows);
                }
            }

            // Build table function result
            for (size_t i = 0; i < _fn_result_slots.size(); ++i) {
                columns[_outer_slots.size() + i]->append(*(fn_result_cols[i]), start, copy_rows);
            }
        }

        curr_output_size += copy_rows;
        _next_output_row += copy_rows;
        DCHECK_LE(start + copy_rows, end);
        if (start + copy_rows == end) {
            _next_output_row_offset++;
        }
    }
}

} // namespace starrocks::pipeline
