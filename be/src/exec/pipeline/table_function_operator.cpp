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
        _table_function->close(state, _table_function_state);
    }
    Operator::close(state);
}

bool TableFunctionOperator::has_output() const {
    return _input_chunk != nullptr && (_remain_repeat_times > 0 || _input_chunk_index < _input_chunk->num_rows());
}

bool TableFunctionOperator::need_input() const {
    return _input_chunk == nullptr;
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

    _input_chunk_index = 0;
    _table_function_result_eos = false;
    _remain_repeat_times = 0;

    _table_function_exec_timer = ADD_TIMER(_unique_metrics, "TableFunctionExecTime");
    _table_function_exec_counter = ADD_COUNTER(_unique_metrics, "TableFunctionExecCount", TUnit::UNIT);
    RETURN_IF_ERROR(_table_function->prepare(_table_function_state));
    return _table_function->open(state, _table_function_state);
}

StatusOr<ChunkPtr> TableFunctionOperator::pull_chunk(RuntimeState* state) {
    DCHECK(_input_chunk != nullptr);
    size_t chunk_size = state->chunk_size();
    size_t remain_chunk_size = chunk_size;
    std::vector<ColumnPtr> output_columns;

    _process_table_function();

    output_columns.reserve(_outer_slots.size());
    for (int _outer_slot : _outer_slots) {
        output_columns.emplace_back(_input_chunk->get_column_by_slot_id(_outer_slot)->clone_empty());
    }
    for (size_t i = 0; i < _fn_result_slots.size(); ++i) {
        output_columns.emplace_back(_table_function_result.first[i]->clone_empty());
    }

    // If _remain_repeat_times > 0, first use the remaining data of the previous chunk to construct this data
    while (_remain_repeat_times > 0 || _input_chunk_index < _input_chunk->num_rows()) {
        bool has_remain_repeat_times = _remain_repeat_times > 0;

        if (!has_remain_repeat_times) {
            DCHECK_LT(_input_chunk_index + 1, _table_function_result.second->size());
            _remain_repeat_times = _table_function_result.second->get(_input_chunk_index + 1).get_int32() -
                                   _table_function_result.second->get(_input_chunk_index).get_int32();
        }
        size_t repeat_times = std::min(_remain_repeat_times, remain_chunk_size);
        if (repeat_times == 0) {
            ++_input_chunk_index;
            continue;
        }

        // Build outer data, repeat multiple times
        for (size_t i = 0; i < _outer_slots.size(); ++i) {
            ColumnPtr& input_column_ptr = _input_chunk->get_column_by_slot_id(_outer_slots[i]);
            Datum value = input_column_ptr->get(_input_chunk_index);
            if (value.is_null()) {
                DCHECK(output_columns[i]->is_nullable());
                down_cast<NullableColumn*>(output_columns[i].get())->append_nulls(repeat_times);
            } else {
                output_columns[i]->append_value_multiple_times(&value, repeat_times);
            }
        }
        // Build table function result
        for (size_t i = 0; i < _fn_result_slots.size(); ++i) {
            uint32_t start_offset;
            if (has_remain_repeat_times) {
                start_offset =
                        _table_function_result.second->get(_input_chunk_index + 1).get_int32() - _remain_repeat_times;
            } else {
                start_offset = _table_function_result.second->get(_input_chunk_index).get_int32();
            }
            output_columns[_outer_slots.size() + i]->append(*(_table_function_result.first[i]), start_offset,
                                                            repeat_times);
        }

        remain_chunk_size -= repeat_times;
        _remain_repeat_times -= repeat_times;
        if (_remain_repeat_times == 0) {
            ++_input_chunk_index;
        }

        if (remain_chunk_size == 0) {
            // Chunk is full
            break;
        }
    }

    // Current input chunk has been processed, clean the state to be ready for next input chunk
    if (_remain_repeat_times == 0 && _input_chunk_index >= _input_chunk->num_rows()) {
        _input_chunk = nullptr;
    }

    // Just return the chunk whether its full or not in order to keep the semantics of pipeline
    return _build_chunk(output_columns);
}

Status TableFunctionOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    _input_chunk = chunk;
    _table_function_result_eos = false;

    _input_chunk_index = 0;
    Columns table_function_params;
    for (SlotId slotId : _param_slots) {
        table_function_params.emplace_back(_input_chunk->get_column_by_slot_id(slotId));
    }

    _table_function_state->set_params(table_function_params);
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

void TableFunctionOperator::_process_table_function() {
    if (!_table_function_result_eos) {
        SCOPED_TIMER(_table_function_exec_timer);
        COUNTER_UPDATE(_table_function_exec_counter, 1);
        _table_function_result = _table_function->process(_table_function_state, &_table_function_result_eos);
        DCHECK_EQ(_input_chunk->num_rows() + 1, _table_function_result.second->size());
    }
}
Status TableFunctionOperator::reset_state(starrocks::RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks) {
    _input_chunk.reset();
    _remain_repeat_times = 0;
    _input_chunk_index = 0;
    _table_function_result_eos = false;
    _is_finished = false;
    return Status::OK();
}
} // namespace starrocks::pipeline
