// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/vectorized/table_function_node.h"

#include "column/chunk.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/table_function_operator.h"
#include "runtime/runtime_state.h"

namespace starrocks::vectorized {
TableFunctionNode::TableFunctionNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& desc)
        : ExecNode(pool, tnode, desc), _tnode(tnode) {}

TableFunctionNode::~TableFunctionNode() {
    if (runtime_state() != nullptr) {
        close(runtime_state());
    }
}

Status TableFunctionNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));

    if (tnode.table_function_node.__isset.param_columns) {
        _param_slots.insert(_param_slots.end(), tnode.table_function_node.param_columns.begin(),
                            tnode.table_function_node.param_columns.end());
    } else {
        return Status::InternalError("param slots not set in table function node");
    }

    if (tnode.table_function_node.__isset.outer_columns) {
        _outer_slots.insert(_outer_slots.end(), tnode.table_function_node.outer_columns.begin(),
                            tnode.table_function_node.outer_columns.end());
    } else {
        return Status::InternalError("outer slots not set in table function node");
    }

    if (tnode.table_function_node.__isset.fn_result_columns) {
        _fn_result_slots.insert(_fn_result_slots.end(), tnode.table_function_node.fn_result_columns.begin(),
                                tnode.table_function_node.fn_result_columns.end());
    } else {
        return Status::InternalError("fn result slots not set in table function node");
    }

    //Get table function from TableFunctionResolver
    TFunction table_fn = tnode.table_function_node.table_function.nodes[0].fn;
    std::string table_function_name = table_fn.name.function_name;
    std::vector<PrimitiveType> arg_types;
    for (const TTypeDesc& ttype_desc : table_fn.arg_types) {
        TypeDescriptor arg_type = TypeDescriptor::from_thrift(ttype_desc);
        arg_types.emplace_back(arg_type.type);
    }

    std::vector<PrimitiveType> return_types;
    for (const TTypeDesc& ttype_desc : table_fn.table_fn.ret_types) {
        TypeDescriptor return_type = TypeDescriptor::from_thrift(ttype_desc);
        return_types.emplace_back(return_type.type);
    }

    _table_function = get_table_function(table_function_name, arg_types, return_types);
    if (_table_function == nullptr) {
        return Status::InternalError("can't find table function " + table_function_name);
    }
    RETURN_IF_ERROR(_table_function->init(&_table_function_state));

    _input_chunk_seek_rows = 0;
    _table_function_result_eos = true;
    _outer_column_remain_repeat_times = 0;

    return Status::OK();
}

Status TableFunctionNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _table_function_exec_timer = ADD_TIMER(_runtime_profile, "TableFunctionTime");
    RETURN_IF_ERROR(_table_function->prepare(_table_function_state));
    return Status::OK();
}

Status TableFunctionNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(child(0)->open(state));
    return Status::OK();
}

Status TableFunctionNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    int chunk_size = runtime_state()->chunk_size();
    int reserve_chunk_size = chunk_size;
    std::vector<ColumnPtr> output_columns;

    if (reached_limit()) {
        *eos = true;
        return Status::OK();
    }

    if (_input_chunk_ptr == nullptr || !_table_function_result_eos) {
        RETURN_IF_ERROR(get_next_input_chunk(state, eos));
        if (*eos) {
            return Status::OK();
        }
    }

    output_columns.reserve(_outer_slots.size());
    for (int _outer_slot : _outer_slots) {
        output_columns.emplace_back(_input_chunk_ptr->get_column_by_slot_id(_outer_slot)->clone_empty());
    }
    for (int result_idx = 0; result_idx < _fn_result_slots.size(); ++result_idx) {
        output_columns.emplace_back(_table_function_result.first[result_idx]->clone_empty());
    }

    //If _outer_column_remain_repeat_times > 0, first use the remaining data of the previous chunk to construct this data
    if (_outer_column_remain_repeat_times > 0) {
        size_t repeat_times = std::min(_outer_column_remain_repeat_times, chunk_size);
        //Build outer data, repeat multiple times
        for (int outer_idx = 0; outer_idx < _outer_slots.size(); ++outer_idx) {
            ColumnPtr& input_column_ptr = _input_chunk_ptr->get_column_by_slot_id(_outer_slots[outer_idx]);
            Datum value = input_column_ptr->get(_input_chunk_seek_rows);
            if (value.is_null()) {
                //The output_columns[outer_idx] is must Nullable, if value has null
                down_cast<NullableColumn*>(output_columns[outer_idx].get())->append_nulls(repeat_times);
            } else {
                output_columns[outer_idx]->append_value_multiple_times(&value, repeat_times);
            }
        }
        //Build table function result
        for (int result_idx = 0; result_idx < _fn_result_slots.size(); ++result_idx) {
            int tvf_offset_start = _table_function_result.second->get(_input_chunk_seek_rows + 1).get_int32() -
                                   _outer_column_remain_repeat_times;
            output_columns[_outer_slots.size() + result_idx]->append(*(_table_function_result.first[result_idx]),
                                                                     tvf_offset_start, repeat_times);
        }

        reserve_chunk_size -= repeat_times;
        _outer_column_remain_repeat_times -= repeat_times;
        if (_outer_column_remain_repeat_times == 0) {
            ++_input_chunk_seek_rows;
        }

        if (reserve_chunk_size == 0) {
            return build_chunk(chunk, output_columns);
        }
    }

    while (true) {
        if (_input_chunk_ptr == nullptr || !_table_function_result_eos) {
            RETURN_IF_ERROR(get_next_input_chunk(state, eos));
            if (*eos) {
                (*eos) = false;
                _input_chunk_ptr = nullptr;
                return build_chunk(chunk, output_columns);
            }
        }

        while (_input_chunk_seek_rows < _input_chunk_ptr->num_rows()) {
            int tvf_result_size = _table_function_result.second->get(_input_chunk_seek_rows + 1).get_int32() -
                                  _table_function_result.second->get(_input_chunk_seek_rows).get_int32();
            int repeat_times = std::min(tvf_result_size, reserve_chunk_size);
            if (repeat_times == 0) {
                ++_input_chunk_seek_rows;
                continue;
            }
            //Build outer data, repeat multiple times
            for (int outer_idx = 0; outer_idx < _outer_slots.size(); ++outer_idx) {
                ColumnPtr& input_column_ptr = _input_chunk_ptr->get_column_by_slot_id(_outer_slots[outer_idx]);
                Datum value = input_column_ptr->get(_input_chunk_seek_rows);
                if (value.is_null()) {
                    //The output_columns[outer_idx] is must Nullable, if value has null
                    down_cast<NullableColumn*>(output_columns[outer_idx].get())->append_nulls(repeat_times);
                } else {
                    output_columns[outer_idx]->append_value_multiple_times(&value, repeat_times);
                }
            }
            //Build table function result
            for (int result_idx = 0; result_idx < _fn_result_slots.size(); ++result_idx) {
                output_columns[_outer_slots.size() + result_idx]->append(
                        *(_table_function_result.first[result_idx]),
                        _table_function_result.second->get(_input_chunk_seek_rows).get_int32(), repeat_times);
            }

            reserve_chunk_size -= repeat_times;

            _outer_column_remain_repeat_times = tvf_result_size - repeat_times;
            if (_outer_column_remain_repeat_times == 0) {
                ++_input_chunk_seek_rows;
            }

            if (reserve_chunk_size == 0) {
                return build_chunk(chunk, output_columns);
            }
        }

        _input_chunk_ptr = nullptr;
    }
}

Status TableFunctionNode::reset(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::reset(state));
    return Status::OK();
}

Status TableFunctionNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    if (_table_function != nullptr && _table_function_state != nullptr) {
        _table_function->close(_table_function_state);
    }
    return ExecNode::close(state);
}

Status TableFunctionNode::build_chunk(ChunkPtr* chunk, const std::vector<ColumnPtr>& output_columns) {
    *chunk = std::make_shared<Chunk>();

    for (int outer_idx = 0; outer_idx < _outer_slots.size(); ++outer_idx) {
        (*chunk)->append_column(output_columns[outer_idx], _outer_slots[outer_idx]);
    }
    for (int result_idx = 0; result_idx < _fn_result_slots.size(); ++result_idx) {
        (*chunk)->append_column(output_columns[_outer_slots.size() + result_idx], _fn_result_slots[result_idx]);
    }

    _num_rows_returned += (*chunk)->num_rows();

    if (reached_limit()) {
        int64_t num_rows_over = _num_rows_returned - _limit;
        (*chunk)->set_num_rows((*chunk)->num_rows() - num_rows_over);
        COUNTER_SET(_rows_returned_counter, _limit);
        return Status::OK();
    }

    COUNTER_SET(_rows_returned_counter, _num_rows_returned);

    return Status::OK();
}

Status TableFunctionNode::get_next_input_chunk(RuntimeState* state, bool* eos) {
    if (!_table_function_result_eos) {
        SCOPED_TIMER(_table_function_exec_timer);
        _table_function_result = _table_function->process(_table_function_state, &_table_function_result_eos);
        return Status::OK();
    }

    do {
        RETURN_IF_ERROR(child(0)->get_next(state, &_input_chunk_ptr, eos));
    } while (!*eos && _input_chunk_ptr->is_empty());

    if (*eos) {
        return Status::OK();
    }

    _input_chunk_seek_rows = 0;
    Columns table_function_params;
    for (SlotId slotId : _param_slots) {
        table_function_params.emplace_back(_input_chunk_ptr->get_column_by_slot_id(slotId));
    }

    _table_function_state->set_params(table_function_params);
    {
        SCOPED_TIMER(_table_function_exec_timer);
        _table_function_result = _table_function->process(_table_function_state, &_table_function_result_eos);
    }
    return Status::OK();
}

std::vector<std::shared_ptr<pipeline::OperatorFactory>> TableFunctionNode::decompose_to_pipeline(
        pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;
    OpFactories operators = _children[0]->decompose_to_pipeline(context);

    operators.emplace_back(std::make_shared<TableFunctionOperatorFactory>(context->next_operator_id(), id(), _tnode));
    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(1, std::move(this->runtime_filter_collector()));
    // Initialize OperatorFactory's fields involving runtime filters.
    this->init_runtime_filter_for_operator(operators.back().get(), context, rc_rf_probe_collector);

    return operators;
}

} // namespace starrocks::vectorized
