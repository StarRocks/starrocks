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

#include "exec/table_function_node.h"

#include "column/chunk.h"
#include "exec/pipeline/exec_node_pipeline_adapter.h"
#include "exec/pipeline/limit_operator.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/pipeline_builder.h"
#include "exec/pipeline/table_function_operator.h"
#include "runtime/runtime_state.h"

namespace starrocks {
TableFunctionNode::TableFunctionNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& desc)
        : PipelineNode(pool, tnode, desc), _tnode(tnode) {}

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

    //Get table function from TableFunctionResolver
    TFunction table_fn = tnode.table_function_node.table_function.nodes[0].fn;
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

    return Status::OK();
}

Status TableFunctionNode::reset(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::reset(state));
    return Status::OK();
}

void TableFunctionNode::close(RuntimeState* state) {
    if (is_closed()) {
        return;
    }
    if (_table_function != nullptr && _table_function_state != nullptr) {
        (void)_table_function->close(state, _table_function_state);
    }
    ExecNode::close(state);
}

StatusOr<pipeline::OpFactories> TableFunctionNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    using namespace pipeline;
    ASSIGN_OR_RETURN(auto operators, _children[0]->decompose_to_pipeline(context));

    operators.emplace_back(std::make_shared<TableFunctionOperatorFactory>(context->next_operator_id(), id(), _tnode));
    // Create a shared RefCountedRuntimeFilterCollector
    auto&& rc_rf_probe_collector = std::make_shared<RcRfProbeCollector>(1, std::move(this->runtime_filter_collector()));
    // Initialize OperatorFactory's fields involving runtime filters.
    pipeline::init_runtime_filter_for_operator(*this, operators.back().get(), context, rc_rf_probe_collector);
    if (limit() != -1) {
        operators.emplace_back(std::make_shared<LimitOperatorFactory>(context->next_operator_id(), id(), limit()));
    }

    return operators;
}

} // namespace starrocks
