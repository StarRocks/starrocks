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

#include "agent/master_info.h"
#include "common/configbase.h"
#include "exec/pipeline/pipeline_driver_executor.h"
#include "exec/pipeline/sink/schema_table_sink_opeartor.h"
#include "exprs/expr_context.h"
#include "http/action/update_config_action.h"

namespace starrocks::pipeline {
Status SchemaTableSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    return Status::OK();
}

void SchemaTableSinkOperator::close(RuntimeState* state) {
    Operator::close(state);
}

bool SchemaTableSinkOperator::need_input() const {
    return !_is_finished;
}

bool SchemaTableSinkOperator::is_finished() const {
    return _is_finished;
}

Status SchemaTableSinkOperator::set_finishing(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

bool SchemaTableSinkOperator::pending_finish() const {
    return false;
}

Status SchemaTableSinkOperator::set_cancelled(RuntimeState* state) {
    _is_finished = true;
    return Status::OK();
}

StatusOr<ChunkPtr> SchemaTableSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from SchemaTableSinkOperator");
}

Status SchemaTableSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    Columns result_columns(_output_expr_ctxs.size());
    for (int i = 0; i < _output_expr_ctxs.size(); ++i) {
        ASSIGN_OR_RETURN(result_columns[i], _output_expr_ctxs[i]->evaluate(chunk.get()));
    }
    if (_table_name == "be_configs") {
        return write_be_configs_table(result_columns);
    }
    return Status::InternalError("only support updating table be_config");
}

Status SchemaTableSinkOperator::write_be_configs_table(Columns& columns) {
    if (columns.size() < 3) {
        return Status::InternalError("write be_configs table should have at least 3 columns");
    }

    auto o_id = get_backend_id();
    auto self_be_id = o_id.has_value() ? o_id.value() : -1;

    auto update_config = UpdateConfigAction::instance();
    if (update_config == nullptr) {
        LOG(WARNING) << "write_be_configs_table ignored: UpdateConfigAction is not inited";
        return Status::OK();
    }
    Status ret;
    for (size_t i = 0; i < columns[0]->size(); ++i) {
        int64_t be_id = columns[0]->get(i).get_int64();
        const auto& name = columns[1]->get(i).get_slice().to_string();
        const auto& value = columns[2]->get(i).get_slice().to_string();

        StatusOr<std::string> readStatus = config::get_config(name);
        if (!readStatus.ok()) {
            LOG(WARNING) << "set_config "
                         << " " << name << "=" << value << " failed " << readStatus.status().to_string();
            return readStatus.status();
        }

        Status updateStatus;
        if (be_id == -1) {
            LOG(INFO) << strings::Substitute("set_config ignored: be_id=-1 name:$0 value:$1", name, value);
            continue;
        } else if (be_id == self_be_id) {
            updateStatus = update_config->update_config(name, value);
        } else {
            continue;
        }
        if (updateStatus.ok()) {
            LOG(INFO) << "set_config "
                      << " " << name << " from " << readStatus.value() << " to " << value << " success";
        } else {
            LOG(WARNING) << "set_config "
                         << " " << name << "=" << value << " failed " << updateStatus.to_string();
        }
        ret.update(updateStatus);
    }
    return ret;
}

SchemaTableSinkOperatorFactory::SchemaTableSinkOperatorFactory(int32_t id, std::vector<TExpr> t_output_expr,
                                                               const TSchemaTableSink& t_schema_table_sink,
                                                               FragmentContext* const fragment_ctx)
        : OperatorFactory(id, "schema_table_sink", Operator::s_pseudo_plan_node_id_for_final_sink),
          _t_output_expr(std::move(t_output_expr)),
          _table_name(t_schema_table_sink.table) {}

Status SchemaTableSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));

    return Status::OK();
}

void SchemaTableSinkOperatorFactory::close(RuntimeState* state) {
    Expr::close(_output_expr_ctxs, state);
    OperatorFactory::close(state);
}
} // namespace starrocks::pipeline