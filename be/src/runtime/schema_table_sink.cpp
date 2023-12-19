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

#include "runtime/schema_table_sink.h"

#include <memory>
#include <sstream>

#include "agent/master_info.h"
#include "column/chunk.h"
#include "column/datum.h"
#include "common/configbase.h"
#include "exec/tablet_info.h"
#include "exprs/expr.h"
#include "gutil/strings/substitute.h"
#include "http/action/update_config_action.h"
#include "runtime/runtime_state.h"
#include "util/brpc_stub_cache.h"
#include "util/defer_op.h"
#include "util/ref_count_closure.h"
#include "util/runtime_profile.h"
#include "util/stack_util.h"

namespace starrocks {

SchemaTableSink::SchemaTableSink(ObjectPool* pool, const RowDescriptor& row_desc, const std::vector<TExpr>& t_exprs)
        : _pool(pool), _t_output_expr(t_exprs) {}

SchemaTableSink::~SchemaTableSink() = default;

Status SchemaTableSink::init(const TDataSink& t_sink, RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::init(t_sink, state));
    const auto& schema_table_sink = t_sink.schema_table_sink;
    _table_name = schema_table_sink.table;

    auto o_id = get_backend_id();
    _be_id = o_id.has_value() ? o_id.value() : -1;
    _nodes_info = std::make_unique<StarRocksNodesInfo>(schema_table_sink.nodes_info);

    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, _t_output_expr, &_output_expr_ctxs, state));
    return Status::OK();
}

Status SchemaTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    _profile =
            state->obj_pool()->add(new RuntimeProfile(strings::Substitute("SchemaTableSink (table=$0)", _table_name)));
    return Status::OK();
}

Status SchemaTableSink::open(RuntimeState* state) {
    return Expr::open(_output_expr_ctxs, state);
}

static Status set_config_remote(const StarRocksNodesInfo& nodes_info, int64_t be_id, const string& name,
                                const string& value) {
    auto node_info = nodes_info.find_node(be_id);
    if (node_info == nullptr) {
        return Status::InternalError(strings::Substitute("set_config fail: be $0 not found", be_id));
    }
    PInternalService_Stub* stub =
            ExecEnv::GetInstance()->brpc_stub_cache()->get_stub(node_info->host, node_info->brpc_port);
    if (stub == nullptr) {
        return Status::InternalError(strings::Substitute("set_config fail to get brpc stub for $0:$1", node_info->host,
                                                         node_info->brpc_port));
    }
    ExecuteCommandRequestPB request;
    request.set_command("set_config");
    request.set_params(strings::Substitute(R"({"name":"$0","value":"$1"})", name, value));
    auto* closure = new RefCountClosure<ExecuteCommandResultPB>();
    closure->cntl.set_timeout_ms(10000);
    closure->ref();
    DeferOp op([&]() {
        if (closure->unref()) {
            delete closure;
            closure = nullptr;
        }
    });
    stub->execute_command(&closure->cntl, &request, &closure->result, closure);
    closure->join();
    if (closure->cntl.Failed()) {
        return Status::InternalError(closure->cntl.ErrorText());
    }
    auto& result = closure->result;
    Status st = result.status();
    if (!st.ok()) {
        LOG(WARNING) << strings::Substitute("set_config_remote failed be:$0 name:$1 value:$2 ret:$3 st:$4", be_id, name,
                                            value, result.result(), st.to_string());
        return st;
    }
    return Status::OK();
}

static Status write_be_configs_table(const StarRocksNodesInfo& nodes_info, int64_t self_be_id, Columns& columns) {
    if (columns.size() < 3) {
        return Status::InternalError("write be_configs table should have at least 3 columns");
    }
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
        string mode;
        Status s;
        if (be_id == -1) {
            LOG(INFO) << strings::Substitute("set_config ignored: be_id=-1 name:$0 value:$1", name, value);
            continue;
        } else if (self_be_id == be_id) {
            s = update_config->update_config(name, value);
            mode = "local";
        } else {
            s = set_config_remote(nodes_info, be_id, name, value);
            mode = strings::Substitute("remote be:$0", be_id);
        }
        if (s.ok()) {
            LOG(INFO) << "set_config " << mode << " " << name << "=" << value << " success";
        } else {
            LOG(WARNING) << "set_config " << mode << " " << name << "=" << value << " failed " << s.to_string();
        }
        ret.update(s);
    }
    return ret;
}

Status SchemaTableSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    Columns result_columns(_output_expr_ctxs.size());
    for (int i = 0; i < _output_expr_ctxs.size(); ++i) {
        ASSIGN_OR_RETURN(result_columns[i], _output_expr_ctxs[i]->evaluate(chunk));
    }
    if (_table_name == "be_configs") {
        return write_be_configs_table(*_nodes_info, _be_id, result_columns);
    }
    return Status::OK();
}

Status SchemaTableSink::close(RuntimeState* state, Status exec_status) {
    Expr::close(_output_expr_ctxs, state);
    return Status::OK();
}

} // namespace starrocks
