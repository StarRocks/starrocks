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

#include "exec/data_sinks/hive_table_sink.h"

#include "common/runtime_profile.h"
#include "exprs/expr_executor.h"
#include "runtime/runtime_state.h"

namespace starrocks {

HiveTableSink::HiveTableSink(ObjectPool* pool, const std::vector<TExpr>& t_exprs)
        : _pool(pool), _t_output_expr(t_exprs) {}

HiveTableSink::~HiveTableSink() = default;

Status HiveTableSink::init(const TDataSink& thrift_sink, RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::init(thrift_sink, state));
    RETURN_IF_ERROR(prepare(state));
    RETURN_IF_ERROR(open(state));
    return Status::OK();
}

Status HiveTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    RETURN_IF_ERROR(ExprExecutor::prepare(_output_expr_ctxs, state));
    std::stringstream title;
    title << "IcebergTableSink (frag_id=" << state->fragment_instance_id() << ")";
    _profile = _pool->add(new RuntimeProfile(title.str()));
    return Status::OK();
}

Status HiveTableSink::open(RuntimeState* state) {
    RETURN_IF_ERROR(ExprExecutor::open(_output_expr_ctxs, state));
    return Status::OK();
}

Status HiveTableSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    return Status::OK();
}

Status HiveTableSink::close(RuntimeState* state, const Status& exec_status) {
    ExprExecutor::close(_output_expr_ctxs, state);
    return Status::OK();
}

} // namespace starrocks
