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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/mysql_table_sink.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/mysql_table_sink.h"

#include <memory>
#include <sstream>

#include "exprs/expr.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace starrocks {

const int MYSQL_SINK_BATCH_SIZE = 1024;

MysqlTableSink::MysqlTableSink(ObjectPool* pool, const RowDescriptor& row_desc, const std::vector<TExpr>& t_exprs)
        : _pool(pool), _t_output_expr(t_exprs) {}

MysqlTableSink::~MysqlTableSink() = default;

Status MysqlTableSink::init(const TDataSink& t_sink, RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::init(t_sink, state));
    const TMysqlTableSink& t_mysql_sink = t_sink.mysql_table_sink;

    _conn_info.host = t_mysql_sink.host;
    _conn_info.port = t_mysql_sink.port;
    _conn_info.user = t_mysql_sink.user;
    _conn_info.passwd = t_mysql_sink.passwd;
    _conn_info.db = t_mysql_sink.db;
    _mysql_tbl = t_mysql_sink.table;
    _chunk_size = MYSQL_SINK_BATCH_SIZE;

    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, _t_output_expr, &_output_expr_ctxs, state));
    return Status::OK();
}

Status MysqlTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    std::stringstream title;
    title << "MysqlTableSink (frag_id=" << state->fragment_instance_id() << ")";
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(title.str()));
    return Status::OK();
}

Status MysqlTableSink::open(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));
    // create writer
    _writer = std::make_unique<MysqlTableWriter>(_output_expr_ctxs, _chunk_size);
    RETURN_IF_ERROR(_writer->open(_conn_info, _mysql_tbl));
    return Status::OK();
}

Status MysqlTableSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    return _writer->append(chunk);
}

Status MysqlTableSink::close(RuntimeState* state, Status exec_status) {
    Expr::close(_output_expr_ctxs, state);
    return Status::OK();
}

} // namespace starrocks
