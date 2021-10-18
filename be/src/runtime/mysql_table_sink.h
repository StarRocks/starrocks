// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/mysql_table_sink.h

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

#ifndef STARROCKS_BE_RUNTIME_MYSQL_TABLE_SINK_H
#define STARROCKS_BE_RUNTIME_MYSQL_TABLE_SINK_H

#include <vector>

#include "common/status.h"
#include "exec/data_sink.h"
#include "runtime/mysql_table_writer.h"

namespace starrocks {

class RowDescriptor;
class TExpr;
class TMysqlTableSink;
class RuntimeState;
class RuntimeProfile;
class ExprContext;
class MemTracker;

// This class is a sinker, which put input data to mysql table
class MysqlTableSink : public DataSink {
public:
    MysqlTableSink(ObjectPool* pool, const RowDescriptor& row_desc, const std::vector<TExpr>& t_exprs);

    ~MysqlTableSink() override;

    Status init(const TDataSink& thrift_sink) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    // Flush all buffered data and close all existing channels to destination
    // hosts. Further send() calls are illegal after calling close().
    Status close(RuntimeState* state, Status exec_status) override;

    RuntimeProfile* profile() override { return _profile; }

private:
    // owned by RuntimeState
    ObjectPool* _pool;
    const RowDescriptor& _row_desc;
    const std::vector<TExpr>& _t_output_expr;

    std::vector<ExprContext*> _output_expr_ctxs;
    MysqlConnInfo _conn_info;
    std::string _mysql_tbl;
    MysqlTableWriter* _writer = nullptr;

    RuntimeProfile* _profile = nullptr;
    std::unique_ptr<MemTracker> _mem_tracker;
};

} // namespace starrocks

#endif
