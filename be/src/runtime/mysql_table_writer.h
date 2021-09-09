// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/mysql_table_writer.h

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

#ifndef STARROCKS_BE_RUNTIME_MYSQL_TABLE_WRITER_H
#define STARROCKS_BE_RUNTIME_MYSQL_TABLE_WRITER_H

#include <string>
#include <vector>

#include "common/status.h"

#ifndef __StarRocksMysql
#define __StarRocksMysql void
#endif

namespace starrocks {

struct MysqlConnInfo {
    std::string host;
    std::string user;
    std::string passwd;
    std::string db;
    int port = 0;

    std::string debug_string() const;
};

class RowBatch;
class TupleRow;
class ExprContext;

class MysqlTableWriter {
public:
    MysqlTableWriter(const std::vector<ExprContext*>& output_exprs);
    ~MysqlTableWriter();

    // connnect to mysql server
    Status open(const MysqlConnInfo& conn_info, const std::string& tbl);

    Status begin_trans() { return Status::OK(); }

    Status append(RowBatch* batch);

    Status abort_tarns() { return Status::OK(); }

    Status finish_tarns() { return Status::OK(); }

private:
    Status insert_row(TupleRow* row);

    const std::vector<ExprContext*>& _output_expr_ctxs;
    std::string _mysql_tbl;
    __StarRocksMysql* _mysql_conn;
};

} // namespace starrocks

#endif
