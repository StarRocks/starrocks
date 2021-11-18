// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/CreateTableAsSelectStmt.java

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

package com.starrocks.analysis;

import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.common.UserException;
import com.starrocks.qe.ConnectContext;

import java.util.List;

/**
 * Represents a CREATE TABLE AS SELECT (CTAS) statement
 * Syntax:
 * CREATE TABLE table_name [( column_name_list )]
 * opt_engine opt_partition opt_properties KW_AS query_stmt
 */
public class CreateTableAsSelectStmt extends StatementBase {
    private final CreateTableStmt createTableStmt;
    private final List<String> columnNames;
    private final QueryStmt queryStmt;
    private final InsertStmt insertStmt;

    public CreateTableAsSelectStmt(CreateTableStmt createTableStmt,
                                   List<String> columnNames,
                                   QueryStmt queryStmt) {
        this.createTableStmt = createTableStmt;
        this.columnNames = columnNames;
        this.queryStmt = queryStmt;
        this.insertStmt = new InsertStmt(createTableStmt.getDbTbl(), queryStmt);
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        throw new AnalysisException("old planner does not support CTAS statement");
    }

    public void createTable(ConnectContext session) throws AnalysisException {
        try {
            session.getCatalog().createTable(createTableStmt);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    public void dropTable(ConnectContext session) throws AnalysisException {
        try {
            session.getCatalog().dropTable(new DropTableStmt(true, createTableStmt.getDbTbl(), true));
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    public void createTable(Analyzer analyzer) throws AnalysisException {
        // TODO(zc): Support create table later.
        // Create table
        try {
            analyzer.getCatalog().createTable(createTableStmt);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public QueryStmt getQueryStmt() {
        return queryStmt;
    }

    public CreateTableStmt getCreateTableStmt() {
        return createTableStmt;
    }

    public InsertStmt getInsertStmt() {
        return insertStmt;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }
}
