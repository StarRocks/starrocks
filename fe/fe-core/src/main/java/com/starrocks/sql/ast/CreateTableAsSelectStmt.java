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


package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.exception.DdlException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.parser.NodePosition;

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
    private final QueryStatement queryStatement;
    private final InsertStmt insertStmt;

    public CreateTableAsSelectStmt(CreateTableStmt createTableStmt,
                                   List<String> columnNames,
                                   QueryStatement queryStatement) {
        this(createTableStmt, columnNames, queryStatement, NodePosition.ZERO);
    }

    public CreateTableAsSelectStmt(CreateTableStmt createTableStmt,
                                   List<String> columnNames,
                                   QueryStatement queryStatement, NodePosition pos) {
        super(pos);
        this.createTableStmt = createTableStmt;
        this.columnNames = columnNames;
        this.queryStatement = queryStatement;
        this.insertStmt = new InsertStmt(createTableStmt.getDbTbl(), queryStatement);
    }

    public boolean createTable(ConnectContext session) throws AnalysisException {
        try {
            return session.getGlobalStateMgr().getMetadataMgr().createTable(createTableStmt);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    public void dropTable(ConnectContext session) throws AnalysisException {
        try {
            session.getGlobalStateMgr().getMetadataMgr().dropTable(new DropTableStmt(true, createTableStmt.getDbTbl(), true));
        } catch (Exception e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
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

    @Override
    public String toSql() {
        return createTableStmt.toSql() + " AS " + queryStatement.toSql();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateTableAsSelectStatement(this, context);
    }
}
