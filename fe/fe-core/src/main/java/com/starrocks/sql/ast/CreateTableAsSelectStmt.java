// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
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
    private final QueryStatement queryStatement;
    private final InsertStmt insertStmt;

    public CreateTableAsSelectStmt(CreateTableStmt createTableStmt,
                                   List<String> columnNames,
                                   QueryStatement queryStatement) {
        this.createTableStmt = createTableStmt;
        this.columnNames = columnNames;
        this.queryStatement = queryStatement;
        this.insertStmt = new InsertStmt(createTableStmt.getDbTbl(), queryStatement);
    }

    public void createTable(ConnectContext session) throws AnalysisException {
        try {
            session.getGlobalStateMgr().createTable(createTableStmt);
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }
    }

    public void dropTable(ConnectContext session) throws AnalysisException {
        try {
            session.getGlobalStateMgr().dropTable(new DropTableStmt(true, createTableStmt.getDbTbl(), true));
        } catch (DdlException e) {
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
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateTableAsSelectStatement(this, context);
    }
}
