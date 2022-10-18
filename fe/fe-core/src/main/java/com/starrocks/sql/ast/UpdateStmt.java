// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Table;

import java.util.List;

public class UpdateStmt extends DmlStmt {
    private final TableName tableName;
    private final List<ColumnAssignment> assignments;
    private final Expr wherePredicate;

    private Table table;
    private QueryStatement queryStatement;

    public UpdateStmt(TableName tableName, List<ColumnAssignment> assignments, Expr wherePredicate) {
        this.tableName = tableName;
        this.assignments = assignments;
        this.wherePredicate = wherePredicate;
    }

    @Override
    public TableName getTableName() {
        return tableName;
    }

    public List<ColumnAssignment> getAssignments() {
        return assignments;
    }

    public Expr getWherePredicate() {
        return wherePredicate;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public Table getTable() {
        return table;
    }

    public void setQueryStatement(QueryStatement queryStatement) {
        this.queryStatement = queryStatement;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUpdateStatement(this, context);
    }
}
