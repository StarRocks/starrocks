// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.analysis;

import com.starrocks.catalog.Table;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.ColumnAssignment;
import com.starrocks.sql.ast.SelectRelation;

import java.util.List;

public class UpdateStmt extends DmlStmt {
    private final TableName tableName;
    private final List<ColumnAssignment> assignments;
    private final Expr wherePredicate;

    private Table table;
    private SelectRelation updateRelation;

    public UpdateStmt(TableName tableName, List<ColumnAssignment> assignments, Expr wherePredicate) {
        this.tableName = tableName;
        this.assignments = assignments;
        this.wherePredicate = wherePredicate;
    }

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

    public void setUpdateRelation(SelectRelation updateRelation) {
        this.updateRelation = updateRelation;
    }

    public SelectRelation getUpdateRelation() {
        return updateRelation;
    }

    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUpdateStatement(this, context);
    }
}
