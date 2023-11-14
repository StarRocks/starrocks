// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Table;

import java.util.List;

public class UpdateStmt extends DmlStmt {
    private final TableName tableName;
    private final List<ColumnAssignment> assignments;
    private final List<Relation> fromRelations;
    private final Expr wherePredicate;
    private final List<CTERelation> commonTableExpressions;

    private Table table;
    private QueryStatement queryStatement;

    public UpdateStmt(TableName tableName, List<ColumnAssignment> assignments, List<Relation> fromRelations,
                      Expr wherePredicate, List<CTERelation> commonTableExpressions) {
        this.tableName = tableName;
        this.assignments = assignments;
        this.fromRelations = fromRelations;
        this.wherePredicate = wherePredicate;
        this.commonTableExpressions = commonTableExpressions;
    }

    @Override
    public TableName getTableName() {
        return tableName;
    }

    public List<ColumnAssignment> getAssignments() {
        return assignments;
    }

    public List<Relation> getFromRelations() {
        return fromRelations;
    }

    public Expr getWherePredicate() {
        return wherePredicate;
    }

    public List<CTERelation> getCommonTableExpressions() {
        return commonTableExpressions;
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
