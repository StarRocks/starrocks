// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InfoSchemaDb;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

// Show database statement.
public class ShowDbStmt extends ShowStmt {
    private static final TableName TABLE_NAME = new TableName(InfoSchemaDb.DATABASE_NAME, "schemata");
    private static final String DB_COL = "Database";
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column(DB_COL, ScalarType.createVarchar(20)))
                    .build();
    private final String pattern;
    private Expr where;

    private String catalogName;

    public ShowDbStmt(String pattern) {
        this.pattern = pattern;
    }

    public ShowDbStmt(String pattern, Expr where) {
        this.pattern = pattern;
        this.where = where;
    }

    public ShowDbStmt(String pattern, String catalogName) {
        this.pattern = pattern;
        this.catalogName = catalogName;
    }

    public ShowDbStmt(String pattern, Expr where, String catalogName) {
        this.pattern = pattern;
        this.where = where;
        this.catalogName = catalogName;
    }

    public String getPattern() {
        return pattern;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    @Override
    public QueryStatement toSelectStmt() {
        if (where == null) {
            return null;
        }
        // Columns
        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap(false);
        SelectListItem item = new SelectListItem(new SlotRef(TABLE_NAME, "SCHEMA_NAME"), DB_COL);
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, DB_COL), item.getExpr().clone(null));
        where = where.substitute(aliasMap);
        return new QueryStatement(new SelectRelation(selectList, new TableRelation(TABLE_NAME),
                where, null, null));
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowDatabasesStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
