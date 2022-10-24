// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InfoSchemaDb;
import com.starrocks.catalog.ScalarType;
import com.starrocks.common.AnalysisException;
import com.starrocks.qe.ShowResultSetMetaData;

// Show rollup statement, used to show rollup information of one table.
//
// Syntax:
//      SHOW MATERIALIZED VIEW { FROM | IN } db
public class ShowMaterializedViewStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("id", ScalarType.createVarchar(50)))
                    .addColumn(new Column("name", ScalarType.createVarchar(50)))
                    .addColumn(new Column("database_name", ScalarType.createVarchar(20)))
                    .addColumn(new Column("text", ScalarType.createVarchar(1024)))
                    .addColumn(new Column("rows", ScalarType.createVarchar(50)))
                    .build();

    private static final TableName TABLE_NAME = new TableName(InfoSchemaDb.DATABASE_NAME, "materialized_views");

    private String db;

    private final String pattern;

    private Expr where;

    public ShowMaterializedViewStmt(String db) {
        this.db = db;
        this.pattern = null;
        this.where = null;
    }

    public ShowMaterializedViewStmt(String db, String pattern) {
        this.db = db;
        this.pattern = pattern;
        this.where = null;
    }

    public ShowMaterializedViewStmt(String db, Expr where) {
        this.db = db;
        this.pattern = null;
        this.where = where;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public QueryStatement toSelectStmt() throws AnalysisException {
        if (where == null) {
            return null;
        }
        // Columns
        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap(false);
        // id
        SelectListItem item = new SelectListItem(new SlotRef(TABLE_NAME, "MATERIALIZED_VIEW_ID"), "id");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "id"), item.getExpr().clone(null));
        // name
        item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE_NAME"), "name");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "name"), item.getExpr().clone(null));
        // database_name
        item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE_SCHEMA"), "database_name");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "database_name"), item.getExpr().clone(null));
        // text
        item = new SelectListItem(new SlotRef(TABLE_NAME, "MATERIALIZED_VIEW_DEFINITION"), "text");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "text"), item.getExpr().clone(null));
        // rows
        item = new SelectListItem(new SlotRef(TABLE_NAME, "TABLE_ROWS"), "rows");
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, "rows"), item.getExpr().clone(null));
        where = where.substitute(aliasMap);
        return new QueryStatement(new SelectRelation(selectList, new TableRelation(TABLE_NAME),
                where, null, null));
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowMaterializedViewStatement(this, context);
    }
}
