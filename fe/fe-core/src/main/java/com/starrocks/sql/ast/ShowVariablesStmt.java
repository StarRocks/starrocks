// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
import com.starrocks.analysis.SetType;
import com.starrocks.analysis.ShowStmt;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InfoSchemaDb;
import com.starrocks.catalog.ScalarType;
import com.starrocks.qe.ShowResultSetMetaData;

// Show variables statement.
public class ShowVariablesStmt extends ShowStmt {
    private static final String NAME_COL = "Variable_name";
    private static final String VALUE_COL = "Value";
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column(NAME_COL, ScalarType.createVarchar(20)))
                    .addColumn(new Column(VALUE_COL, ScalarType.createVarchar(20)))
                    .build();

    private SetType type;
    private final String pattern;
    private Expr where;

    public ShowVariablesStmt(SetType type, String pattern) {
        this.type = type;
        this.pattern = pattern;
    }

    public ShowVariablesStmt(SetType type, String pattern, Expr where) {
        this.type = type;
        this.pattern = pattern;
        this.where = where;
    }

    public SetType getType() {
        return type;
    }

    public void setType(SetType type) {
        this.type = type;
    }

    public String getPattern() {
        return pattern;
    }

    @Override
    public QueryStatement toSelectStmt() {
        if (where == null) {
            return null;
        }
        if (type == null) {
            type = SetType.DEFAULT;
        }
        // Columns
        SelectList selectList = new SelectList();
        ExprSubstitutionMap aliasMap = new ExprSubstitutionMap(false);
        TableName tableName;
        if (type == SetType.GLOBAL) {
            tableName = new TableName(InfoSchemaDb.DATABASE_NAME, "GLOBAL_VARIABLES");
        } else {
            tableName = new TableName(InfoSchemaDb.DATABASE_NAME, "SESSION_VARIABLES");
        }
        // name
        SelectListItem item = new SelectListItem(new SlotRef(tableName, "VARIABLE_NAME"), NAME_COL);
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, NAME_COL), item.getExpr().clone(null));
        // value
        item = new SelectListItem(new SlotRef(tableName, "VARIABLE_VALUE"), VALUE_COL);
        selectList.addItem(item);
        aliasMap.put(new SlotRef(null, VALUE_COL), item.getExpr().clone(null));
        // change
        where = where.substitute(aliasMap);

        return new QueryStatement(new SelectRelation(selectList, new TableRelation(tableName),
                where, null, null));
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowVariablesStmt(this, context);
    }

    @Override
    public boolean isSupportNewPlanner() {
        return true;
    }
}
