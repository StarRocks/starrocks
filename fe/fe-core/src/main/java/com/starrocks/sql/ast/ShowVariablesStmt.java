// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ExprSubstitutionMap;
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
    private static final String DEFAULT_VALUE = "Default_value";
    private static final String IS_CHANGED = "Is_changed";
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column(NAME_COL, ScalarType.createVarchar(20)))
                    .addColumn(new Column(VALUE_COL, ScalarType.createVarchar(20)))
                    .build();

    private static final ShowResultSetMetaData VERBOSE_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column(NAME_COL, ScalarType.createVarchar(20)))
                    .addColumn(new Column(VALUE_COL, ScalarType.createVarchar(20)))
                    .addColumn(new Column(DEFAULT_VALUE, ScalarType.createVarchar(20)))
                    .addColumn(new Column(IS_CHANGED, ScalarType.createVarchar(20)))
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
        if (type == SetType.VERBOSE) {
            // default_value
            item = new SelectListItem(new SlotRef(tableName, DEFAULT_VALUE), DEFAULT_VALUE);
            selectList.addItem(item);
            aliasMap.put(new SlotRef(null, DEFAULT_VALUE), item.getExpr().clone(null));
            // is_changed
            item = new SelectListItem(new SlotRef(tableName, IS_CHANGED), IS_CHANGED);
            selectList.addItem(item);
            aliasMap.put(new SlotRef(null, IS_CHANGED), item.getExpr().clone(null));
        }

        where = where.substitute(aliasMap);

        return new QueryStatement(new SelectRelation(selectList, new TableRelation(tableName),
                where, null, null));
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return type != SetType.VERBOSE ? META_DATA : VERBOSE_META_DATA;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowVariablesStatement(this, context);
    }
}
