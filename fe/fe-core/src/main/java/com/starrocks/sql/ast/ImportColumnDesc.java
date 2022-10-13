// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;

public class ImportColumnDesc {
    private String columnName;
    private Expr expr;

    public ImportColumnDesc(ImportColumnDesc other) {
        this.columnName = other.columnName;
        if (other.expr != null) {
            this.expr = other.expr.clone();
        }
    }

    public ImportColumnDesc(String column) {
        this.columnName = column;
    }

    public ImportColumnDesc(String column, Expr expr) {
        this.columnName = column;
        this.expr = expr;
    }

    public void reset(String column, Expr expr) {
        this.columnName = column;
        this.expr = expr;
    }

    public String getColumnName() {
        return columnName;
    }

    public Expr getExpr() {
        return expr;
    }

    public boolean isColumn() {
        return expr == null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(columnName);
        if (expr != null) {
            sb.append("=").append(expr.toSql());
        }
        return sb.toString();
    }
}
