// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ParseNode;

public class ColumnAssignment implements ParseNode {
    private final String column;
    private final Expr expr;

    public ColumnAssignment(String column, Expr expr) {
        this.column = column;
        this.expr = expr;
    }

    public String getColumn() {
        return column;
    }

    public Expr getExpr() {
        return expr;
    }
}
