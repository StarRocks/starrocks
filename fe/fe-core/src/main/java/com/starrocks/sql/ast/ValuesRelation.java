// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;

import java.util.ArrayList;
import java.util.List;

public class ValuesRelation extends QueryRelation {
    private final List<List<Expr>> rows;
    private final List<String> columnOutputNames;

    public ValuesRelation(List<ArrayList<Expr>> rows, List<String> columnOutputNames) {
        this.rows = new ArrayList<>(rows);
        this.columnOutputNames = columnOutputNames;
    }

    public void addRow(ArrayList<Expr> row) {
        this.rows.add(row);
    }

    public List<Expr> getRow(int rowIdx) {
        return rows.get(rowIdx);
    }

    public List<List<Expr>> getRows() {
        return rows;
    }

    @Override
    public List<String> getColumnOutputNames() {
        return columnOutputNames;
    }

    @Override
    public List<Expr> getOutputExpression() {
        return rows.get(0);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitValues(this, context);
    }
}
