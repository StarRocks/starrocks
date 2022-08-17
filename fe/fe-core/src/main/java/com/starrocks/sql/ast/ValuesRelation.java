// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;

import java.util.ArrayList;
import java.util.List;

public class ValuesRelation extends QueryRelation {
    private final List<List<Expr>> rows;
    private final List<String> columnOutputNames;

    /*
        isNullValues means a statement without from or from dual, add a single row of null values here,
        so that the semantics are the same, and the processing of subsequent query logic can be simplified,
        such as select sum(1) or select sum(1) from dual, will be converted to select sum(1) from (values(null)) t.
        This can share the same logic as select sum(1) from table
    */
    private boolean isNullValues;

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

    public void setNullValues(boolean nullValues) {
        isNullValues = nullValues;
    }

    public boolean isNullValues() {
        return isNullValues;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitValues(this, context);
    }
}
