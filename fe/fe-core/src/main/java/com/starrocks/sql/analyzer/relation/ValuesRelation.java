// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer.relation;

import com.starrocks.analysis.Expr;

import java.util.ArrayList;
import java.util.List;

public class ValuesRelation extends QueryRelation {
    private final List<List<Expr>> rows;

    public ValuesRelation(List<ArrayList<Expr>> rows, List<String> columnOutputNames) {
        super(rows.get(0), columnOutputNames);
        this.rows = new ArrayList<>(rows);
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

    public <R, C> R accept(RelationVisitor<R, C> visitor, C context) {
        return visitor.visitValues(this, context);
    }
}
