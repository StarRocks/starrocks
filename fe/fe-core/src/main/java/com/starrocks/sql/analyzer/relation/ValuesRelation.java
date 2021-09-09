// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.analyzer.relation;

import com.starrocks.analysis.Expr;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ValuesRelation extends QueryRelation {
    private final List<List<Expr>> rows;

    public ValuesRelation(List<ArrayList<Expr>> rows, RelationFields relationFields) {
        super(rows.get(0), new Scope(RelationId.anonymous(), relationFields),
                relationFields.getAllFields().stream().map(Field::getName).collect(Collectors.toList()));
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
