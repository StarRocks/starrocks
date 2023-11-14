// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.base;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.analysis.CaseExpr;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ColumnRefFactory {
    private int nextId = 1;
    // The unique id for each scan operator
    // For table a join table a, the two unique ids for table a is different
    private int nextRelationId = 1;
    private final List<ColumnRefOperator> columnRefs = Lists.newArrayList();
    private final Map<Integer, Integer> columnToRelationIds = Maps.newHashMap();
    private final Map<ColumnRefOperator, Column> columnRefToColumns = Maps.newHashMap();
    private final Map<ColumnRefOperator, Table> columnRefToTable = Maps.newHashMap();

    public Map<ColumnRefOperator, Column> getColumnRefToColumns() {
        return columnRefToColumns;
    }

    public ColumnRefOperator create(Expr expression, Type type, boolean nullable) {
        String nameHint = "expr";
        if (expression instanceof SlotRef) {
            nameHint = ((SlotRef) expression).getColumnName();
        } else if (expression instanceof FunctionCallExpr) {
            nameHint = ((FunctionCallExpr) expression).getFnName().toString();
        } else if (expression instanceof CaseExpr) {
            nameHint = "case";
        } else if (expression instanceof CastExpr) {
            nameHint = "cast";
        }
        return create(nextId++, nameHint, type, nullable, false);
    }

    public ColumnRefOperator create(ScalarOperator operator, Type type, boolean nullable) {
        String nameHint = "expr";
        if (operator.isColumnRef()) {
            nameHint = ((ColumnRefOperator) operator).getName();
        } else if (operator instanceof CallOperator) {
            if (operator instanceof CaseWhenOperator) {
                nameHint = "case";
            } else if (operator instanceof CastOperator) {
                nameHint = "cast";
            } else {
                nameHint = ((CallOperator) operator).getFnName();
            }
        }
        return create(nextId++, nameHint, type, nullable, false);
    }

    public ColumnRefOperator create(String name, Type type, boolean nullable) {
        return create(nextId++, name, type, nullable, false);
    }

    public ColumnRefOperator create(String name, Type type, boolean nullable, boolean isLambdaArg) {
        return create(nextId++, name, type, nullable, isLambdaArg);
    }

    private ColumnRefOperator create(int id, String name, Type type, boolean nullable, boolean isLambdaArg) {
        ColumnRefOperator columnRef = new ColumnRefOperator(id, type, name, nullable, isLambdaArg);
        columnRefs.add(columnRef);
        return columnRef;
    }

    public ColumnRefOperator getColumnRef(int id) {
        return columnRefs.get(id - 1);
    }

    public Set<ColumnRefOperator> getColumnRefs(ColumnRefSet columnRefSet) {
        Set<ColumnRefOperator> columnRefOperators = Sets.newHashSet();
        for (int idx : columnRefSet.getColumnIds()) {
            columnRefOperators.add(getColumnRef(idx));
        }
        return columnRefOperators;
    }

    public List<ColumnRefOperator> getColumnRefs() {
        return columnRefs;
    }

    public void updateColumnRefToColumns(ColumnRefOperator columnRef, Column column, Table table) {
        columnRefToColumns.put(columnRef, column);
        columnRefToTable.put(columnRef, table);
    }

    public Column getColumn(ColumnRefOperator columnRef) {
        return columnRefToColumns.get(columnRef);
    }

    public void updateColumnToRelationIds(int columnId, int tableId) {
        columnToRelationIds.put(columnId, tableId);
    }

    public Integer getRelationId(int id) {
        return columnToRelationIds.getOrDefault(id, -1);
    }

    public int getNextRelationId() {
        return nextRelationId++;
    }

    public Map<Integer, Integer> getColumnToRelationIds() {
        return columnToRelationIds;
    }

    public Map<ColumnRefOperator, Table> getColumnRefToTable() {
        return columnRefToTable;
    }

    public Table getTableForColumn(int columnId) {
        return columnRefToTable.get(getColumnRef(columnId));
    }
}
