// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.operator;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.AddDecodeNodeForDictStringRule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Projection {
    private final Map<ColumnRefOperator, ScalarOperator> columnRefMap;
    // Used for common operator compute result reuse, we need to compute
    // common sub operators firstly in BE
    private final Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap;

    public Projection(Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
        this.columnRefMap = columnRefMap;
        this.commonSubOperatorMap = new HashMap<>();
    }

    public Projection(Map<ColumnRefOperator, ScalarOperator> columnRefMap,
                      Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap) {
        this.columnRefMap = columnRefMap;
        if (commonSubOperatorMap == null) {
            this.commonSubOperatorMap = new HashMap<>();
        } else {
            this.commonSubOperatorMap = commonSubOperatorMap;
        }
    }

    public List<ColumnRefOperator> getOutputColumns() {
        return new ArrayList<>(columnRefMap.keySet());
    }

    public Map<ColumnRefOperator, ScalarOperator> getColumnRefMap() {
        return columnRefMap;
    }

    public Map<ColumnRefOperator, ScalarOperator> getCommonSubOperatorMap() {
        return commonSubOperatorMap;
    }

    // For sql: select *, to_bitmap(S_SUPPKEY) from table, we needn't apply global dict optimization
    // This method differ from `couldApplyStringDict` method is for ColumnRefOperator, we return false.
    public boolean needApplyStringDict(Set<Integer> childDictColumns) {
        Preconditions.checkState(!childDictColumns.isEmpty());
        ColumnRefSet dictSet = new ColumnRefSet();
        for (Integer id : childDictColumns) {
            dictSet.union(id);
        }

        for (ScalarOperator operator : columnRefMap.values()) {
            if (!operator.isColumnRef() && couldApplyStringDict(operator, dictSet)) {
                return true;
            }
        }

        for (ScalarOperator operator : commonSubOperatorMap.values()) {
            if (!operator.isColumnRef() && couldApplyStringDict(operator, dictSet)) {
                return true;
            }
        }
        return false;
    }

    public boolean couldApplyStringDict(Set<Integer> childDictColumns) {
        Preconditions.checkState(!childDictColumns.isEmpty());
        ColumnRefSet dictSet = new ColumnRefSet();
        for (Integer id : childDictColumns) {
            dictSet.union(id);
        }

        for (ScalarOperator operator : columnRefMap.values()) {
            if (couldApplyStringDict(operator, dictSet)) {
                return true;
            }
        }

        for (ScalarOperator operator : commonSubOperatorMap.values()) {
            if (couldApplyStringDict(operator, dictSet)) {
                return true;
            }
        }

        return false;
    }

    public static boolean couldApplyDictOptimize(ScalarOperator operator) {
        return operator.getUsedColumns().cardinality() == 1 &&
                operator.accept(new AddDecodeNodeForDictStringRule.CouldApplyDictOptimizeVisitor(), null);
    }

    private boolean couldApplyStringDict(ScalarOperator operator, ColumnRefSet dictSet) {
        ColumnRefSet usedColumns = operator.getUsedColumns();
        if (usedColumns.isIntersect(dictSet)) {
            return couldApplyDictOptimize(operator);
        }
        return false;
    }

    public void fillDisableDictOptimizeColumns(ColumnRefSet columnRefSet) {
        columnRefMap.forEach((k, v) -> {
            if (columnRefSet.contains(k.getId())) {
                columnRefSet.union(v.getUsedColumns());
            }
            fillDisableDictOptimizeColumns(v, columnRefSet);
        });
    }

    public boolean hasUnsupportedDictOperator(Set<Integer> stringColumnIds) {
        ColumnRefSet stringColumnRefSet = new ColumnRefSet();
        for (Integer stringColumnId : stringColumnIds) {
            stringColumnRefSet.union(stringColumnId);
        }

        ColumnRefSet columnRefSet = new ColumnRefSet();
        this.fillDisableDictOptimizeColumns(columnRefSet);
        return columnRefSet.isIntersect(stringColumnRefSet);
    }

    private void fillDisableDictOptimizeColumns(ScalarOperator operator, ColumnRefSet columnRefSet) {
        if (!couldApplyDictOptimize(operator)) {
            columnRefSet.union(operator.getUsedColumns());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Projection that = (Projection) o;
        return columnRefMap.keySet().equals(that.columnRefMap.keySet());
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnRefMap.keySet());
    }

    @Override
    public String toString() {
        return columnRefMap.values().toString();
    }
}
