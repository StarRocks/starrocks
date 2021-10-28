// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

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

    private boolean couldApplyStringDict(ScalarOperator operator, ColumnRefSet dictSet) {
        ColumnRefSet usedColumns = operator.getUsedColumns();
        if (usedColumns.isIntersect(dictSet)) {
            if (usedColumns.cardinality() > 1) {
                return false;
            }
            if (operator instanceof ColumnRefOperator) {
                return true;
            }  else if (operator instanceof CallOperator) {
                CallOperator callOperator = (CallOperator) operator;
                return callOperator.getFunction().isCouldApplyDictOptimize();
            }
        }
        return false;
    }

    public void fillDisableDictOptimizeColumns(ColumnRefSet columnRefSet) {
        for (ScalarOperator operator : columnRefMap.values()) {
            fillDisableDictOptimizeColumns(operator, columnRefSet);
        }

        for (ScalarOperator operator : commonSubOperatorMap.values()) {
            fillDisableDictOptimizeColumns(operator, columnRefSet);
        }
    }

    private void fillDisableDictOptimizeColumns(ScalarOperator operator, ColumnRefSet columnRefSet) {
        if (operator instanceof CallOperator) {
            CallOperator callOperator = (CallOperator) operator;
            if (callOperator instanceof CaseWhenOperator ||
                    callOperator instanceof CastOperator) {
                columnRefSet.union(callOperator.getUsedColumns());
            } else if (!callOperator.getFunction().isCouldApplyDictOptimize()) {
                columnRefSet.union(callOperator.getUsedColumns());
            } else if (operator.getUsedColumns().cardinality() > 1) {
                columnRefSet.union(callOperator.getUsedColumns());
            }
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
}
