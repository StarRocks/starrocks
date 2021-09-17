// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.optimizer.operator;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


public class Projection {
    public static Long totalTime = 0L;

    private final Map<ColumnRefOperator, ScalarOperator> columnRefMap;
    // Used for common operator compute result reuse, we need to compute
    // common sub operators firstly in BE
    private final Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap;

    public Projection(Map<ColumnRefOperator, ScalarOperator> columnRefMap,
                      Map<ColumnRefOperator, ScalarOperator> commonSubOperatorMap) {
        this.columnRefMap = columnRefMap;
        if (commonSubOperatorMap == null) {
            this.commonSubOperatorMap = new HashMap<>();
        } else {
            this.commonSubOperatorMap = commonSubOperatorMap;
        }
    }

    public Map<ColumnRefOperator, ScalarOperator> getColumnRefMap() {
        return columnRefMap;
    }

    public Map<ColumnRefOperator, ScalarOperator> getCommonSubOperatorMap() {
        return commonSubOperatorMap;
    }

    @Override
    public boolean equals(Object o) {
        Long start = System.currentTimeMillis();
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Projection that = (Projection) o;
        boolean b = columnRefMap.keySet().equals(that.columnRefMap.keySet());
        Long end = System.currentTimeMillis();

        totalTime += (end - start);
        return b;
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnRefMap.keySet());
    }
}
