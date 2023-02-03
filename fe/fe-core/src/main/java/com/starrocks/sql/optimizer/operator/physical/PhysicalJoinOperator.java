// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Objects;
import java.util.Set;

public abstract class PhysicalJoinOperator extends PhysicalOperator {
    protected final JoinOperator joinType;
    protected final ScalarOperator onPredicate;
    protected final String joinHint;

    protected PhysicalJoinOperator(OperatorType operatorType, JoinOperator joinType,
                                ScalarOperator onPredicate,
                                String joinHint,
                                long limit,
                                ScalarOperator predicate,
                                Projection projection) {
        super(operatorType);
        this.joinType = joinType;
        this.onPredicate = onPredicate;
        this.joinHint = joinHint;
        this.limit = limit;
        this.predicate = predicate;
        this.projection = projection;
    }

    public JoinOperator getJoinType() {
        return joinType;
    }

    public String getJoinAlgo() {
        return "PhysicalJoin";
    }

    public ScalarOperator getOnPredicate() {
        return onPredicate;
    }

    public String getJoinHint() {
        return joinHint;
    }


    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet refs = super.getUsedColumns();
        if (onPredicate != null) {
            refs.union(onPredicate.getUsedColumns());
        }
        return refs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!super.equals(o)) {
            return false;
        }

        PhysicalJoinOperator that = (PhysicalJoinOperator) o;
        return joinType == that.joinType && Objects.equals(onPredicate, that.onPredicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), joinType, onPredicate);
    }

    @Override
    public boolean couldApplyStringDict(Set<Integer> childDictColumns) {
        Preconditions.checkState(!childDictColumns.isEmpty());
        ColumnRefSet dictSet = new ColumnRefSet();
        for (Integer id : childDictColumns) {
            dictSet.union(id);
        }

        if (predicate != null && predicate.getUsedColumns().isIntersect(dictSet)) {
            return false;
        }

        if (onPredicate != null && onPredicate.getUsedColumns().isIntersect(dictSet)) {
            return false;
        }

        return true;
    }

    public void fillDisableDictOptimizeColumns(ColumnRefSet columnRefSet) {
        if (predicate != null) {
            columnRefSet.union(predicate.getUsedColumns());
        }

        if (onPredicate != null) {
            columnRefSet.union(onPredicate.getUsedColumns());
        }
    }

}
