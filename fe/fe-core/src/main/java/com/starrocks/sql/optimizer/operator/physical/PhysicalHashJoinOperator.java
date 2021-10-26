// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Objects;
import java.util.Set;

public class PhysicalHashJoinOperator extends PhysicalOperator {
    private final JoinOperator joinType;
    private final ScalarOperator joinPredicate;
    private final String joinHint;

    public PhysicalHashJoinOperator(JoinOperator joinType,
                                    ScalarOperator joinPredicate,
                                    String joinHint,
                                    long limit,
                                    ScalarOperator predicate,
                                    Projection projection) {
        super(OperatorType.PHYSICAL_HASH_JOIN);
        this.joinType = joinType;
        this.joinPredicate = joinPredicate;
        this.joinHint = joinHint;
        this.limit = limit;
        this.predicate = predicate;
        this.projection = projection;
    }

    public JoinOperator getJoinType() {
        return joinType;
    }

    public ScalarOperator getJoinPredicate() {
        return joinPredicate;
    }

    public String getJoinHint() {
        return joinHint;
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalHashJoin(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalHashJoin(optExpression, context);
    }

    public String toString() {
        return "PhysicalHashJoin" + " {" +
                "joinType='" + joinType.toString() + '\'' +
                ", onConjuncts='" + joinPredicate + '\'' +
                '}';
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        ColumnRefSet refs = super.getUsedColumns();
        if (joinPredicate != null) {
            refs.union(joinPredicate.getUsedColumns());
        }
        return refs;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        PhysicalHashJoinOperator that = (PhysicalHashJoinOperator) o;
        return joinType == that.joinType && Objects.equals(joinPredicate, that.joinPredicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), joinType, joinPredicate);
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

        if (joinPredicate != null && joinPredicate.getUsedColumns().isIntersect(dictSet)) {
            return false;
        }

        return true;
    }

    public void fillDisableDictOptimizeColumns(ColumnRefSet columnRefSet) {
        if (predicate != null) {
            columnRefSet.union(predicate.getUsedColumns());
        }

        if (joinPredicate != null) {
            columnRefSet.union(joinPredicate.getUsedColumns());
        }
    }

}
