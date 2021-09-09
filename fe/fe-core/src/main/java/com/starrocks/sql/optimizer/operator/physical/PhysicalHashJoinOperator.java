// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.base.Objects;
import com.starrocks.analysis.JoinOperator;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;

public class PhysicalHashJoinOperator extends PhysicalOperator {
    private final JoinOperator joinType;
    private final ScalarOperator joinPredicate;
    private String joinHint;
    private final List<ColumnRefOperator> pruneOutputColumns;

    public PhysicalHashJoinOperator(JoinOperator joinType, ScalarOperator joinPredicate,
                                    List<ColumnRefOperator> pruneOutputColumns) {
        super(OperatorType.PHYSICAL_HASH_JOIN);
        this.joinType = joinType;
        this.joinPredicate = joinPredicate;
        this.pruneOutputColumns = pruneOutputColumns;
    }

    public JoinOperator getJoinType() {
        return joinType;
    }

    public ScalarOperator getJoinPredicate() {
        return joinPredicate;
    }

    public void setJoinHint(String joinHint) {
        this.joinHint = joinHint;
    }

    public String getJoinHint() {
        return joinHint;
    }

    public List<ColumnRefOperator> getPruneOutputColumns() {
        return pruneOutputColumns;
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
        PhysicalHashJoinOperator that = (PhysicalHashJoinOperator) o;
        return joinType == that.joinType && Objects.equal(joinPredicate, that.joinPredicate) &&
                Objects.equal(joinHint, that.joinHint) && Objects.equal(pruneOutputColumns, that.pruneOutputColumns);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(joinType, joinPredicate, joinHint);
    }
}
