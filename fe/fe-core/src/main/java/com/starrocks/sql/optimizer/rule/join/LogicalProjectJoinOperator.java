// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.rule.join;

import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;

public class LogicalProjectJoinOperator extends LogicalOperator {
    private LogicalProjectOperator projectOperator;
    private final LogicalJoinOperator joinOperator;

    private Group group;

    public LogicalProjectJoinOperator(LogicalProjectOperator projectOperator, LogicalJoinOperator joinOperator) {
        super(OperatorType.LOGICAL_PROJECT_JOIN);
        this.projectOperator = projectOperator;
        this.joinOperator = joinOperator;
    }

    public String getJoinHint() {
        return joinOperator.getJoinHint();
    }

    public boolean isInnerOrCrossJoin() {
        return joinOperator.isInnerOrCrossJoin();
    }

    public LogicalJoinOperator getJoinOperator() {
        return joinOperator;
    }

    public ScalarOperator getOnPredicate() {
        return joinOperator.getOnPredicate();
    }

    @Override
    public ScalarOperator getPredicate() {
        return joinOperator.getPredicate();
    }

    public LogicalProjectOperator getProjectOperator() {
        return projectOperator;
    }

    public void setProjectOperator(LogicalProjectOperator projectOperator) {
        this.projectOperator = projectOperator;
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    public Group getGroup() {
        return group;
    }

    public ColumnRefSet getRequiredChildInputColumns() {
        ColumnRefSet requiredInput = new ColumnRefSet();
        requiredInput.union(joinOperator.getRequiredChildInputColumns());
        for (ScalarOperator op : projectOperator.getColumnRefMap().values()) {
            requiredInput.union(op.getUsedColumns());
        }
        return requiredInput;
    }

    public ColumnRefSet getOutputColumns() {
        return new ColumnRefSet(new ArrayList<>(projectOperator.getColumnRefMap().keySet()));
    }

    public long getLimit() {
        return projectOperator.getLimit();
    }

    @Override
    public void setLimit(long limit) {
        this.projectOperator.setLimit(limit);
    }

    public boolean hasLimit() {
        return projectOperator.hasLimit();
    }

    @Override
    public ColumnRefSet getOutputColumns(ExpressionContext expressionContext) {
        return new ColumnRefSet(new ArrayList<>(projectOperator.getColumnRefMap().keySet()));
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitMultiJoin(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitLogicalProjectJoin(optExpression, context);
    }
}
