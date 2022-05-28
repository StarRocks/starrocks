// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.List;
import java.util.Objects;

public class CloneOperator extends ScalarOperator {
    private List<ScalarOperator> arguments;

    public CloneOperator(ScalarOperator argument) {
        super(OperatorType.CLONE, argument.getType());
        arguments = Lists.newArrayList(argument);
        setType(argument.getType());
    }

    @Override
    public boolean isNullable() {
        return arguments.get(0).isNullable();
    }

    @Override
    public List<ScalarOperator> getChildren() {
        return arguments;
    }

    @Override
    public ScalarOperator getChild(int index) {
        return arguments.get(0);
    }

    @Override
    public void setChild(int index, ScalarOperator child) {
        arguments.set(0, child);
    }

    @Override
    public String toString() {
        return "Clone(" + arguments.get(0) + ")";
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public int hashCode() {
        return Objects.hash(arguments);
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitCloneOperator(this, context);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        return arguments.get(0).getUsedColumns();
    }

    @Override
    public ScalarOperator clone() {
        CloneOperator operator = (CloneOperator) super.clone();
        // Deep copy here
        List<ScalarOperator> newArguments = Lists.newArrayList();
        this.arguments.forEach(p -> newArguments.add(p.clone()));
        operator.arguments = newArguments;
        return operator;
    }
}
