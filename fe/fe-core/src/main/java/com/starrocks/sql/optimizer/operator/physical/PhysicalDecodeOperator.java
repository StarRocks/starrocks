// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.collect.ImmutableMap;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;

public class PhysicalDecodeOperator extends PhysicalOperator {
    private final ImmutableMap<Integer, Integer> dictToStrings;

    public PhysicalDecodeOperator(ImmutableMap<Integer, Integer> dictToStrings) {
        super(OperatorType.PHYSICAL_DECODE);
        this.dictToStrings = dictToStrings;
    }

    public ImmutableMap<Integer, Integer> getDictToStrings() {
        return dictToStrings;
    }

    @Override
    public boolean equals(Object o) {
        return this == o;
    }

    @Override
    public int hashCode() {
        return dictToStrings.hashCode();
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalDecode(optExpression, context);
    }

    @Override
    public ColumnRefSet getUsedColumns() {
        return new ColumnRefSet();
    }
}