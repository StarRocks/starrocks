// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.collect.ImmutableMap;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.Map;

public class PhysicalDecodeOperator extends PhysicalOperator {
    private final ImmutableMap<Integer, Integer> dictToStrings;
    private final Map<ColumnRefOperator, ScalarOperator> stringFunctions;

    public PhysicalDecodeOperator(ImmutableMap<Integer, Integer> dictToStrings,
                                  Map<ColumnRefOperator, ScalarOperator> stringFunctions) {
        super(OperatorType.PHYSICAL_DECODE);
        this.dictToStrings = dictToStrings;
        this.stringFunctions = stringFunctions;
    }

    public ImmutableMap<Integer, Integer> getDictToStrings() {
        return dictToStrings;
    }

    public Map<ColumnRefOperator, ScalarOperator> getStringFunctions() {
        return stringFunctions;
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