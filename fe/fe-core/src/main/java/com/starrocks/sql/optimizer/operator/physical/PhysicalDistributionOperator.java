// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.operator.physical;

import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.OperatorVisitor;
import com.starrocks.sql.optimizer.statistics.ColumnDict;

import java.util.List;
import java.util.Set;

public class PhysicalDistributionOperator extends PhysicalOperator {
    public PhysicalDistributionOperator(DistributionSpec spec) {
        super(OperatorType.PHYSICAL_DISTRIBUTION, spec);
    }

    public void setDistributionSpec(DistributionSpec spec) {
        this.distributionSpec = spec;
    }

    private final List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();

    public List<Pair<Integer, ColumnDict>> getGlobalDicts() {
        return globalDicts;
    }

    public void addGlobalDictColumns(Pair<Integer, ColumnDict> dict) {
        globalDicts.add(dict);
    }


    @Override
    public boolean equals(Object obj) {
        return this == obj;
    }

    @Override
    public int hashCode() {
        return this.distributionSpec.hashCode();
    }

    @Override
    public <R, C> R accept(OperatorVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalDistribution(this, context);
    }

    @Override
    public <R, C> R accept(OptExpressionVisitor<R, C> visitor, OptExpression optExpression, C context) {
        return visitor.visitPhysicalDistribution(optExpression, context);
    }

    @Override
    public boolean couldApplyStringDict(Set<Integer> childDictColumns) {
        return true;
    }
}
