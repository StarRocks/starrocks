// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.physical;

import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;

import java.util.Set;

public abstract class PhysicalOperator extends Operator {
    protected OrderSpec orderSpec;
    protected DistributionSpec distributionSpec;

    protected PhysicalOperator(OperatorType type) {
        this(type, DistributionSpec.createAnyDistributionSpec(), OrderSpec.createEmpty());
    }

    protected PhysicalOperator(OperatorType type, DistributionSpec distributionSpec) {
        this(type, distributionSpec, OrderSpec.createEmpty());
    }

    protected PhysicalOperator(OperatorType type, OrderSpec orderSpec) {
        this(type, DistributionSpec.createAnyDistributionSpec(), orderSpec);
    }

    protected PhysicalOperator(OperatorType type, DistributionSpec distributionSpec,
                               OrderSpec orderSpec) {
        super(type);
        this.distributionSpec = distributionSpec;
        this.orderSpec = orderSpec;
    }

    public OrderSpec getOrderSpec() {
        return orderSpec;
    }

    public DistributionSpec getDistributionSpec() {
        return distributionSpec;
    }

    @Override
    public boolean isPhysical() {
        return true;
    }

    public ColumnRefSet getUsedColumns() {
        ColumnRefSet result = new ColumnRefSet();
        if (predicate != null) {
            result.union(predicate.getUsedColumns());
        }

        if (orderSpec != null) {
            orderSpec.getOrderDescs().forEach(o -> result.union(o.getColumnRef()));
        }

        return result;
    }

    public boolean couldApplyStringDict(Set<Integer> childDictColumns) {
        return false;
    }
}
