// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.base;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;

public class SortProperty implements PhysicalProperty {
    private final OrderSpec spec;

    public static final SortProperty EMPTY = new SortProperty();

    public SortProperty() {
        this.spec = OrderSpec.createEmpty();
    }

    public SortProperty(OrderSpec spec) {
        this.spec = spec;
    }

    public OrderSpec getSpec() {
        return spec;
    }

    public boolean isEmpty() {
        return spec.getOrderDescs().isEmpty();
    }

    @Override
    public boolean isSatisfy(PhysicalProperty other) {
        final OrderSpec rhs = ((SortProperty) other).getSpec();
        return spec.isSatisfy(rhs);
    }

    @Override
    public int hashCode() {
        return spec.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof SortProperty)) {
            return false;
        }

        SortProperty rhs = (SortProperty) obj;
        return spec.equals(rhs.getSpec());
    }

    @Override
    public GroupExpression appendEnforcers(Group child) {
        return new GroupExpression(new PhysicalTopNOperator(spec,
                Operator.DEFAULT_LIMIT, Operator.DEFAULT_OFFSET, null, SortPhase.FINAL, false, true, null, null),
                Lists.newArrayList(child));
    }
}
