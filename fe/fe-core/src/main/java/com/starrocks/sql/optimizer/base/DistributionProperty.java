// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.base;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;

public class DistributionProperty implements PhysicalProperty {
    private final DistributionSpec spec;

    public static final DistributionProperty EMPTY = new DistributionProperty();

    public DistributionProperty() {
        this.spec = DistributionSpec.createAnyDistributionSpec();
    }

    public DistributionProperty(DistributionSpec spec) {
        this.spec = spec;
    }

    public DistributionSpec getSpec() {
        return spec;
    }

    public boolean isAny() {
        return spec.type == DistributionSpec.DistributionType.ANY;
    }

    public boolean isShuffle() {
        return spec.type == DistributionSpec.DistributionType.SHUFFLE;
    }

    public boolean isGather() {
        return spec.type == DistributionSpec.DistributionType.GATHER;
    }

    public boolean isBroadcast() {
        return spec.type == DistributionSpec.DistributionType.BROADCAST;
    }

    @Override
    public boolean isSatisfy(PhysicalProperty other) {
        DistributionSpec otherSpec = ((DistributionProperty) other).getSpec();
        return spec.isSatisfy(otherSpec);
    }

    public GroupExpression appendEnforcers(Group child) {
        return new GroupExpression(new PhysicalDistributionOperator(spec), Lists.newArrayList(child));
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
        if (!(obj instanceof DistributionProperty)) {
            return false;
        }

        DistributionProperty rhs = (DistributionProperty) obj;
        return spec.equals(rhs.getSpec());
    }
}
