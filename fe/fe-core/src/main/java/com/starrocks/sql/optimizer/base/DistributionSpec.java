// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.base;

public class DistributionSpec {
    protected final DistributionType type;

    protected DistributionSpec(DistributionType type) {
        this.type = type;
    }

    public DistributionType getType() {
        return type;
    }

    public static DistributionSpec createAnyDistributionSpec() {
        return new AnyDistributionSpec();
    }

    public static HashDistributionSpec createHashDistributionSpec(HashDistributionDesc distributionDesc) {
        return new HashDistributionSpec(distributionDesc);
    }

    public static DistributionSpec createReplicatedDistributionSpec() {
        return new ReplicatedDistributionSpec();
    }

    public static DistributionSpec createGatherDistributionSpec() {
        return new GatherDistributionSpec();
    }

    public static DistributionSpec createGatherDistributionSpec(long limit) {
        return new GatherDistributionSpec(limit);
    }

    public boolean isSatisfy(DistributionSpec spec) {
        return false;
    }

    public enum DistributionType {
        ANY,
        BROADCAST,
        SHUFFLE,
        GATHER,
    }

    @Override
    public String toString() {
        return type.toString();
    }
}
