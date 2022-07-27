// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.base;

import com.starrocks.thrift.TDistributionType;

public class DistributionSpec {
    protected final DistributionType type;

    protected DistributionSpec(DistributionType type) {
        this.type = type;
    }

    @SuppressWarnings("unchecked")
    public <T> T cast() {
        return (T) this;
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

    public boolean isSatisfy(DistributionSpec spec, PropertyInfo propertyInfo) {
        return false;
    }

    public enum DistributionType {
        ANY,
        BROADCAST,
        SHUFFLE,
        GATHER,
        ;

        public TDistributionType toThrift() {
            if (this == ANY) {
                return TDistributionType.ANY;
            } else if (this == BROADCAST) {
                return TDistributionType.BROADCAST;
            } else if (this == SHUFFLE) {
                return TDistributionType.SHUFFLE;
            } else {
                return TDistributionType.GATHER;
            }
        }
    }

    @Override
    public String toString() {
        return type.toString();
    }
}
