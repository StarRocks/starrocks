// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.base;

import com.starrocks.thrift.TDistributionType;

import java.util.List;

public class DistributionSpec {
    protected final DistributionType type;
    protected PhysicalPropertyInfo physicalPropertyInfo;

    protected DistributionSpec(DistributionType type) {
        this(type, new PhysicalPropertyInfo());
    }

    protected DistributionSpec(DistributionType type, PhysicalPropertyInfo physicalPropertyInfo) {
        this.type = type;
        this.physicalPropertyInfo = physicalPropertyInfo;
    }

    // Physical property for hash distribution desc
    public static class PhysicalPropertyInfo {
        public List<Long> colocateTableList = com.clearspring.analytics.util.Lists.newArrayList();
        public boolean isReplicate = false;
        public List<Long> partitionIds = com.clearspring.analytics.util.Lists.newArrayList();
        public ColumnRefSet nullableColumns = new ColumnRefSet();

        public boolean isSinglePartition() {
            return partitionIds.size() == 1;
        }

        public void addColocateTableList(List<Long> colocateTableList) {
            this.colocateTableList.addAll(colocateTableList);
        }
    }

    public PhysicalPropertyInfo getPhysicalPropertyInfo() {
        return physicalPropertyInfo;
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
