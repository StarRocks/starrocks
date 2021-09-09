// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.base;

import java.util.List;
import java.util.Objects;

public class HashDistributionSpec extends DistributionSpec {
    private final HashDistributionDesc hashDistributionDesc;

    public HashDistributionSpec(HashDistributionDesc distributionDesc) {
        super(DistributionType.SHUFFLE);
        this.hashDistributionDesc = distributionDesc;
    }

    public HashDistributionDesc getHashDistributionDesc() {
        return this.hashDistributionDesc;
    }

    public boolean isSatisfy(DistributionSpec spec) {
        if (spec.type.equals(DistributionType.ANY)) {
            return true;
        }

        if (!spec.type.equals(DistributionType.SHUFFLE)) {
            return false;
        }

        HashDistributionSpec other = (HashDistributionSpec) spec;
        return hashDistributionDesc.isSatisfy(other.hashDistributionDesc);
    }

    public List<Integer> getShuffleColumns() {
        return hashDistributionDesc.getColumns();
    }

    @Override
    public int hashCode() {
        return Objects.hash(hashDistributionDesc);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof HashDistributionSpec)) {
            return false;
        }

        final HashDistributionSpec spec = (HashDistributionSpec) obj;
        return this.hashDistributionDesc.equals(spec.hashDistributionDesc);
    }

    @Override
    public String toString() {
        return hashDistributionDesc.getSourceType().toString() + hashDistributionDesc.toString();
    }
}
