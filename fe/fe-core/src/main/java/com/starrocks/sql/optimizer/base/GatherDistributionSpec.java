// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.sql.optimizer.base;

import java.util.Objects;

/**
 * Re-shuffle all date to one node
 */
public class GatherDistributionSpec extends DistributionSpec {
    // limit doesn't affect distribution property
    private long limit = -1;

    public GatherDistributionSpec() {
        super(DistributionType.GATHER);
    }

    public GatherDistributionSpec(long limit) {
        super(DistributionType.GATHER);
        this.limit = limit;
    }

    public boolean isSatisfy(DistributionSpec spec) {
        if (spec.type.equals(DistributionType.ANY)) {
            return true;
        }

        return spec instanceof GatherDistributionSpec;
    }

    public long getLimit() {
        return limit;
    }

    public boolean hasLimit() {
        return limit != -1;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        return obj instanceof GatherDistributionSpec;
    }

    @Override
    public String toString() {
        return "GATHER";
    }
}
