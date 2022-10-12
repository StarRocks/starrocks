// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.base;

import com.starrocks.sql.optimizer.operator.Operator;

import java.util.Objects;

/**
 * Re-shuffle all date to one node
 */
public class GatherDistributionSpec extends DistributionSpec {
    // limit doesn't affect distribution property
    private long limit = Operator.DEFAULT_LIMIT;

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

        if (spec.type.equals(DistributionType.SHUFFLE) &&
                ((HashDistributionSpec) spec).getHashDistributionDesc().isAggShuffle()) {
            return true;
        }

        return spec instanceof GatherDistributionSpec;
    }

    public long getLimit() {
        return limit;
    }

    public boolean hasLimit() {
        return limit != Operator.DEFAULT_LIMIT;
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
