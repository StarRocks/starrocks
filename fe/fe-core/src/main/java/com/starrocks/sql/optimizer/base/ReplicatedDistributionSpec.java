// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.base;

import java.util.Objects;

public class ReplicatedDistributionSpec extends DistributionSpec {
    public ReplicatedDistributionSpec() {
        super(DistributionType.BROADCAST);
    }

    public boolean isSatisfy(DistributionSpec spec) {
        if (spec.type.equals(DistributionType.ANY)) {
            return true;
        }

        if (spec.type.equals(DistributionType.BROADCAST)) {
            return true;
        }

        return false;
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

        return obj instanceof ReplicatedDistributionSpec;
    }

    @Override
    public String toString() {
        return "BROADCAST";
    }
}
