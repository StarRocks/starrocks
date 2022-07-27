// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.optimizer.base;

import java.util.Objects;

public class AnyDistributionSpec extends DistributionSpec {
    public AnyDistributionSpec() {
        super(DistributionType.ANY);
    }

    public boolean isSatisfy(DistributionSpec spec, PropertyInfo propertyInfo) {
        return spec.type.equals(DistributionType.ANY);
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

        return obj instanceof AnyDistributionSpec;
    }
}
