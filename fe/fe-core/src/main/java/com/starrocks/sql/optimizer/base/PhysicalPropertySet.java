// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.base;

import java.util.Objects;

/**
 * PropertySet represents a set of required properties
 */
public class PhysicalPropertySet {
    private SortProperty sortProperty;
    private DistributionProperty distributionProperty;

    public static final PhysicalPropertySet EMPTY = new PhysicalPropertySet();

    public PhysicalPropertySet() {
        this(DistributionProperty.EMPTY, SortProperty.EMPTY);
    }

    public PhysicalPropertySet(DistributionProperty distributionProperty) {
        this(distributionProperty, SortProperty.EMPTY);
    }

    public PhysicalPropertySet(SortProperty sortProperty) {
        this(DistributionProperty.EMPTY, sortProperty);
    }

    public PhysicalPropertySet(DistributionProperty distributionProperty, SortProperty sortProperty) {
        this.distributionProperty = distributionProperty;
        this.sortProperty = sortProperty;
    }

    public SortProperty getSortProperty() {
        return sortProperty;
    }

    public void setSortProperty(SortProperty sortProperty) {
        this.sortProperty = sortProperty;
    }

    public DistributionProperty getDistributionProperty() {
        return distributionProperty;
    }

    public void setDistributionProperty(DistributionProperty distributionProperty) {
        this.distributionProperty = distributionProperty;
    }

    public boolean isSatisfy(PhysicalPropertySet other) {
        return sortProperty.isSatisfy(other.sortProperty) &&
                distributionProperty.isSatisfy(other.distributionProperty);
    }

    public PhysicalPropertySet copy() {
        return new PhysicalPropertySet(distributionProperty, sortProperty);
    }

    public boolean isEmpty() {
        return sortProperty.isEmpty() & distributionProperty.isAny();
    }

    @Override
    public int hashCode() {
        return Objects.hash(sortProperty, distributionProperty);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof PhysicalPropertySet)) {
            return false;
        }

        final PhysicalPropertySet other = (PhysicalPropertySet) obj;
        return this.sortProperty.equals(other.sortProperty) &&
                this.distributionProperty.equals(other.distributionProperty);
    }

    @Override
    public String toString() {
        return sortProperty.getSpec().getOrderDescs().toString() +
                ", " + distributionProperty.getSpec();
    }
}
