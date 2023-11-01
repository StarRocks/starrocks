// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.sql.optimizer.base;

import java.util.Objects;

/**
 * PropertySet represents a set of required properties
 */
public class PhysicalPropertySet {
    private SortProperty sortProperty;
    private DistributionProperty distributionProperty;
    private CTEProperty cteProperty;

    public PhysicalPropertySet() {
        this(DistributionProperty.empty(), SortProperty.empty(), CTEProperty.empty());
    }

    public PhysicalPropertySet(DistributionProperty distributionProperty) {
        this(distributionProperty, SortProperty.empty(), CTEProperty.empty());
    }

    public PhysicalPropertySet(SortProperty sortProperty) {
        this(DistributionProperty.empty(), sortProperty, CTEProperty.empty());
    }

    public PhysicalPropertySet(DistributionProperty distributionProperty, SortProperty sortProperty) {
        this(distributionProperty, sortProperty, CTEProperty.empty());
    }

    public PhysicalPropertySet(DistributionProperty distributionProperty, SortProperty sortProperty,
                               CTEProperty cteProperty) {
        this.distributionProperty = distributionProperty;
        this.sortProperty = sortProperty;
        this.cteProperty = cteProperty;
    }

    public static PhysicalPropertySet empty() {
        return new PhysicalPropertySet();
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

    public CTEProperty getCteProperty() {
        return cteProperty;
    }

    public void setCteProperty(CTEProperty cteProperty) {
        this.cteProperty = cteProperty;
    }

    public boolean isSatisfy(PhysicalPropertySet other) {
        return sortProperty.isSatisfy(other.sortProperty) &&
                distributionProperty.isSatisfy(other.distributionProperty);
    }

    public PhysicalPropertySet copy() {
        return new PhysicalPropertySet(distributionProperty, sortProperty, cteProperty);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sortProperty, distributionProperty, cteProperty);
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
                this.distributionProperty.equals(other.distributionProperty) &&
                this.cteProperty.equals(other.cteProperty);
    }

    @Override
    public String toString() {
        return sortProperty.getSpec().getOrderDescs().toString() +
                ", " + distributionProperty.getSpec() + ", " + cteProperty.toString();
    }
}
