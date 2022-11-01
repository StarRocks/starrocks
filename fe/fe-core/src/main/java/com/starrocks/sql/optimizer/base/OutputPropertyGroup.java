// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.base;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

public class OutputPropertyGroup {
    private final PhysicalPropertySet outputProperty;
    private final boolean isSameGroup;
    private final List<PhysicalPropertySet> childrenOutputProperties;

    public OutputPropertyGroup(PhysicalPropertySet outputProperty, boolean isSameGroup,
                               List<PhysicalPropertySet> childrenOutputProperties) {
        this.outputProperty = outputProperty;
        this.isSameGroup = isSameGroup;
        this.childrenOutputProperties = childrenOutputProperties;
    }

    public static OutputPropertyGroup of(PhysicalPropertySet outputProperty,
                                         boolean isSameGroup,
                                         List<PhysicalPropertySet> inputProperties) {
        return new OutputPropertyGroup(outputProperty, isSameGroup, inputProperties);
    }

    public static OutputPropertyGroup of(PhysicalPropertySet outputProperty,
                                         boolean isSameGroup,
                                         PhysicalPropertySet... inputProperties) {
        return new OutputPropertyGroup(outputProperty, isSameGroup, Lists.newArrayList(inputProperties));
    }

    public PhysicalPropertySet getOutputProperty() {
        return outputProperty;
    }

    public boolean isSameGroup() {
        return isSameGroup;
    }

    public List<PhysicalPropertySet> getChildrenOutputProperties() {
        return childrenOutputProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OutputPropertyGroup that = (OutputPropertyGroup) o;
        return Objects.equals(outputProperty, that.outputProperty) &&
                Objects.equals(isSameGroup, that.isSameGroup) &&
                Objects.equals(childrenOutputProperties, that.childrenOutputProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputProperty, isSameGroup, childrenOutputProperties);
    }

    @Override
    public String toString() {
        return "OutputPropertyGroup{" +
                "outputProperty=" + outputProperty +
                ", isSameGroup=" + isSameGroup +
                ", childrenOutputProperties=" + childrenOutputProperties +
                '}';
    }
}
