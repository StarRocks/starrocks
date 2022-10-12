// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.base;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

public class OutputInputProperty {
    private final PhysicalPropertySet outputProperty;

    private final List<PhysicalPropertySet> inputProperties;

    public OutputInputProperty(PhysicalPropertySet outputProperty,
                               List<PhysicalPropertySet> inputProperties) {
        this.outputProperty = outputProperty;
        this.inputProperties = inputProperties;
    }

    public static OutputInputProperty of(PhysicalPropertySet outputProperty,
                                         List<PhysicalPropertySet> inputProperties) {
        return new OutputInputProperty(outputProperty, inputProperties);
    }

    public static OutputInputProperty of(PhysicalPropertySet outputProperty,
                                         PhysicalPropertySet... inputProperties) {
        return new OutputInputProperty(outputProperty, Lists.newArrayList(inputProperties));
    }

    public PhysicalPropertySet getOutputProperty() {
        return outputProperty;
    }

    public List<PhysicalPropertySet> getInputProperties() {
        return inputProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OutputInputProperty that = (OutputInputProperty) o;
        return Objects.equals(outputProperty, that.outputProperty) &&
                Objects.equals(inputProperties, that.inputProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(outputProperty, inputProperties);
    }

    @Override
    public String toString() {
        return "OutputInputProperty{" +
                "outputProperty=" + outputProperty +
                ", inputProperties=" + inputProperties +
                '}';
    }
}
