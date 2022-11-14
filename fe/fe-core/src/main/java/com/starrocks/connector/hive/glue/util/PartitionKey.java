// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive.glue.util;

import com.amazonaws.services.glue.model.Partition;

import java.util.List;

public class PartitionKey {

    private final List<String> partitionValues;
    private final int hashCode;

    public PartitionKey(Partition partition) {
        this(partition.getValues());
    }

    public PartitionKey(List<String> partitionValues) {
        if (partitionValues == null) {
            throw new IllegalArgumentException("Partition values cannot be null");
        }
        this.partitionValues = partitionValues;
        this.hashCode = partitionValues.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || (other != null && other instanceof PartitionKey
                && this.partitionValues.equals(((PartitionKey) other).partitionValues));
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    List<String> getValues() {
        return partitionValues;
    }

}
