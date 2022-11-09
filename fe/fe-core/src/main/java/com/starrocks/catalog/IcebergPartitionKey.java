// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

public class IcebergPartitionKey extends PartitionKey implements NullablePartitionKey {
    public IcebergPartitionKey() {
        super();
    }

    @Override
    public String nullPartitionValue() {
        return IcebergTable.PARTITION_NULL_VALUE;
    }
}
