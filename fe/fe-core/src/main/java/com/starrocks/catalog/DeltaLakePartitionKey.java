// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

public class DeltaLakePartitionKey extends PartitionKey implements NullablePartitionKey  {
    public DeltaLakePartitionKey() {
        super();
    }

    @Override
    public String nullPartitionValue() {
        return DeltaLakeTable.PARTITION_NULL_VALUE;
    }
}
