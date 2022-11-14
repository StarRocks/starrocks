// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.starrocks.connector.hive.HiveMetaClient;

public class HivePartitionKey extends PartitionKey implements NullablePartitionKey {
    public HivePartitionKey() {
        super();
    }

    @Override
    public String nullPartitionValue() {
        return HiveMetaClient.PARTITION_NULL_VALUE;
    }
}
