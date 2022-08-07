package com.starrocks.catalog;

import com.starrocks.external.hive.HiveMetaClient;

public class HivePartitionKey extends PartitionKey implements NullableKey {
    public HivePartitionKey() {
        super();
    }

    @Override
    public String nullPartitionValue() {
        return HiveMetaClient.PARTITION_NULL_VALUE;
    }
}
