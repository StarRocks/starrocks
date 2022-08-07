package com.starrocks.catalog;

import com.starrocks.external.hive.HiveMetaClient;

public class HudiPartitionKey extends PartitionKey implements NullableKey {
    public HudiPartitionKey() {
        super();
    }

    @Override
    public String nullPartitionValue() {
        return HiveMetaClient.HUDI_PARTITION_NULL_VALUE;
    }
}