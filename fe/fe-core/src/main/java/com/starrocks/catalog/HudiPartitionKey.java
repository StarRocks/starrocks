// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.starrocks.external.hive.HiveMetaClient;

public class HudiPartitionKey extends PartitionKey implements NullablePartitionKey {
    public HudiPartitionKey() {
        super();
    }

    @Override
    public String nullPartitionValue() {
        return HiveMetaClient.HUDI_PARTITION_NULL_VALUE;
    }
}
