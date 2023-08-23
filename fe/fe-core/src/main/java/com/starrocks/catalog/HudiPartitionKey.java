// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.google.common.collect.ImmutableList;
import com.starrocks.connector.hive.HiveMetaClient;

import java.util.List;

public class HudiPartitionKey extends PartitionKey implements NullablePartitionKey {
    public HudiPartitionKey() {
        super();
    }

    @Override
    public List<String> nullPartitionValueList() {
        return ImmutableList.of(HiveMetaClient.PARTITION_NULL_VALUE, HiveMetaClient.HUDI_PARTITION_NULL_VALUE);
    }
}
