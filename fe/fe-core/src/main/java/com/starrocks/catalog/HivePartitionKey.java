// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.google.common.collect.ImmutableList;
import com.starrocks.connector.hive.HiveMetaClient;

import java.util.List;

public class HivePartitionKey extends PartitionKey implements NullablePartitionKey {
    public HivePartitionKey() {
        super();
    }

    @Override
    public List<String> nullPartitionValueList() {
        return ImmutableList.of(HiveMetaClient.PARTITION_NULL_VALUE);
    }
}
