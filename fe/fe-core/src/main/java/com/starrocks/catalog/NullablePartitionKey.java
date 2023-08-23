// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.catalog;

import com.google.common.collect.ImmutableList;

import java.util.List;

public interface NullablePartitionKey {
    default List<String> nullPartitionValueList() {
        return ImmutableList.of("");
    }
}
