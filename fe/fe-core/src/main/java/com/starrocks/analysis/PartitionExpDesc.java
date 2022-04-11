// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.analysis;

import java.util.List;

public class PartitionExpDesc extends PartitionDesc {

    private String tableName;

    private String columnName;

    private FunctionName functionName;

    public PartitionExpDesc(List<String> names, FunctionName functionName) {
        this.tableName = names.get(0);
        this.columnName = names.get(1);
        this.functionName = functionName;
    }
}
