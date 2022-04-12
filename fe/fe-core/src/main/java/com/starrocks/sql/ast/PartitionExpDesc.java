// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.PartitionDesc;

import java.util.List;

public class PartitionExpDesc extends PartitionDesc {

    private String tableName;

    private String columnName;

    private FunctionName functionName;

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public FunctionName getFunctionName() {
        return functionName;
    }

    public PartitionExpDesc(List<String> names, FunctionName functionName) {
        this.tableName = names.get(0);
        this.columnName = names.get(1);
        this.functionName = functionName;
    }
}
