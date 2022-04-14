// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.sql.ast;

import com.starrocks.analysis.PartitionDesc;

import java.util.List;

public class PartitionExpDesc extends PartitionDesc {

    private String tableName;

    private String columnName;

    private String functionName;

    private String functionArgs;

    public String getTableName() {
        return tableName;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getFunctionName() {
        return functionName;
    }

    public String getFunctionArgs() {
        return functionArgs;
    }

    public PartitionExpDesc(List<String> names, String functionName, String functionArgs) {
        this.tableName = names.get(0);
        this.columnName = names.get(1);
        this.functionName = functionName;
        this.functionArgs = functionArgs;
    }
}
