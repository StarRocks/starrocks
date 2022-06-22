// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.Expr;

import java.util.List;

/**
 * ExpressionRangePartitionInfo replace columns with expressions
 * Some Descriptions:
 * 1. no overwrite old serialized method: read、write and readFields, because we use gson now
 * 2  no overwrite toSql method，because we will redesign range partition grammar in the future
 */
public class ExpressionRangePartitionInfo extends RangePartitionInfo {

    @SerializedName(value = "partitionExprs")
    private final List<Expr> partitionExprs;

    public ExpressionRangePartitionInfo(List<Expr> partitionExprs, List<Column> columns) {
        super(columns);
        Preconditions.checkState(partitionExprs != null);
        Preconditions.checkState(partitionExprs.size() > 0);
        Preconditions.checkState(columns != null);
        Preconditions.checkState(partitionExprs.size() == columns.size());
        this.partitionExprs = partitionExprs;
        this.isMultiColumnPartition = partitionExprs.size() > 0;
    }


    public List<Expr> getPartitionExprs() {
        return partitionExprs;
    }

}

