// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.external.hive;

import com.starrocks.analysis.LiteralExpr;

import java.util.List;

public class HivePartitionStats {
    // -1: unknown
    // from partition parameters
    private long numRows;
    // the size (in bytes) of all the files inside this partition
    private long totalFileBytes;
    // the key for this hive partition;
    private List<LiteralExpr> key;

    public HivePartitionStats(long numRows) {
        this.numRows = numRows;
    }

    public void setTotalFileBytes(long totalFileBytes) {
        this.totalFileBytes = totalFileBytes;
    }

    public long getNumRows() {
        return numRows;
    }

    public long getTotalFileBytes() {
        return totalFileBytes;
    }

    public List<LiteralExpr> getKey() {
        return key;
    }

    public void setKey(List<LiteralExpr> key) {
        this.key = key;
    }
}
