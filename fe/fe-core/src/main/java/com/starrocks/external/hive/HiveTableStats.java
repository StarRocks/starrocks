// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

public class HiveTableStats {
    // -1: unknown
    // from table parameters
    private long numRows;
    // from table parameters
    private long totalFileBytes;

    public HiveTableStats(long numRows, long totalFileBytes) {
        this.numRows = numRows;
        this.totalFileBytes = totalFileBytes;
    }

    public long getNumRows() {
        return numRows;
    }

    public long getTotalFileBytes() {
        return totalFileBytes;
    }
}
