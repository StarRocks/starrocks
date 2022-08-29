// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.gson.annotations.SerializedName;

public class HiveTableStats {
    // -1: unknown
    // from table parameters
    @SerializedName(value = "numRows")
    private long numRows;
    // from table parameters
    @SerializedName(value = "totalFileBytes")
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
