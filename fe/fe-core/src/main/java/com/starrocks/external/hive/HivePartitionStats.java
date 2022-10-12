// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.hive;

import com.google.gson.annotations.SerializedName;

public class HivePartitionStats {
    // -1: unknown
    // from partition parameters
    @SerializedName(value = "numRows")
    private long numRows;
    // the size (in bytes) of all the files inside this partition
    @SerializedName(value = "totalFileBytes")
    private long totalFileBytes;

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
}
