// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.connector.hive;

public class HiveCommonStats {
    private static final HiveCommonStats EMPTY = new HiveCommonStats(-1, -1);

    // Row num is first obtained from the table or partition's parameters.
    // If the num is null or -1, it will be estimated from total size of the partition or table's files.
    private final long rowNums;

    private long totalFileBytes;

    public HiveCommonStats(long rowNums, long totalSize) {
        this.rowNums = rowNums;
        this.totalFileBytes = totalSize;
    }

    public static HiveCommonStats empty() {
        return EMPTY;
    }

    public void setTotalFileBytes(long totalFileBytes) {
        this.totalFileBytes = totalFileBytes;
    }

    public long getRowNums() {
        return rowNums;
    }

    public long getTotalFileBytes() {
        return totalFileBytes;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("HiveCommonStats{");
        sb.append("rowNums=").append(rowNums);
        sb.append(", totalFileBytes=").append(totalFileBytes);
        sb.append('}');
        return sb.toString();
    }
}
