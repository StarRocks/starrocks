package com.starrocks.external.hive;

import com.starrocks.external.elasticsearch.HivePartitionStatistics;

public class HiveCommonStats {
    // -1: unknown
    // from table or partition parameters
    private long rowNums;
    // the size (in bytes) of all the files inside this partition
    private long totalFileBytes;

    private static HiveCommonStats EMPTY = new HiveCommonStats(-1, -1);

    public static HiveCommonStats empty() {
        return EMPTY;
    }

    public HiveCommonStats(long rowNums, long totalSize) {
        this.rowNums = rowNums;
        this.totalFileBytes = totalSize;
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
}
