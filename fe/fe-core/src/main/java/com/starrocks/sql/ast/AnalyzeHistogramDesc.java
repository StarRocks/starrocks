// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.ast;

public class AnalyzeHistogramDesc implements AnalyzeTypeDesc {
    private long buckets;

    public AnalyzeHistogramDesc(long buckets) {
        this.buckets = buckets;
    }

    public long getBuckets() {
        return buckets;
    }

    public void setBuckets(long buckets) {
        this.buckets = buckets;
    }
}
