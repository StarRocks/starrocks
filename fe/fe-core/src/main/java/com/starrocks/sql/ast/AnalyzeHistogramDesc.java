// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.ast;

public class AnalyzeHistogramDesc implements AnalyzeTypeDesc {
    private long buckets;
    private long mcv;

    public AnalyzeHistogramDesc(long buckets, long mcv) {
        this.buckets = buckets;
        this.mcv = mcv;
    }

    public long getBuckets() {
        return buckets;
    }

    public void setBuckets(long buckets) {
        this.buckets = buckets;
    }

    public long getMcv() {
        return mcv;
    }

    public void setMcv(long mcv) {
        this.mcv = mcv;
    }
}
