// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.statistics;

import com.starrocks.sql.optimizer.statistics.Bucket;

import java.util.List;

public class Histogram {
    private final List<Bucket> buckets;

    public Histogram(List<Bucket> buckets) {
        this.buckets = buckets;
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }
}
