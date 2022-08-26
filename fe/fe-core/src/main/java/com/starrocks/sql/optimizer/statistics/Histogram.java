// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.statistics;

import java.util.List;
import java.util.Map;

public class Histogram {
    private final List<Bucket> buckets;
    private final Map<String, Long> mcv;

    public Histogram(List<Bucket> buckets, Map<String, Long> mcv) {
        this.buckets = buckets;
        this.mcv = mcv;

    }

    public long getTotalRows() {
        long totalRows = 0;
        if (buckets != null && !buckets.isEmpty()) {
            totalRows += buckets.get(buckets.size() - 1).getCount();
        }
        if (mcv != null) {
            totalRows += mcv.values().stream().reduce(Long::sum).orElse(0L);
        }
        return totalRows;
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public Map<String, Long> getMCV() {
        return mcv;
    }
}
