// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.statistics;

import java.util.List;
import java.util.Map;

public class Histogram {
    private double min;
    private boolean containMin;
    private double max;
    private boolean containMax;
    private final List<Bucket> buckets;
    private Map<Double, Long> mcv;

    public Histogram(List<Bucket> buckets, Map<Double, Long> mcv) {
        this.buckets = buckets;
        this.mcv = mcv;
        this.min = Double.MIN_VALUE;
        this.containMin = false;
        this.max = Double.MAX_VALUE;
        this.containMax = false;
    }

    public Histogram(List<Bucket> buckets, double min, boolean containMin, double max, boolean containMax) {
        this.buckets = buckets;
        this.min = min;
        this.containMin = containMin;
        this.max = max;
        this.containMax = containMax;
    }

    public void setMin(double min, boolean containMin) {
        this.min = min;
        this.containMin = containMin;
    }

    public double getMin() {
        return min;
    }

    public boolean isContainMin() {
        return containMin;
    }

    public void setMax(double max, boolean containMax) {
        this.max = max;
        this.containMax = containMax;
    }

    public double getMax() {
        return max;
    }

    public boolean isContainMax() {
        return containMax;
    }

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public Map<Double, Long> getMCV() {
        return mcv;
    }
}
