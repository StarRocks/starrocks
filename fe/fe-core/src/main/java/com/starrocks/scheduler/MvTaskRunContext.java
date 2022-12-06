// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.scheduler;

import java.util.Map;
import java.util.Set;

public class MvTaskRunContext extends TaskRunContext {

    Map<String, Set<String>> baseToMvNameRef;
    Map<String, Set<String>> mvToBaseNameRef;

    String nextPartitionStart = null;
    String nextPartitionEnd = null;

    public MvTaskRunContext(TaskRunContext context) {
        this.ctx = context.ctx;
        this.definition = context.definition;
        this.remoteIp = context.remoteIp;
        this.properties = context.properties;
    }

    public Map<String, Set<String>> getBaseToMvNameRef() {
        return baseToMvNameRef;
    }

    public void setBaseToMvNameRef(Map<String, Set<String>> baseToMvNameRef) {
        this.baseToMvNameRef = baseToMvNameRef;
    }

    public Map<String, Set<String>> getMvToBaseNameRef() {
        return mvToBaseNameRef;
    }

    public void setMvToBaseNameRef(Map<String, Set<String>> mvToBaseNameRef) {
        this.mvToBaseNameRef = mvToBaseNameRef;
    }

    public boolean hasNextBatchPartition() {
        return nextPartitionStart != null && nextPartitionEnd != null;
    }

    public String getNextPartitionStart() {
        return nextPartitionStart;
    }

    public void setNextPartitionStart(String nextPartitionStart) {
        this.nextPartitionStart = nextPartitionStart;
    }

    public String getNextPartitionEnd() {
        return nextPartitionEnd;
    }

    public void setNextPartitionEnd(String nextPartitionEnd) {
        this.nextPartitionEnd = nextPartitionEnd;
    }
}
