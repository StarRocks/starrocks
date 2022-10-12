// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.scheduler;

import java.util.Map;
import java.util.Set;

public class MvTaskRunContext extends TaskRunContext {

    Map<String, Set<String>> baseToMvNameRef;
    Map<String, Set<String>> mvToBaseNameRef;

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
}
