// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.lock;

import java.util.Arrays;
import java.util.stream.Collectors;

public class LockTarget {
    // path to lock target
    // example:
    //    for a table, it is Long[2], pathIds[0] is dbId, pathIds[1] is tableId
    //    for a partition, it is Long[3], pathIds[0] is dbId, pathIds[1] is tableId, pathIds[2] is partition id
    private Long[] pathIds = null;
    private LockContext lockContext;

    public LockTarget(Long[] pathIds, LockContext lockContext) {
        this.pathIds = pathIds;
        this.lockContext = lockContext;
    }

    public Long[] getPathIds() {
        return pathIds;
    }

    public LockContext getTargetContext() {
        return lockContext;
    }

    public LockMode getLockMode() {
        return lockContext != null ? lockContext.getLockMode() : null;
    }

    public String getName() {
        if (pathIds == null) {
            return null;
        }
        String str = Arrays.stream(pathIds).map(pathId -> pathId.toString()).collect(Collectors.joining("."));
        str += String.format("(mode:{}", getLockMode());
        return str;
    }
}
