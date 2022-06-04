// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.lock;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.transaction.TransactionState;

import java.util.Arrays;
import java.util.stream.Collectors;

public class LockTarget {
    private Long[] pathIds = null;
    public static class TargetContext {
        // should be unique
        // example: txnId for load
        private final long relatedId;
        private final LockMode lockMode;
        private final long lockTimestamp;
        private final TransactionState.LoadJobSourceType loadJobSourceType;

        public TargetContext(long relatedId, LockMode lockMode, long lockTimestamp,
                             TransactionState.LoadJobSourceType loadJobSourceType) {
            this.relatedId = relatedId;
            this.lockMode = lockMode;
            this.lockTimestamp = lockTimestamp;
            this.loadJobSourceType = loadJobSourceType;
        }

        public long getRelatedId() {
            return relatedId;
        }

        public LockMode getLockMode() {
            return lockMode;
        }

        public long getLockTimestamp() {
            return lockTimestamp;
        }

        public TransactionState.LoadJobSourceType getLoadJobSourceType() {
            return loadJobSourceType;
        }
    }
    private TargetContext targetContext;

    public LockTarget() {
        this.pathIds = null;
        this.targetContext = null;
    }

    public LockTarget(Long[] pathIds, TargetContext targetContext) {
        this.pathIds = pathIds;
        this.targetContext = targetContext;
    }

    public LockTarget(Database database, Table table, TargetContext targetContext) {
        this.pathIds = new Long[2];
        // dbId
        this.pathIds[0] = database.getId();
        this.pathIds[1] = table.getId();
        this.targetContext = targetContext;
    }

    public LockTarget(long dbId, long tableId, TargetContext targetContext) {
        this.pathIds = new Long[2];
        // dbId
        this.pathIds[0] = dbId;
        this.pathIds[1] = tableId;
        this.targetContext = targetContext;
    }

    public LockTarget(long dbId, long tableId, long partitionId, TargetContext targetContext) {
        this.pathIds = new Long[3];
        // dbId
        this.pathIds[0] = dbId;
        this.pathIds[1] = tableId;
        this.pathIds[2] = partitionId;
        this.targetContext = targetContext;
    }

    public Long[] getPathIds() {
        return pathIds;
    }

    public TargetContext getTargetContext() {
        return targetContext;
    }

    public String getName() {
        if (pathIds == null) {
            return null;
        }
        final String str = Arrays.stream(pathIds).map(pathId -> pathId.toString()).collect(Collectors.joining("."));
        return str;
    }
}
