// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.server.GlobalStateMgr;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UpdateTableIncrementIdJob extends FailoverGroupJob {

    protected UpdateTableIncrementIdJob(FailoverGroup failoverGroup) {
        super(failoverGroup);
    }

    @Override
    public void execute() {
        ConcurrentHashMap<Long, Long> tableIdToIncrementId = new ConcurrentHashMap<>();
        for (Map.Entry<Long, Long> entry : failoverGroup.getObjectMeta().getTableIdToIncrementId().entrySet()) {
            Long localTableId = failoverGroup.getObjectMap().getLocalTableId(entry.getKey());
            if (localTableId == null) {
                continue;
            }
            tableIdToIncrementId.put(localTableId, entry.getValue());
        }

        GlobalStateMgr.getServingState().getLocalMetastore().setTableAutoIncrementId(tableIdToIncrementId);
    }
}
