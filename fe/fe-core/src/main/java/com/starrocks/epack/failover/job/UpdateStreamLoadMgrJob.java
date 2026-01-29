// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.epack.load.streamload.StreamLoadMgrEPack;
import com.starrocks.load.streamload.AbstractStreamLoadTask;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
import com.starrocks.warehouse.cngroup.ComputeResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UpdateStreamLoadMgrJob extends FailoverGroupJob {
    private static final Logger LOG = LogManager.getLogger(UpdateStreamLoadMgrJob.class);

    public UpdateStreamLoadMgrJob(FailoverGroup failoverGroup) {
        super(failoverGroup);
    }

    @Override
    public void execute() {
        for (AbstractStreamLoadTask task : failoverGroup.getObjectMeta().getStreamLoadMgr().getIdToStreamLoadTask()
                .values()) {
            if (!(task instanceof StreamLoadTask streamLoadTask)) {
                continue;
            }
            if (streamLoadTask.getState() != StreamLoadTask.State.FINISHED) {
                continue;
            }

            Long localDbId = failoverGroup.getObjectMap().getLocalDatabaseId(streamLoadTask.getDBId());
            if (localDbId == null) {
                continue;
            }

            Long localTableId = failoverGroup.getObjectMap().getLocalTableId(streamLoadTask.getTableId());
            if (localTableId == null) {
                continue;
            }

            StreamLoadTask.setId(streamLoadTask, GlobalStateMgr.getServingState().getNextId());
            StreamLoadTask.setDBId(streamLoadTask, localDbId);
            StreamLoadTask.setTableId(streamLoadTask, localTableId);
            final WarehouseManager warehouseManager = GlobalStateMgr.getServingState().getWarehouseMgr();
            try {
                ComputeResource resource = warehouseManager.getBackgroundComputeResource();
                StreamLoadTask.setComputeResource(streamLoadTask, resource);
            } catch (Exception e) {
                LOG.warn("Failed to acquire resource for stream load task: {}", streamLoadTask.getId(), e);
                continue;
            }

            ((StreamLoadMgrEPack) GlobalStateMgr.getServingState().getStreamLoadMgr()).registerLoadTask(streamLoadTask);
        }
    }
}
