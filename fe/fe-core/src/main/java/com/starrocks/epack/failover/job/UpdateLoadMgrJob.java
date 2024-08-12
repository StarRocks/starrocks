// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.epack.load.loadv2.LoadMgrEPack;
import com.starrocks.load.loadv2.BrokerLoadJob;
import com.starrocks.load.loadv2.InsertLoadJob;
import com.starrocks.load.loadv2.JobState;
import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.SparkLoadJob;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UpdateLoadMgrJob extends FailoverGroupJob {
    private static final Logger LOG = LogManager.getLogger(UpdateLoadMgrJob.class);

    public UpdateLoadMgrJob(FailoverGroup failoverGroup) {
        super(failoverGroup);
    }

    @Override
    public void execute() {
        for (LoadJob loadJob : failoverGroup.getObjectMeta().getLoadMgr().getLoadJobs(null)) {
            if (loadJob.getState() != JobState.FINISHED) {
                continue;
            }
            Long localDbId = failoverGroup.getObjectMap().getLocalDatabaseId(loadJob.getDbId());
            if (localDbId == null) {
                continue;
            }
            if (loadJob instanceof InsertLoadJob) {
                InsertLoadJob insertLoadJob = (InsertLoadJob) loadJob;
                if (insertLoadJob.isInternalJob()) {
                    continue;
                }
                Long localTableId = failoverGroup.getObjectMap().getLocalTableId(insertLoadJob.getTableId());
                if (localTableId == null) {
                    continue;
                }
                GlobalStateMgr globalStateMgr = GlobalStateMgr.getServingState();
                loadJob.setId(globalStateMgr.getNextId());
                LoadJob.setDbId(loadJob, localDbId);
                loadJob.setWarehouseId(globalStateMgr.getWarehouseMgr().getBackgroundWarehouse().getId());
                InsertLoadJob.setTableId(insertLoadJob, localTableId);
                ((LoadMgrEPack) globalStateMgr.getLoadMgr()).registerLoadJob(insertLoadJob);
            } else if (loadJob instanceof BrokerLoadJob || loadJob instanceof SparkLoadJob) {
                GlobalStateMgr globalStateMgr = GlobalStateMgr.getServingState();
                loadJob.setId(globalStateMgr.getNextId());
                LoadJob.setDbId(loadJob, localDbId);
                loadJob.setWarehouseId(globalStateMgr.getWarehouseMgr().getBackgroundWarehouse().getId());
                ((LoadMgrEPack) globalStateMgr.getLoadMgr()).registerLoadJob(loadJob);
            } else {
                LOG.warn("Unknown load job: {}", loadJob);
            }
        }
    }
}
