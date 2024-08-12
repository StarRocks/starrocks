// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.common.InternalErrorCode;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.epack.load.routineload.RoutineLoadMgrEPack;
import com.starrocks.load.routineload.ErrorReason;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UpdateRoutineLoadMgrJob extends FailoverGroupJob {
    private static final Logger LOG = LogManager.getLogger(UpdateRoutineLoadMgrJob.class);

    public UpdateRoutineLoadMgrJob(FailoverGroup failoverGroup) {
        super(failoverGroup);
    }

    @Override
    public void execute() {
        try {
            for (RoutineLoadJob routineLoadJob : failoverGroup.getObjectMeta().getRoutineLoadMgr().getJob(null, null,
                    true)) {
                if (routineLoadJob.getState() == RoutineLoadJob.JobState.NEED_SCHEDULE) {
                    continue;
                }
                Long localDbId = failoverGroup.getObjectMap().getLocalDatabaseId(routineLoadJob.getDbId());
                if (localDbId == null) {
                    continue;
                }
                Long localTableId = failoverGroup.getObjectMap().getLocalTableId(routineLoadJob.getTableId());
                if (localTableId == null) {
                    continue;
                }

                RoutineLoadJob.setId(routineLoadJob, GlobalStateMgr.getServingState().getNextId());
                RoutineLoadJob.setDbId(routineLoadJob, localDbId);
                RoutineLoadJob.setTableId(routineLoadJob, localTableId);
                routineLoadJob.setWarehouseId(
                        GlobalStateMgr.getServingState().getWarehouseMgr().getBackgroundWarehouse().getId());
                if (routineLoadJob.getState() == RoutineLoadJob.JobState.PAUSED) {
                    routineLoadJob.updateState(RoutineLoadJob.JobState.PAUSED,
                            new ErrorReason(InternalErrorCode.MANUAL_PAUSE_ERR,
                                    "Failover group " + failoverGroup.getName() + " pauses routine load job"),
                            true);
                }
                ((RoutineLoadMgrEPack) GlobalStateMgr.getServingState().getRoutineLoadMgr())
                        .registerOrUpdateJob(routineLoadJob);
            }
        } catch (Exception e) {
            LOG.warn("Failed to replicate routine load job, ", e);
        }
    }
}
