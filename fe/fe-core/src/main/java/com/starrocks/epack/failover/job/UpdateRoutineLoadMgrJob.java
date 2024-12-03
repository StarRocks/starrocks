// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.common.InternalErrorCode;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.StarRocksException;
import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.epack.load.routineload.RoutineLoadMgrEPack;
import com.starrocks.load.routineload.ErrorReason;
import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class UpdateRoutineLoadMgrJob extends FailoverGroupJob {
    private static final Logger LOG = LogManager.getLogger(UpdateRoutineLoadMgrJob.class);

    public UpdateRoutineLoadMgrJob(FailoverGroup failoverGroup) {
        super(failoverGroup);
    }

    @Override
    public void execute() {
        List<RoutineLoadJob> routineLoadJobs = null;
        try {
            routineLoadJobs = failoverGroup.getObjectMeta().getRoutineLoadMgr().getJob(null, null, true);
        } catch (MetaNotFoundException e) {
            failoverGroup.addErrorMessage("Failed to get routine load jobs " + e.getMessage());
            LOG.warn("Failed to get routine load jobs ", e);
            return;
        }

        for (RoutineLoadJob routineLoadJob : routineLoadJobs) {
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
            if (!routineLoadJob.isFinal() && routineLoadJob.getState() != RoutineLoadJob.JobState.PAUSED) {
                try {
                    routineLoadJob.updateState(RoutineLoadJob.JobState.PAUSED,
                            new ErrorReason(InternalErrorCode.MANUAL_PAUSE_ERR,
                                    "Failover group " + failoverGroup.getName() + " pauses routine load job"),
                            true);
                } catch (StarRocksException e) {
                    failoverGroup.addErrorMessage("Failed to update routine load job state " + e.getMessage());
                    LOG.warn("Failed to update routine load job state ", e);
                    continue;
                }
            }
            ((RoutineLoadMgrEPack) GlobalStateMgr.getServingState().getRoutineLoadMgr())
                    .registerOrUpdateJob(routineLoadJob);
        }
    }
}
