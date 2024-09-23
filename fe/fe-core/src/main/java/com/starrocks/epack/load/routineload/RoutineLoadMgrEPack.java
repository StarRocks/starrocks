// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.load.routineload;

import com.starrocks.load.routineload.RoutineLoadJob;
import com.starrocks.load.routineload.RoutineLoadMgr;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Map;

public class RoutineLoadMgrEPack extends RoutineLoadMgr {

    public void registerOrUpdateJob(RoutineLoadJob job) {
        writeLock();
        try {
            Map<String, List<RoutineLoadJob>> dbJobs = dbToNameToRoutineLoadJob.get(job.getDbId());
            if (dbJobs == null || dbJobs.isEmpty()) {
                unprotectedAddJob(job);
                GlobalStateMgr.getCurrentState().getEditLog().logCreateRoutineLoadJob(job);
                return;
            }
            List<RoutineLoadJob> jobs = dbJobs.get(job.getName());
            if (jobs == null || jobs.isEmpty()) {
                unprotectedAddJob(job);
                GlobalStateMgr.getCurrentState().getEditLog().logCreateRoutineLoadJob(job);
                return;
            }

            RoutineLoadJob existedJob = jobs.get(jobs.size() - 1);
            RoutineLoadJob.setId(job, existedJob.getId()); // Same job

            unprotectedAddJob(job);
            GlobalStateMgr.getCurrentState().getEditLog().logCreateRoutineLoadJob(job);
        } finally {
            writeUnlock();
        }
    }
}
