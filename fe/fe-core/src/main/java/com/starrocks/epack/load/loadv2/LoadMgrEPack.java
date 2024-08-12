// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.load.loadv2;

import com.starrocks.load.loadv2.LoadJob;
import com.starrocks.load.loadv2.LoadJobScheduler;
import com.starrocks.load.loadv2.LoadMgr;
import com.starrocks.server.GlobalStateMgr;

import java.util.List;
import java.util.Map;

public class LoadMgrEPack extends LoadMgr {

    public LoadMgrEPack(LoadJobScheduler loadJobScheduler) {
        super(loadJobScheduler);
    }

    public boolean registerLoadJob(LoadJob loadJob) {
        writeLock();
        try {
            Map<String, List<LoadJob>> labelToLoadJobs = dbIdToLabelToLoadJobs.get(loadJob.getDbId());
            if (labelToLoadJobs != null) {
                List<LoadJob> jobs = labelToLoadJobs.get(loadJob.getLabel());
                if (jobs != null && !jobs.isEmpty()) {
                    return false;
                }
            }
            addLoadJob(loadJob);
        } finally {
            writeUnlock();
        }
        // persistent
        GlobalStateMgr.getCurrentState().getEditLog().logCreateLoadJob(loadJob);
        return true;
    }
}
