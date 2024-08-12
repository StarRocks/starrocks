// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.epack.failover.FailoverGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UpdateReplicatedObjectJob extends FailoverGroupJob {
    private static final Logger LOG = LogManager.getLogger(UpdateReplicatedObjectJob.class);

    public UpdateReplicatedObjectJob(FailoverGroup failoverGroup) {
        super(failoverGroup);
    }

    @Override
    public void execute() {
        UpdateLoadMgrJob updateLoadMgrJob = new UpdateLoadMgrJob(failoverGroup);
        updateLoadMgrJob.start();

        UpdateRoutineLoadMgrJob updateRoutineLoadJob = new UpdateRoutineLoadMgrJob(failoverGroup);
        updateRoutineLoadJob.start();

        UpdateStreamLoadMgrJob updateStreamLoadMgrJob = new UpdateStreamLoadMgrJob(failoverGroup);
        updateStreamLoadMgrJob.start();

        UpdatePipeManagerJob updatePipeManagerJob = new UpdatePipeManagerJob(failoverGroup);
        updatePipeManagerJob.start();

        UpdateDeleteMgrJob updateDeleteMgrJob = new UpdateDeleteMgrJob(failoverGroup);
        updateDeleteMgrJob.start();

        UpdateTableIncrementIdJob updateTableIncrementIdJob = new UpdateTableIncrementIdJob(failoverGroup);
        updateTableIncrementIdJob.start();
    }
}
