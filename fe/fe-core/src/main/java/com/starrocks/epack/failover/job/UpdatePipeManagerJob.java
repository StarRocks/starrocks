// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.epack.failover.FailoverGroup;
import com.starrocks.epack.load.pipe.PipeManagerEPack;
import com.starrocks.load.pipe.Pipe;
import com.starrocks.load.pipe.PipeId;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UpdatePipeManagerJob extends FailoverGroupJob {
    private static final Logger LOG = LogManager.getLogger(UpdateLoadMgrJob.class);

    public UpdatePipeManagerJob(FailoverGroup failoverGroup) {
        super(failoverGroup);
    }

    @Override
    public void execute() {
        for (Pipe pipe : failoverGroup.getObjectMeta().getPipeManager().getAllPipes()) {
            PipeId pipeId = pipe.getPipeId();
            Long localDbId = failoverGroup.getObjectMap().getLocalDatabaseId(pipeId.getDbId());
            if (localDbId == null) {
                continue;
            }
            pipeId.setDbId(localDbId);
            pipeId.setId(GlobalStateMgr.getServingState().getNextId());
            if (pipe.getState() == Pipe.State.RUNNING) {
                pipe.setState(Pipe.State.SUSPEND);
            }
            ((PipeManagerEPack) GlobalStateMgr.getServingState().getPipeManager()).registerOrUpdatePipe(pipe);
        }
    }
}
