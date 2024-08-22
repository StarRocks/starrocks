// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.epack.failover.FailoverGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class FailoverGroupJob implements Runnable {
    private static final Logger LOG = LogManager.getLogger(FailoverGroupJob.class);

    protected final FailoverGroup failoverGroup;
    protected volatile boolean canceled = false;

    protected FailoverGroupJob(FailoverGroup failoverGroup) {
        this.failoverGroup = failoverGroup;
    }

    public FailoverGroup getFailoverGroup() {
        return this.failoverGroup;
    }

    public void start() {
        failoverGroup.getJobExecutor().addFailoverGroupJob(this);
    }

    public void cancel() {
        canceled = true;
    }

    @Override
    public final void run() {
        if (canceled) {
            return;
        }

        try {
            execute();
        } catch (Exception e) {
            LOG.warn("Failed to execute failover group job in failover group {}, ", failoverGroup.getName(), e);
        } finally {
            failoverGroup.getJobExecutor().removeFailoverGroupJob(this);
        }
    }

    public abstract void execute();
}
