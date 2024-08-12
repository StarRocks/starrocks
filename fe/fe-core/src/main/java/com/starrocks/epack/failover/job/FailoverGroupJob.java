// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.failover.job;

import com.starrocks.epack.failover.FailoverGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class FailoverGroupJob implements Runnable {
    private static final Logger LOG = LogManager.getLogger(FailoverGroupJob.class);

    protected final FailoverGroup failoverGroup;

    protected FailoverGroupJob(FailoverGroup failoverGroup) {
        this.failoverGroup = failoverGroup;
    }

    public FailoverGroup getFailoverGroup() {
        return this.failoverGroup;
    }

    public void start() {
        failoverGroup.addFailoverGroupJob(this);
    }

    @Override
    public final void run() {
        try {
            execute();
        } catch (Exception e) {
            LOG.warn("Failed to execute failover group job in failover group {}, ", failoverGroup.getName(), e);
        } finally {
            failoverGroup.removeFailoverGroupJob(this);
        }
    }

    public abstract void execute();
}
